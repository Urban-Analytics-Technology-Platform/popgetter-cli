use anyhow::{anyhow, Result};
use enum_dispatch::enum_dispatch;
use geo::geometry::Geometry;
use geojson;
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_json::Value;
use std::fmt::Write as FmtWrite;
use std::io::Cursor;
use std::io::Write;
use wkb::geom_to_wkb;
use wkt::TryFromWkt;

/// Utility function to convert a polars series from WKT geometries to
/// WKB geometries (as a string)
fn convert_wkt_to_wkb_string(s: &Series) -> PolarsResult<Option<Series>> {
    let ca = s.str()?;
    let wkb_series = ca
        .into_iter()
        .map(|opt_wkt| {
            opt_wkt
                .map(|wkt_str| {
                    Geometry::try_from_wkt_str(wkt_str)
                        .map_err(|err| {
                            PolarsError::ComputeError(
                                format!("Failed to parse wkt: {err:?}").into(),
                            )
                        })
                        .and_then(|geom: Geometry<f64>| {
                            geom_to_wkb(&geom).map_err(|_| {
                                PolarsError::ComputeError("Failed to format geom: {err:?}".into())
                            })
                        })
                })
                .unwrap_or_else(|| Ok(Vec::new()))
        })
        .collect::<Result<Vec<Vec<u8>>, _>>()?;

    let wkb_string_series: Vec<String> = wkb_series
        .into_iter()
        .map(|v| {
            v.iter().fold(String::new(), |mut acc, s| {
                let _ = write!(acc, "{s}");
                acc
            })
        })
        .collect();
    Ok(Some(Series::new("geometry", wkb_string_series)))
}

/// Utility function to convert from polars `AnyValue` to `serde_json::Value`
/// Doesn't cover all types but most of them.
fn any_value_to_json(value: &AnyValue) -> Result<Value> {
    match value {
        AnyValue::Null => Ok(Value::Null),
        AnyValue::Boolean(b) => Ok(Value::Bool(*b)),
        AnyValue::String(s) => Ok(Value::String((*s).to_string())),
        AnyValue::Int8(n) => Ok(json!(*n)),
        AnyValue::Int16(n) => Ok(json!(*n)),
        AnyValue::Int32(n) => Ok(json!(*n)),
        AnyValue::Int64(n) => Ok(json!(*n)),
        AnyValue::UInt8(n) => Ok(json!(*n)),
        AnyValue::UInt16(n) => Ok(json!(*n)),
        AnyValue::UInt32(n) => Ok(json!(*n)),
        AnyValue::UInt64(n) => Ok(json!(*n)),
        AnyValue::Float32(n) => Ok(json!(*n)),
        AnyValue::Float64(n) => Ok(json!(*n)),
        AnyValue::Date(d) => Ok(json!(d.to_string())), // You might want to format this
        AnyValue::Datetime(dt, _, _) => Ok(json!(dt.to_string())), // You might want to format this
        AnyValue::Time(t) => Ok(json!(t.to_string())), // You might want to format this
        AnyValue::List(series) => {
            let json_values: Result<Vec<Value>> =
                series.iter().map(|val| any_value_to_json(&val)).collect();
            Ok(Value::Array(json_values?))
        }
        _ => Err(anyhow!("Failed to convert type")),
    }
}

/// Trait to define different output generators. Defines two
/// functions, format which generates a serialized string of the
/// `DataFrame` and save which generates a file with the generated
/// file
#[enum_dispatch]
pub trait OutputGenerator {
    fn save(&self, writer: &mut impl Write, df: &mut DataFrame) -> Result<()>;
    fn format(&self, df: &mut DataFrame) -> Result<String> {
        // Just creating an empty vec to store the buffered output
        let mut data: Vec<u8> = vec![];
        let mut buff = Cursor::new(&mut data);
        self.save(&mut buff, df)?;

        Ok(String::from_utf8(data)?)
    }
}

/// Enum of OutputFormatters one for each potential
/// output type
#[enum_dispatch(OutputGenerator)]
#[derive(Serialize, Deserialize, Debug)]
pub enum OutputFormatter {
    GeoJSON(GeoJSONFormatter),
    GeoJSONSeq(GeoJSONSeqFormatter),
    Csv(CSVFormatter),
}

/// Format the results as geojson sequence format
/// This is one line per feature serialized as a
/// geojson feature
#[derive(Serialize, Deserialize, Debug)]
pub struct GeoJSONSeqFormatter;

impl OutputGenerator for GeoJSONSeqFormatter {
    fn save(&self, writer: &mut impl Write, df: &mut DataFrame) -> Result<()> {
        let geometry_col = df.column("geometry")?;
        let other_cols = df.drop("geometry")?;
        for (idx, geom) in geometry_col.str()?.into_iter().enumerate() {
            if let Some(wkt_str) = geom {
                let geom: Geometry<f64> = Geometry::try_from_wkt_str(wkt_str).map_err(|err| {
                    anyhow!("Invalid `Geometry<f64>` from well-known text string: {err}")
                })?;
                let mut properties = serde_json::Map::new();
                for col in other_cols.get_columns() {
                    let val = any_value_to_json(&col.get(idx)?)?;
                    properties.insert(col.name().to_string(), val);
                }
                let feature = geojson::Feature {
                    bbox: None,
                    geometry: Some(geojson::Geometry::from(&geom)),
                    id: None,
                    properties: Some(properties),
                    foreign_members: None,
                };
                writeln!(writer, "{feature}")?;
            }
        }
        Ok(())
    }
}

/// Define what format geometries are represented in
///
/// Wkb: Well-known binary
/// Wkt: Well-known text
#[derive(Serialize, Deserialize, Debug)]
pub enum GeoFormat {
    Wkb,
    Wkt,
}

/// Format the results as a CSV file with the
/// geometry encoded in the specified format
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct CSVFormatter {
    pub geo_format: Option<GeoFormat>,
}

impl OutputGenerator for CSVFormatter {
    fn save(&self, writer: &mut impl Write, df: &mut DataFrame) -> Result<()> {
        if let Some(GeoFormat::Wkb) = self.geo_format {
            let mut df = df
                .clone()
                .lazy()
                .with_column(
                    col("geometry")
                        .map(
                            |s: Series| convert_wkt_to_wkb_string(&s),
                            GetOutput::from_type(DataType::String),
                        )
                        .alias("geometry"),
                )
                .collect()?;
            CsvWriter::new(writer).finish(&mut df)?;
        } else {
            CsvWriter::new(writer).finish(df)?;
        };
        Ok(())
    }
}

/// Format the results as a geojson file
/// TODO there is probably a better way to do this using
/// geozero to process the dataframe to a file without
/// having to construct the entire thing in memory first
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct GeoJSONFormatter;

impl OutputGenerator for GeoJSONFormatter {
    fn format(&self, df: &mut DataFrame) -> Result<String> {
        let geometry_col = df.column("geometry")?;
        let other_cols = df.drop("geometry")?;
        let mut features: Vec<geojson::Feature> = vec![];

        for (idx, geom) in geometry_col.str()?.into_iter().enumerate() {
            if let Some(wkt_str) = geom {
                let geom: Geometry<f64> = Geometry::try_from_wkt_str(wkt_str)
                    .map_err(|_| anyhow!("Failed to parse geometry"))?;
                let mut properties = serde_json::Map::new();

                for col in other_cols.get_columns() {
                    let val = any_value_to_json(&col.get(idx)?)?;
                    properties.insert(col.name().to_string(), val);
                }

                let feature = geojson::Feature {
                    geometry: Some(geojson::Geometry::from(&geom)),
                    properties: Some(properties),
                    bbox: None,
                    id: None,
                    foreign_members: None,
                };
                features.push(feature);
            }
        }

        let feature_collection = geojson::FeatureCollection {
            bbox: None,
            features,
            foreign_members: None,
        };
        Ok(feature_collection.to_string())
    }

    fn save(&self, writer: &mut impl Write, df: &mut DataFrame) -> Result<()> {
        let result = self.format(df)?;
        writer.write_all(result.as_bytes())?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_df() -> DataFrame {
        df!(
             "int_val" => &[2, 3, 4],
             "float_val" => &[2.0, 3.0, 4.0],
             "str_val" => &["two", "three", "four"],
             "geometry" => &["POINT (0 0)", "POINT (20 20)", "POINT (30 44)"]
        )
        .unwrap()
    }

    #[test]
    fn geojson_formatter_should_work() {
        let formatter = GeoJSONFormatter;
        let mut df = test_df();
        let output = formatter.format(&mut df);
        assert!(output.is_ok(), "Output should not error");
        let correct_str = r#"{"features":[{"geometry":{"coordinates":[0.0,0.0],"type":"Point"},"properties":{"float_val":2.0,"int_val":2,"str_val":"two"},"type":"Feature"},{"geometry":{"coordinates":[20.0,20.0],"type":"Point"},"properties":{"float_val":3.0,"int_val":3,"str_val":"three"},"type":"Feature"},{"geometry":{"coordinates":[30.0,44.0],"type":"Point"},"properties":{"float_val":4.0,"int_val":4,"str_val":"four"},"type":"Feature"}],"type":"FeatureCollection"}"#;
        assert_eq!(output.unwrap(), correct_str, "Output should be correct");
    }

    #[test]
    fn geojsonseq_formatter_should_work() {
        let formatter = GeoJSONSeqFormatter;
        let mut df = test_df();
        let output = formatter.format(&mut df);

        let correct_str = [
            r#"{"geometry":{"coordinates":[0.0,0.0],"type":"Point"},"properties":{"float_val":2.0,"int_val":2,"str_val":"two"},"type":"Feature"}"#,
            r#"{"geometry":{"coordinates":[20.0,20.0],"type":"Point"},"properties":{"float_val":3.0,"int_val":3,"str_val":"three"},"type":"Feature"}"#,
            r#"{"geometry":{"coordinates":[30.0,44.0],"type":"Point"},"properties":{"float_val":4.0,"int_val":4,"str_val":"four"},"type":"Feature"}"#,
            ""
        ].join("\n");
        assert!(output.is_ok(), "Output should not error");
        assert_eq!(output.unwrap(), correct_str, "Output should be correct");
    }

    #[test]
    fn csv_formatter_should_work() {
        let formatter = CSVFormatter { geo_format: None };
        let mut df = test_df();
        let output = formatter.format(&mut df);
        let correct_str = [
            "int_val,float_val,str_val,geometry",
            "2,2.0,two,POINT (0 0)",
            "3,3.0,three,POINT (20 20)",
            "4,4.0,four,POINT (30 44)",
            "",
        ]
        .join("\n");

        assert!(output.is_ok(), "Output should not error");
        assert_eq!(output.unwrap(), correct_str, "Output should be correct");
    }

    #[test]
    fn csv_formatter_with_wkb_should_work() {
        let formatter = CSVFormatter {
            geo_format: Some(GeoFormat::Wkb),
        };
        let mut df = test_df();
        let output = formatter.format(&mut df);
        let correct_str = [
            "int_val,float_val,str_val,geometry",
            "2,2.0,two,110000000000000000000",
            "3,3.0,three,1100000000052640000005264",
            "4,4.0,four,1100000000062640000007064",
            "",
        ]
        .join("\n");

        assert!(output.is_ok(), "Output should not error");
        assert_eq!(output.unwrap(), correct_str, "Output should be correct");
    }
}
