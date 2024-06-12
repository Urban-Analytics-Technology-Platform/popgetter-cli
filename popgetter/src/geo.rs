use crate::data_request_spec::BBox;
use anyhow::{Context, Result};
use flatgeobuf::{geozero, FeatureProperties, HttpFgbReader};
use geozero::ToWkt;
use polars::{frame::DataFrame, prelude::NamedFrom, series::Series};

/// Function to request geometries from a remotly hosted FGB
///
/// `file_url`: The url of the file to read from
///
/// `bbox`: an optional bounding box to filter the features by
///
/// Returns: a Result object containing a vector of (geometry, properties).
pub async fn get_geometries(
    file_url: &str,
    bbox: Option<&BBox>,
    geoid_col: Option<String>,
) -> Result<DataFrame> {
    let fgb = HttpFgbReader::open(file_url).await?;

    let mut fgb = if let Some(bbox) = bbox {
        fgb.select_bbox(bbox[0], bbox[1], bbox[2], bbox[3]).await?
    } else {
        fgb.select_all().await?
    };

    let mut geoms: Vec<String> = vec![];
    let mut ids: Vec<String> = vec![];
    let geoid_col = geoid_col.unwrap_or_else(|| "GEOID".to_owned());

    while let Some(feature) = fgb.next().await? {
        let props = feature.properties()?;
        geoms.push(feature.to_wkt()?);
        let id = props.get(&geoid_col).with_context(|| "failed to get id")?;
        ids.push(id.clone());
    }

    let ids = Series::new("GEOID", ids);
    let geoms = Series::new("geometry", geoms);
    let result = DataFrame::new(vec![ids, geoms])?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::geozero::{geojson::GeoJson, ColumnValue};
    use flatgeobuf::{geozero::PropertyProcessor, ColumnType, FgbWriter, GeometryType};
    use httpmock::prelude::*;
    use polars::datatypes::AnyValue;

    fn test_fgb() -> FgbWriter<'static> {
        let mut fgb = FgbWriter::create("countries", GeometryType::Polygon).unwrap();
        fgb.add_column("GEOID", ColumnType::String, |_fbb, col| {
            col.nullable = false
        });
        let geom1 = GeoJson(
            r#"
            {
                    "coordinates": [
                      [
                        [
                          -2.3188783647485707,
                          52.69979322604925
                        ],
                        [
                          -2.5454719671669466,
                          52.15312184913398
                        ],
                        [
                          -2.013470682738614,
                          51.91675689282138
                        ],
                        [
                          -1.6883550645436003,
                          52.297957365071426
                        ],
                        [
                          -1.865690550690033,
                          52.79521231460612
                        ],
                        [
                          -2.3188783647485707,
                          52.69979322604925
                        ]
                      ]
                    ],
                    "type": "Polygon"
                  }
             "#,
        );

        let geom2 = GeoJson(
            r#"
                {
                    "coordinates": [
                      [
                        [
                          -0.12189002707808072,
                          51.69745608244253
                        ],
                        [
                          -0.663758311409083,
                          51.538416425565146
                        ],
                        [
                          -0.3977386344766103,
                          51.38495470191404
                        ],
                        [
                          -0.0627732105033374,
                          51.37880690189908
                        ],
                        [
                          0.16381686538463214,
                          51.54453889108953
                        ],
                        [
                          -0.12189002707808072,
                          51.69745608244253
                        ]
                      ]
                    ],
                    "type": "Polygon"
                  }
            "#,
        );

        fgb.add_feature_geom(geom1, |feat| {
            feat.property(0, "id", &ColumnValue::String("one")).unwrap();
        })
        .unwrap();

        fgb.add_feature_geom(geom2, |feat| {
            feat.property(0, "id", &ColumnValue::String("two")).unwrap();
        })
        .unwrap();

        fgb
    }

    fn mock_fgb_server() -> MockServer {
        let fgb = test_fgb();
        let mut buffer: Vec<u8> = vec![];
        fgb.write(&mut buffer).unwrap();

        // Mock out a server
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(GET).path("/fgb_example.fgb");
            then.status(200).header("content-type", "").body(buffer);
        });
        server
    }

    #[tokio::test]
    async fn test_loading_geometries() {
        // Generate a test FGB server
        let server = mock_fgb_server();

        // Get the geometries
        let geoms = get_geometries(&server.url("/fgb_example.fgb"), None, None).await;
        println!("{geoms:#?}");
        assert!(geoms.is_ok(), "The geometry call should not error");
        let geoms = geoms.unwrap();

        assert_eq!(geoms.shape(), (2, 2), "Should recover two features");
        // Order seems to get moved around when reading back

        let row1 = geoms.get(0).unwrap();
        let row2 = geoms.get(1).unwrap();
        assert!(
            row1[0].eq(&AnyValue::String("two")),
            "Feature 1 should have the right ID"
        );
        assert!(
            row2[0].eq(&AnyValue::String("one")),
            "Feature 2 should have the right ID"
        );
    }

    #[tokio::test]
    async fn test_loading_geometries_with_bbox() {
        // Generate a test FGB server
        let server = mock_fgb_server();
        // Get the geometries
        let bbox = BBox([
            -3.018_352_090_792_945,
            51.795_056_187_175_82,
            -1.373_095_490_899_146_4,
            53.026_908_220_355_35,
        ]);
        let geoms = get_geometries(&server.url("/fgb_example.fgb"), Some(&bbox), None).await;

        assert!(geoms.is_ok(), "The geometry call should not error");
        let geoms = geoms.unwrap();
        println!("{geoms:#?}");

        assert_eq!(geoms.shape(), (1, 2), "Should recover one features");
        // Order seems to get moved around when reading back
        println!("{geoms:#?}");
    }
}
