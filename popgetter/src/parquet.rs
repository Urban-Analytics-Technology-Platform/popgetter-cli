use anyhow::{anyhow, Context, Result};
use itertools::Itertools;
use log::debug;
use polars::prelude::*;
use std::collections::BTreeSet;

use crate::COL;

#[derive(Debug)]
pub struct MetricRequest {
    pub column: String,
    pub metric_file: String,
    pub geom_file: String,
}

pub fn get_metrics_from_file_sql(
    file_url: &str,
    columns: &[String],
    geo_ids: Option<&[&str]>,
) -> anyhow::Result<String> {
    if columns.is_empty() {
        return Err(anyhow!("No columns specified for metrics"));
    }

    let columns_sql = columns
        .iter()
        .map(|col| format!("\"{col}\""))
        .collect_vec()
        .join(", ");

    let geo_ids_sql = geo_ids
        .map(|geo_ids| {
            geo_ids
                .iter()
                .map(|geo_id| format!("'{geo_id}'"))
                .collect_vec()
                .join(", ")
        })
        .map(|geo_ids_str| format!(" WHERE \"{}\" IN ({})", COL::GEO_ID, geo_ids_str))
        .unwrap_or_default();

    Ok(format!(
        "SELECT \"{}\", {} FROM read_parquet('{}'){}",
        COL::GEO_ID,
        columns_sql,
        file_url,
        geo_ids_sql
    ))
}

/// Given a `file_url` and a list of `columns`, return a `Result<DataFrame>`
/// with the requested columns, filtered by `geo_id`s if nessesary
async fn get_metrics_from_file(
    file_url: &str,
    columns: &[String],
    geo_ids: Option<&[&str]>,
) -> Result<DataFrame> {
    let mut cols: Vec<Expr> = columns.iter().map(|c| col(c)).collect();
    cols.push(col(COL::GEO_ID));

    #[cfg(not(target_arch = "wasm32"))]
    {
        // Get owned types for spawn_blocking
        let file_url = file_url.to_owned();
        let geo_ids = geo_ids.map(|v| v.iter().map(|el| el.to_string()).collect::<Vec<_>>());

        // Run spawn_blocking around scan_parquet with interior async runtime call
        let result = tokio::task::spawn_blocking(move || {
            let args = ScanArgsParquet::default();
            let df = match LazyFrame::scan_parquet(file_url, args) {
                Ok(df) => df,
                Err(err) => return Err(err),
            }
            .with_streaming(true)
            .select(cols);

            let df = if let Some(ids) = geo_ids {
                let id_series = Series::new("geo_ids", ids);
                df.filter(col(COL::GEO_ID).is_in(lit(id_series)))
            } else {
                df
            };
            df.collect()
        })
        .await?;
        Ok(result?)
    }
    #[cfg(target_arch = "wasm32")]
    {
        // TODO: this needs to be updated to only request the columns required as currently
        // will request entire parquet file
        // An example of this is in polars (see https://github.com/pola-rs/polars/blob/3dda47e578e0b50a5bb7c459ebee6c5c76d41c75/crates/polars-io/src/parquet/read/async_impl.rs)
        // but calls this code through creating its own multi-threaded tokio runtime that will not
        // compile to WASM.
        let response = reqwest::get(file_url).await?;
        let bytes = response.bytes().await?;
        let cursor = std::io::Cursor::new(bytes);
        let df = ParquetReader::new(cursor).finish()?.lazy().select(cols);
        let df = if let Some(ids) = geo_ids {
            let id_series = Series::new("geo_ids", ids);
            df.filter(col(COL::GEO_ID).is_in(lit(id_series)))
        } else {
            df
        };
        let result = df.collect()?;
        Ok(result)
    }
}

// Returns a BTreeSet of unique columns instead of HashSet to enable deterministic ordering
fn files_from_metrics(metrics: &[MetricRequest]) -> BTreeSet<String> {
    metrics.iter().map(|m| m.metric_file.clone()).collect()
}

/// Given a set of metrics and optional `geo_ids`, this function will
/// retrive all the required metrics from the cloud blob storage
///
pub async fn get_metrics(metrics: &[MetricRequest], geo_ids: Option<&[&str]>) -> Result<DataFrame> {
    let file_list = files_from_metrics(metrics);
    debug!("{:#?}", file_list);
    // TODO Can we do this async so we can be downloading results from each file together?
    let mut dfs = vec![];
    for file_url in &file_list {
        let file_cols: Vec<String> = metrics
            .iter()
            .filter_map(|m| {
                if m.metric_file == file_url.clone() {
                    Some(m.column.clone())
                } else {
                    None
                }
            })
            .collect();
        dfs.push(get_metrics_from_file(file_url, &file_cols, geo_ids).await?);
    }

    // TODO: The following assumes that we requested metrics for the same geo_ids. This is not
    // generally true
    let mut joined_df: Option<DataFrame> = None;

    // Merge the dataframes from each remove file in to a single dataframe
    for df in dfs {
        if let Some(prev_dfs) = joined_df {
            joined_df = Some(prev_dfs.join(
                &df,
                vec![COL::GEO_ID],
                vec![COL::GEO_ID],
                JoinArgs::new(JoinType::Inner),
            )?);
        } else {
            joined_df = Some(df.clone());
        }
    }
    // Return if None, or return df with COL::GEO_ID first
    Ok(joined_df
        .with_context(|| "Failed to combine data queries")?
        .lazy()
        .select(&[col(COL::GEO_ID), col("*").exclude([COL::GEO_ID])])
        .collect()?)
}

pub fn get_metrics_sql(metrics: &[MetricRequest], geo_ids: Option<&[&str]>) -> Result<String> {
    let file_urls = files_from_metrics(metrics);
    let mut columns_by_file_url: Vec<Vec<String>> = vec![];
    let queries = file_urls
        .iter()
        .map(|file_url| {
            let file_columns = metrics
                .iter()
                .filter_map(|m| {
                    if m.metric_file == file_url.clone() {
                        Some(m.column.clone())
                    } else {
                        None
                    }
                })
                .collect_vec();
            columns_by_file_url.push(file_columns.clone());
            get_metrics_from_file_sql(file_url, &file_columns, geo_ids)
        })
        .collect::<Result<Vec<String>>>()?;

    // If from single URL, no join is required
    if file_urls.len().eq(&1) {
        // Unwrap: cannot be None since length is 1
        return Ok(queries.into_iter().next().unwrap());
    }

    // If from multple URLs, join is required
    // Select columns for final table
    let select = format!(
        "SELECT q0.{}, {}",
        COL::GEO_ID,
        columns_by_file_url
            .into_iter()
            .enumerate()
            .flat_map(|(idx, columns)| columns
                .into_iter()
                .map(move |col| format!("q{idx}.\"{col}\"")))
            .collect::<Vec<String>>()
            .join(", ")
    );
    // Construct first query
    let queries_and_joins = queries
        .into_iter()
        .enumerate()
        .map(|(idx, query)| {
            let operation = if idx.eq(&0) { "FROM" } else { "JOIN" };
            let join_column = if idx.eq(&0) {
                "".to_string()
            } else {
                format!(" USING ({})", COL::GEO_ID)
            };
            format!("{} ({}) q{idx}{}", operation, query, join_column)
        })
        .collect_vec();

    // Combine select and queries_and_joins
    Ok(vec![select]
        .into_iter()
        .chain(queries_and_joins)
        .collect_vec()
        .join("\n"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::wasm_bindgen_test;

    #[tokio::test]
    async fn test_fetching_metrics() {
        let metrics  = [
            MetricRequest{
                metric_file: "https://popgetter.blob.core.windows.net/popgetter-cli-test/tracts_2019_fiveYear.parquet".into(),
                column: "B17021_E006".into(),
                geom_file: "Not needed for this test".into(),
            }];
        let df = get_metrics(&metrics, None).await;
        assert!(df.is_ok(), "We should get back a result");
        let df = df.unwrap();
        assert_eq!(
            df.shape().1,
            2,
            "The returned dataframe should have the correct number of columns"
        );
        assert_eq!(
            df.shape().0,
            74001,
            "The returned dataframe should have the correct number of rows"
        );
        assert!(
            df.column(COL::GEO_ID).is_ok(),
            "The returned dataframe should have a GEO_ID column"
        );
        assert!(
            df.column("B17021_E006").is_ok(),
            "The returned dataframe should have the column we requested"
        );
    }

    #[tokio::test]
    async fn test_fetching_metrics_with_geo_filter() {
        let metrics  = [
            MetricRequest{
                metric_file: "https://popgetter.blob.core.windows.net/popgetter-cli-test/tracts_2019_fiveYear.parquet".into(),
                column: "B17021_E006".into(),
                geom_file: "Not needed for this test".into(),
            }];
        let df = get_metrics(
            &metrics,
            Some(&["1400000US01001020100", "1400000US01001020300"]),
        )
        .await;

        assert!(df.is_ok(), "We should get back a result");
        let df = df.unwrap();
        assert_eq!(
            df.shape().1,
            2,
            "The returned dataframe should have the correct number of columns"
        );
        assert_eq!(
            df.shape().0,
            2,
            "The returned dataframe should have the correct number of rows"
        );
    }

    #[test]
    fn test_get_metrics_from_file_sql() -> anyhow::Result<()> {
        let file_url = "https://popgetter.blob.core.windows.net/releases/v0.2/gb_nir/metrics/DZ21DT0001.parquet";
        let columns = vec!["1".to_string(), "2".to_string()];
        let actual_str = get_metrics_from_file_sql(file_url, &columns, None)?;
        let expected_str_without_geo_ids = "SELECT \"GEO_ID\", \"1\", \"2\" FROM read_parquet('https://popgetter.blob.core.windows.net/releases/v0.2/gb_nir/metrics/DZ21DT0001.parquet')";
        assert_eq!(actual_str, expected_str_without_geo_ids);

        let geo_ids = vec!["N20000001", "N20000002"];
        let actual_str = get_metrics_from_file_sql(file_url, &columns, Some(&geo_ids))?;
        let expected_str_with_geo_ids = "SELECT \"GEO_ID\", \"1\", \"2\" FROM read_parquet('https://popgetter.blob.core.windows.net/releases/v0.2/gb_nir/metrics/DZ21DT0001.parquet') WHERE \"GEO_ID\" IN ('N20000001', 'N20000002')";
        println!("{expected_str_with_geo_ids}");
        assert_eq!(actual_str, expected_str_with_geo_ids);
        Ok(())
    }

    #[test]
    fn test_get_metrics_sql() -> anyhow::Result<()> {
        let file_url1 = "https://popgetter.blob.core.windows.net/releases/v0.2/gb_nir/metrics/DZ21DT0001.parquet";
        let file_url2 = "https://popgetter.blob.core.windows.net/releases/v0.2/gb_nir/metrics/DZ21DT0002.parquet";
        let geo_ids = vec!["N20000001", "N20000002"];
        let metric_requests = vec![
            MetricRequest {
                column: "1".to_string(),
                metric_file: file_url1.to_string(),
                geom_file: "".to_string(),
            },
            MetricRequest {
                column: "2".to_string(),
                metric_file: file_url1.to_string(),
                geom_file: "".to_string(),
            },
            MetricRequest {
                column: "3".to_string(),
                metric_file: file_url2.to_string(),
                geom_file: "".to_string(),
            },
            MetricRequest {
                column: "4".to_string(),
                metric_file: file_url2.to_string(),
                geom_file: "".to_string(),
            },
        ];

        let expected_single_file_query = r#"SELECT "GEO_ID", "1", "2" FROM read_parquet('https://popgetter.blob.core.windows.net/releases/v0.2/gb_nir/metrics/DZ21DT0001.parquet') WHERE "GEO_ID" IN ('N20000001', 'N20000002')"#;
        let actual_single_file_query = get_metrics_sql(&metric_requests[..2], Some(&geo_ids))?;
        assert_eq!(actual_single_file_query, expected_single_file_query);

        let expected_multi_file_query = r#"SELECT q0.GEO_ID, q0."1", q0."2", q1."3", q1."4"
FROM (SELECT "GEO_ID", "1", "2" FROM read_parquet('https://popgetter.blob.core.windows.net/releases/v0.2/gb_nir/metrics/DZ21DT0001.parquet') WHERE "GEO_ID" IN ('N20000001', 'N20000002')) q0
JOIN (SELECT "GEO_ID", "3", "4" FROM read_parquet('https://popgetter.blob.core.windows.net/releases/v0.2/gb_nir/metrics/DZ21DT0002.parquet') WHERE "GEO_ID" IN ('N20000001', 'N20000002')) q1 USING (GEO_ID)"#;
        let actual_multi_file_query = get_metrics_sql(&metric_requests, Some(&geo_ids))?;
        assert_eq!(actual_multi_file_query, expected_multi_file_query);
        Ok(())
    }

    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen_test(async)]
    async fn test_fetching_metrics_with_geo_filter_wasm32() {
        let metrics  = [
            MetricRequest{
                metric_file: "https://popgetter.blob.core.windows.net/releases/v0.2/usa/metrics/2019fiveYearblockgroup0.parquet".into(),
                column: "individuals".into(),
                geom_file: "Not needed for this test".into(),
            }];
        let df = get_metrics(
            &metrics,
            Some(&["1500000US010010201001", "1500000US721537506022"]),
        )
        .await;

        assert!(df.is_ok(), "We should get back a result");
        let df = df.unwrap();
        assert_eq!(
            df.shape().1,
            2,
            "The returned dataframe should have the correct number of columns"
        );
        assert_eq!(
            df.shape().0,
            2,
            "The returned dataframe should have the correct number of rows"
        );
    }
}
