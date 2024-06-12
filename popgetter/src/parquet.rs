use anyhow::{Context, Result};
use polars::prelude::*;
use std::collections::HashSet;

static GEO_ID_COL_NAME: &str = "GEO_ID";

#[derive(Debug)]
pub struct MetricRequest {
    pub column: String,
    pub file: String,
}

/// Given a `file_url` and a list of `columns`, return a `Result<DataFrame>`
/// with the requested columns, filtered by `geo_id`s if nessesary
fn get_metrics_from_file(
    file_url: &String,
    columns: &[String],
    geo_ids: Option<&[&str]>,
) -> Result<DataFrame> {
    let mut cols: Vec<Expr> = columns.iter().map(|c| col(c)).collect();
    cols.push(col(GEO_ID_COL_NAME));

    let args = ScanArgsParquet::default();

    let df = LazyFrame::scan_parquet(file_url, args)?
        .with_streaming(true)
        .select(cols);

    let df = if let Some(ids) = geo_ids {
        let id_series = Series::new("geo_ids", ids);
        df.filter(col(GEO_ID_COL_NAME).is_in(lit(id_series)))
    } else {
        df
    };

    let result = df.collect()?;
    Ok(result)
}

/// Given a set of metrics and optional `geo_ids`, this function will
/// retrive all the required metrics from the cloud blob storage
///
pub fn get_metrics(metrics: &[MetricRequest], geo_ids: Option<&[&str]>) -> Result<DataFrame> {
    let file_list: HashSet<String> = metrics.iter().map(|m| m.file.clone()).collect();

    // TODO Can we do this async so we can be downloading results from each file together?
    let dfs: Result<Vec<DataFrame>> = file_list
        .iter()
        .map(|file_url| {
            let file_cols: Vec<String> = metrics
                .iter()
                .filter_map(|m| {
                    if m.file == file_url.clone() {
                        Some(m.column.clone())
                    } else {
                        None
                    }
                })
                .collect();
            get_metrics_from_file(file_url, &file_cols, geo_ids)
        })
        .collect();

    let mut joined_df: Option<DataFrame> = None;

    // Merge the dataframes from each remove file in to a single
    // dataframe
    for df in dfs? {
        if let Some(prev_dfs) = joined_df {
            joined_df = Some(prev_dfs.join(
                &df,
                vec![GEO_ID_COL_NAME],
                vec![GEO_ID_COL_NAME],
                JoinArgs::new(JoinType::Inner),
            )?);
        } else {
            joined_df = Some(df.clone());
        }
    }

    joined_df.with_context(|| "Failed to combine data queries")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fetching_metrics() {
        let metrics  = [
            MetricRequest{
                file:"https://popgetter.blob.core.windows.net/popgetter-cli-test/tracts_2019_fiveYear.parquet".into(),
                column:"B17021_E006".into()
            }];
        let df = get_metrics(&metrics, None);
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
            df.column(GEO_ID_COL_NAME).is_ok(),
            "The returned dataframe should have a GEO_ID column"
        );
        assert!(
            df.column("B17021_E006").is_ok(),
            "The returned dataframe should have the column we requested"
        );
    }

    #[test]
    fn test_fetching_metrics_with_geo_filter() {
        let metrics  = [
            MetricRequest{
                file:"https://popgetter.blob.core.windows.net/popgetter-cli-test/tracts_2019_fiveYear.parquet".into(),
                column:"B17021_E006".into()
            }];
        let df = get_metrics(
            &metrics,
            Some(&["1400000US01001020100", "1400000US01001020300"]),
        );

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
