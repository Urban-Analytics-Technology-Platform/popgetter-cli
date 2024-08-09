//! Types and functions to perform filtering on the fully joined metadata catalogue

use crate::{
    config::Config,
    data_request_spec::RegionSpec,
    geo::get_geometries,
    metadata::ExpandedMetadata,
    parquet::{get_metrics, MetricRequest},
    COL,
};
use anyhow::bail;
use chrono::NaiveDate;
use log::{debug, warn};
use nonempty::{nonempty, NonEmpty};
use polars::lazy::dsl::{col, lit, Expr};
use polars::prelude::{DataFrame, DataFrameJoinOps, IntoLazy, LazyFrame};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, str::FromStr};
use tokio::try_join;

// TODO: add trait/struct for combine_exprs

/// Combine multiple queries with OR. If there are no queries in the input list, returns None.
fn combine_exprs_with_or(exprs: Vec<Expr>) -> Option<Expr> {
    let mut query: Option<Expr> = None;
    for expr in exprs {
        query = if let Some(partial_query) = query {
            Some(partial_query.or(expr))
        } else {
            Some(expr)
        };
    }
    query
}

/// Same as `combine_exprs_with_or`, but takes a NonEmpty list instead of a Vec, and doesn't
/// return an Option.
fn combine_exprs_with_or1(exprs: NonEmpty<Expr>) -> Expr {
    let mut query: Expr = exprs.head;
    for expr in exprs.tail.into_iter() {
        query = query.or(expr);
    }
    query
}

/// Combine multiple queries with AND. If there are no queries in the input list, returns None.
fn combine_exprs_with_and(exprs: Vec<Expr>) -> Option<Expr> {
    let mut query: Option<Expr> = None;
    for expr in exprs {
        query = if let Some(partial_query) = query {
            Some(partial_query.and(expr))
        } else {
            Some(expr)
        };
    }
    query
}

/// Same as `combine_exprs_with_and`, but takes a NonEmpty list instead of a Vec, and doesn't
/// return an Option.
fn _combine_exprs_with_and1(exprs: NonEmpty<Expr>) -> Expr {
    let mut query: Expr = exprs.head;
    for expr in exprs.tail.into_iter() {
        query = query.and(expr);
    }
    query
}

/// Search in a column case-insensitively for a string literal (i.e. not a regex!). The search
/// parameter can appear anywhere in the column value.
fn case_insensitive_contains(column: &str, value: &str) -> Expr {
    let regex = format!("(?i){}", regex::escape(value));
    col(column).str().contains(lit(regex), false)
}

/// Search in a column case-insensitively for a string literal (i.e. not a regex!). The search
/// parameter must be a prefix of the column value.
fn case_insensitive_startswith(column: &str, value: &str) -> Expr {
    let regex = format!("(?i)^{}", regex::escape(value));
    col(column).str().contains(lit(regex), false)
}

/// Where we want to search for a text string in. Pass multiple search contexts to search in all of
/// them.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum SearchContext {
    Hxl,
    HumanReadableName,
    Description,
}

impl SearchContext {
    pub fn all() -> NonEmpty<Self> {
        nonempty![Self::Hxl, Self::HumanReadableName, Self::Description]
    }
}

/// Implementing conversion from `SearchText` to a polars expression enables a
/// `SearchText` to be passed to polars dataframe for filtering results.
impl From<SearchText> for Expr {
    fn from(val: SearchText) -> Self {
        let queries: NonEmpty<Expr> = val.context.map(|field| match field {
            SearchContext::Hxl => case_insensitive_contains(COL::METRIC_HXL_TAG, &val.text),
            SearchContext::HumanReadableName => {
                case_insensitive_contains(COL::METRIC_HUMAN_READABLE_NAME, &val.text)
            }
            SearchContext::Description => {
                case_insensitive_contains(COL::METRIC_DESCRIPTION, &val.text)
            }
        });
        combine_exprs_with_or1(queries)
    }
}

impl From<YearRange> for Expr {
    fn from(value: YearRange) -> Self {
        match value {
            YearRange::Before(year) => col(COL::SOURCE_DATA_RELEASE_REFERENCE_PERIOD_START)
                .lt_eq(lit(NaiveDate::from_ymd_opt(year.into(), 12, 31).unwrap())),
            YearRange::After(year) => col(COL::SOURCE_DATA_RELEASE_REFERENCE_PERIOD_END)
                .gt_eq(lit(NaiveDate::from_ymd_opt(year.into(), 1, 1).unwrap())),
            YearRange::Between(start, end) => {
                let start_col = col(COL::SOURCE_DATA_RELEASE_REFERENCE_PERIOD_START);
                let end_col = col(COL::SOURCE_DATA_RELEASE_REFERENCE_PERIOD_END);
                let start_date = lit(NaiveDate::from_ymd_opt(start.into(), 1, 1).unwrap());
                let end_date = lit(NaiveDate::from_ymd_opt(end.into(), 12, 31).unwrap());
                // (start_col <= start_date AND end_col >= start_date)
                // OR (start_col <= end_date AND end_col >= end_date)
                // OR (start_col >= start_date AND end_col <= end_date)
                let case1 = start_col
                    .clone()
                    .lt_eq(start_date.clone())
                    .and(end_col.clone().gt_eq(start_date.clone()));
                let case2 = start_col
                    .clone()
                    .lt_eq(end_date.clone())
                    .and(end_col.clone().gt_eq(end_date.clone()));
                let case3 = start_col.gt_eq(start_date).and(end_col.lt_eq(end_date));
                case1.or(case2).or(case3)
            }
        }
    }
}

impl From<DataPublisher> for Expr {
    fn from(value: DataPublisher) -> Self {
        case_insensitive_contains(COL::DATA_PUBLISHER_NAME, &value.0)
    }
}

impl From<SourceDataRelease> for Expr {
    fn from(value: SourceDataRelease) -> Self {
        case_insensitive_contains(COL::SOURCE_DATA_RELEASE_NAME, &value.0)
    }
}

impl From<GeometryLevel> for Expr {
    fn from(value: GeometryLevel) -> Self {
        case_insensitive_contains(COL::GEOMETRY_LEVEL, &value.0)
    }
}

impl From<Country> for Expr {
    fn from(value: Country) -> Self {
        combine_exprs_with_or(vec![
            case_insensitive_contains(COL::COUNTRY_NAME_SHORT_EN, &value.0),
            case_insensitive_contains(COL::COUNTRY_NAME_OFFICIAL, &value.0),
            case_insensitive_contains(COL::COUNTRY_ISO2, &value.0),
            case_insensitive_contains(COL::COUNTRY_ISO3, &value.0),
            case_insensitive_contains(COL::COUNTRY_ISO3166_2, &value.0),
            // TODO: add `COUNTRY_ID` for ExpandedMetadata
            // case_insensitive_contains(COL::COUNTRY_ID, &value.0),
            case_insensitive_contains(COL::DATA_PUBLISHER_COUNTRIES_OF_INTEREST, &value.0),
        ])
        // Unwrap: cannot be None as vec above is non-empty
        .unwrap()
    }
}

impl From<SourceMetricId> for Expr {
    fn from(value: SourceMetricId) -> Self {
        case_insensitive_contains(COL::METRIC_SOURCE_METRIC_ID, &value.0)
    }
}

impl From<MetricId> for Expr {
    fn from(value: MetricId) -> Self {
        case_insensitive_startswith(COL::METRIC_ID, &value.0)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SearchText {
    pub text: String,
    pub context: NonEmpty<SearchContext>,
}

impl Default for SearchText {
    fn default() -> Self {
        Self {
            text: "".to_string(),
            context: SearchContext::all(),
        }
    }
}

/// Search over years
#[derive(PartialEq, Eq, Clone, Debug, Deserialize, Serialize)]
pub enum YearRange {
    Before(u16),
    After(u16),
    Between(u16, u16),
}

impl FromStr for YearRange {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        fn str_to_option_u16(value: &str) -> Result<Option<u16>, anyhow::Error> {
            if value.is_empty() {
                return Ok(None);
            }
            match value.parse::<u16>() {
                Ok(value) => Ok(Some(value)),
                Err(_) => bail!("Invalid year range"),
            }
        }
        let parts: Vec<Option<u16>> = s
            .split("...")
            .map(str_to_option_u16)
            .collect::<Result<Vec<Option<u16>>, _>>()?;
        match parts.as_slice() {
            [Some(a)] => Ok(YearRange::Between(*a, *a)),
            [None, Some(a)] => Ok(YearRange::Before(*a)),
            [Some(a), None] => Ok(YearRange::After(*a)),
            [Some(a), Some(b)] => {
                if a > b {
                    bail!("Invalid year range")
                } else {
                    Ok(YearRange::Between(*a, *b))
                }
            }
            _ => bail!("Invalid year range"),
        }
    }
}

/// Search over metric IDs
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MetricId(pub String);

/// Search over geometry levels
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GeometryLevel(pub String);

/// Search over source data release names
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SourceDataRelease(pub String);

/// Search over data publisher names
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DataPublisher(pub String);

/// Search over country (short English names)
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Country(pub String);

/// Search over source metric IDs in the original census table
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SourceMetricId(pub String);

/// This struct represents all the possible parameters one can search the metadata catalogue with.
/// All parameters are optional in that they can either be empty vectors or None.
///
/// Each of the fields are combined with an AND operation, so searching for both text and a year
/// range will only return metrics that satisfy both parameters.
///
/// However, if a parameter has multiple values (e.g. multiple text strings), these are combined
/// with an OR operation. So searching for multiple text strings will return metrics that satisfy
/// any of the text strings.
#[derive(Clone, Debug, Deserialize, Serialize, Default)]
pub struct SearchParams {
    pub text: Vec<SearchText>,
    pub year_range: Option<Vec<YearRange>>,
    pub metric_id: Vec<MetricId>,
    pub geometry_level: Option<GeometryLevel>,
    pub source_data_release: Option<SourceDataRelease>,
    pub data_publisher: Option<DataPublisher>,
    pub country: Option<Country>,
    pub source_metric_id: Option<SourceMetricId>,
    pub region_spec: Vec<RegionSpec>,
}

impl SearchParams {
    pub fn search(self, expanded_metadata: &ExpandedMetadata) -> SearchResults {
        debug!("Searching with request: {:?}", self);
        let expr: Option<Expr> = self.into();
        let full_results: LazyFrame = expanded_metadata.as_df();
        let result: LazyFrame = match expr {
            Some(expr) => full_results.filter(expr),
            None => full_results,
        };
        SearchResults(result.collect().unwrap())
    }
}

fn to_queries_then_or<T: Into<Expr>>(queries: Vec<T>) -> Option<Expr> {
    let queries: Vec<Expr> = queries.into_iter().map(|q| q.into()).collect();
    combine_exprs_with_or(queries)
}

fn _to_optqueries_then_or<T: Into<Option<Expr>>>(queries: Vec<T>) -> Option<Expr> {
    let query_options: Vec<Option<Expr>> = queries.into_iter().map(|q| q.into()).collect();
    let queries: Vec<Expr> = query_options.into_iter().flatten().collect();
    combine_exprs_with_or(queries)
}

impl From<SearchParams> for Option<Expr> {
    fn from(value: SearchParams) -> Self {
        let mut subexprs: Vec<Option<Expr>> = value
            .text
            .into_iter()
            .map(|text| Some(text.into()))
            .collect();
        subexprs.extend([to_queries_then_or(value.metric_id)]);
        if let Some(year_range) = value.year_range {
            subexprs.extend([to_queries_then_or(year_range)]);
        }
        let other_subexprs: Vec<Option<Expr>> = vec![
            value.geometry_level.map(|v| v.into()),
            value.source_data_release.map(|v| v.into()),
            value.data_publisher.map(|v| v.into()),
            value.country.map(|v| v.into()),
            value.source_metric_id.map(|v| v.into()),
        ];
        subexprs.extend(other_subexprs);
        // Remove the Nones and unwrap the Somes
        let valid_subexprs: Vec<Expr> = subexprs.into_iter().flatten().collect();
        combine_exprs_with_and(valid_subexprs)
    }
}

#[derive(Clone, Debug)]
pub struct SearchResults(pub DataFrame);

impl SearchResults {
    /// Convert all the metrics in the dataframe to MetricRequests
    pub fn to_metric_requests(self, config: &Config) -> Vec<MetricRequest> {
        // Using unwrap throughout this function because if any of them fail, it means our upstream
        // data is invalid!
        // TODO: Maybe map the error type instead to provide some useful error messages
        let df = self
            .0
            .lazy()
            .select([
                col(COL::METRIC_PARQUET_PATH),
                col(COL::METRIC_PARQUET_COLUMN_NAME),
                col(COL::GEOMETRY_FILEPATH_STEM),
            ])
            .collect()
            .unwrap();
        df.column(COL::METRIC_PARQUET_COLUMN_NAME)
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .zip(
                df.column(COL::METRIC_PARQUET_PATH)
                    .unwrap()
                    .str()
                    .unwrap()
                    .into_no_null_iter(),
            )
            .zip(
                df.column(COL::GEOMETRY_FILEPATH_STEM)
                    .unwrap()
                    .str()
                    .unwrap()
                    .into_no_null_iter(),
            )
            .map(|((column, metric_file), geom_file)| MetricRequest {
                column: column.to_owned(),
                metric_file: format!("{}/{metric_file}", config.base_path),
                geom_file: format!("{}/{geom_file}.fgb", config.base_path),
            })
            .collect()
    }

    // Given a Data Request Spec
    // Return a DataFrame of the selected dataset
    pub async fn download(
        self,
        config: &Config,
        search_params: &SearchParams,
        include_geoms: bool,
    ) -> anyhow::Result<DataFrame> {
        let metric_requests = self.to_metric_requests(config);
        debug!("metric_requests = {:#?}", metric_requests);

        if metric_requests.is_empty() {
            bail!(
                "No metric requests were derived from given `search_params`: {:#?}",
                search_params
            )
        }

        let all_geom_files: HashSet<String> = metric_requests
            .iter()
            .map(|m| m.geom_file.clone())
            .collect();

        // TODO Handle multiple geometries
        if all_geom_files.len() > 1 {
            unimplemented!("Multiple geometries not supported in current release");
        } else if all_geom_files.len() == 0 {
            bail!(
                "No geometry files for the following `metric_requests`: {:#?}",
                metric_requests
            )
        }

        // Keep as future and run alongside get_geometries
        let metrics = get_metrics(&metric_requests, None);

        let result = if include_geoms {
            // TODO Pass in the bbox as the second argument here
            if search_params.region_spec.len() > 1 {
                todo!(
                    "Multiple region specifications are not yet supported: {:#?}",
                    search_params.region_spec
                );
            }
            let bbox = search_params
                .region_spec
                .first()
                .and_then(|region_spec| region_spec.bbox().clone());

            if bbox.is_some() {
                warn!(
                    "The bounding box should be specified in the same coordinate reference system \
                     as the requested geometry."
                )
            }
            let geoms = get_geometries(all_geom_files.iter().next().unwrap(), bbox);

            // Run metrics and geoms futures
            let (metrics, geoms) = try_join!(metrics, geoms)?;
            debug!("geoms: {geoms:#?}");
            debug!("metrics: {metrics:#?}");
            geoms.inner_join(&metrics, [COL::GEO_ID], [COL::GEO_ID])?
        } else {
            let metrics = metrics.await.map_err(anyhow::Error::from)?;
            debug!("metrics: {metrics:#?}");
            metrics
        };

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    // use super::*;
    // #[test]
    // fn test_search_request() {
    //     let mut sr = SearchRequest{search_string: None}.with_country("a").with_country("b");
    // }
}
