//! Search

use crate::{metadata::Metadata, COL};
use chrono::NaiveDate;
use log::debug;
use polars::lazy::dsl::{col, lit, Expr};
use polars::prelude::{DataFrame, LazyFrame};
use serde::{Deserialize, Serialize};

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

/// Search in a column case-insensitively for a string literal (i.e. not a regex!)
fn case_insensitive_contains(column: &str, value: &str) -> Expr {
    let regex = format!("(?i){}", regex::escape(value));
    col(column).str().contains(lit(regex), false)
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum SearchContext {
    Hxl,
    HumanReadableName,
    Description,
}

impl SearchContext {
    pub fn all() -> Vec<Self> {
        vec![Self::Hxl, Self::HumanReadableName, Self::Description]
    }
}

/// Implementing conversion from `SearchText` to a polars expression enables a
/// `SearchText` to be passed to polars dataframe for filtering results.
impl From<SearchText> for Option<Expr> {
    fn from(val: SearchText) -> Self {
        let queries = val
            .context
            .iter()
            .map(|field| match field {
                SearchContext::Hxl => case_insensitive_contains(COL::METRIC_HXL_TAG, &val.text),
                SearchContext::HumanReadableName => {
                    case_insensitive_contains(COL::METRIC_HUMAN_READABLE_NAME, &val.text)
                }
                SearchContext::Description => {
                    case_insensitive_contains(COL::METRIC_DESCRIPTION, &val.text)
                }
            })
            .collect();
        combine_exprs_with_or(queries)
    }
}

impl From<YearRange> for Expr {
    fn from(value: YearRange) -> Self {
        match value {
            YearRange::Before(year) => col(COL::SOURCE_REFERENCE_PERIOD_START)
                .lt_eq(lit(NaiveDate::from_ymd_opt(year.into(), 12, 31).unwrap())),
            YearRange::After(year) => col(COL::SOURCE_REFERENCE_PERIOD_END)
                .gt_eq(lit(NaiveDate::from_ymd_opt(year.into(), 1, 1).unwrap())),
            YearRange::Between(start, end) => {
                let start_col = col(COL::SOURCE_REFERENCE_PERIOD_START);
                let end_col = col(COL::SOURCE_REFERENCE_PERIOD_END);
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

impl From<DataPublisher> for Option<Expr> {
    fn from(value: DataPublisher) -> Self {
        combine_exprs_with_or(
            value
                .0
                .iter()
                .map(|val| case_insensitive_contains(COL::PUBLISHER_NAME, val))
                .collect(),
        )
    }
}

impl From<SourceDataRelease> for Option<Expr> {
    fn from(value: SourceDataRelease) -> Self {
        combine_exprs_with_or(
            value
                .0
                .iter()
                .map(|val| case_insensitive_contains(COL::SOURCE_NAME, val))
                .collect(),
        )
    }
}

impl From<GeometryLevel> for Option<Expr> {
    fn from(value: GeometryLevel) -> Self {
        combine_exprs_with_or(
            value
                .0
                .iter()
                .map(|val| case_insensitive_contains(COL::GEOMETRY_LEVEL, val))
                .collect(),
        )
    }
}

impl From<Country> for Option<Expr> {
    fn from(value: Country) -> Self {
        combine_exprs_with_or(
            value
                .0
                .iter()
                .map(|val| case_insensitive_contains(COL::COUNTRY_NAME_SHORT_EN, val))
                .collect(),
        )
    }
}

impl From<SourceMetricId> for Option<Expr> {
    fn from(value: SourceMetricId) -> Self {
        combine_exprs_with_or(
            value
                .0
                .iter()
                .map(|val| case_insensitive_contains(COL::METRIC_SOURCE_METRIC_ID, val))
                .collect(),
        )
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SearchText {
    pub text: String,
    pub context: Vec<SearchContext>,
}

impl Default for SearchText {
    fn default() -> Self {
        Self {
            text: "".to_string(),
            context: SearchContext::all(),
        }
    }
}

/// Note: year ranges are inclusive of end points.
#[derive(PartialEq, Eq, Clone, Debug, Deserialize, Serialize)]
pub enum YearRange {
    Before(u16),
    After(u16),
    Between(u16, u16),
}

/// To allow search over multiple years
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GeometryLevel(pub Vec<String>);

/// Source data release: set of strings that will search over this
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SourceDataRelease(pub Vec<String>);

/// Data publisher: set of strings that will search over this
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DataPublisher(pub Vec<String>);

/// Countries: set of countries to be included in the search
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Country(pub Vec<String>);

/// Census tables: set of census tables to be included in the search
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SourceMetricId(pub Vec<String>);

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SearchRequest {
    pub text: Vec<SearchText>,
    pub year_range: Vec<YearRange>,
    pub geometry_level: Option<GeometryLevel>,
    pub source_data_release: Option<SourceDataRelease>,
    pub data_publisher: Option<DataPublisher>,
    pub country: Option<Country>,
    pub census_table: Option<SourceMetricId>,
}

impl SearchRequest {
    pub fn new() -> Self {
        Self {
            text: vec![],
            year_range: vec![],
            geometry_level: None,
            source_data_release: None,
            data_publisher: None,
            country: None,
            census_table: None,
        }
    }

    pub fn with_country(mut self, country: &str) -> Self {
        self.country = Some(Country(vec![country.to_string()]));
        self
    }

    pub fn with_data_publisher(mut self, data_publisher: &str) -> Self {
        self.data_publisher = Some(DataPublisher(vec![data_publisher.to_string()]));
        self
    }

    pub fn with_source_data_release(mut self, source_data_release: &str) -> Self {
        self.source_data_release = Some(SourceDataRelease(vec![source_data_release.to_string()]));
        self
    }

    pub fn with_year_range(mut self, year_range: YearRange) -> Self {
        self.year_range = vec![year_range];
        self
    }

    pub fn with_geometry_level(mut self, geometry_level: &str) -> Self {
        self.geometry_level = Some(GeometryLevel(vec![geometry_level.to_string()]));
        self
    }

    pub fn with_census_table(mut self, census_table: &str) -> Self {
        self.census_table = Some(SourceMetricId(vec![census_table.to_string()]));
        self
    }

    pub fn search_results(self, metadata: &Metadata) -> anyhow::Result<SearchResults> {
        debug!("Searching with request: {:?}", self);
        let expr: Option<Expr> = self.into();
        let full_results: LazyFrame = metadata.combined_metric_source_geometry().0;
        let result: DataFrame = match expr {
            Some(expr) => full_results.filter(expr),
            None => full_results,
        }
        .collect()?;
        Ok(SearchResults(result))
    }
}

impl Default for SearchRequest {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug)]
pub struct SearchResults(pub DataFrame);

impl From<SearchRequest> for Option<Expr> {
    fn from(value: SearchRequest) -> Self {
        let mut subexprs: Vec<Option<Expr>> =
            value.text.into_iter().map(|text| text.into()).collect();
        subexprs.extend(value.year_range.into_iter().map(|v| Some(v.into())));
        let other_subexprs: Vec<Option<Expr>> = vec![
            value.geometry_level.and_then(|v| v.into()),
            value.source_data_release.and_then(|v| v.into()),
            value.data_publisher.and_then(|v| v.into()),
            value.country.and_then(|v| v.into()),
            value.census_table.and_then(|v| v.into()),
        ];
        subexprs.extend(other_subexprs);
        // Remove the Nones and unwrap the Somes
        let valid_subexprs: Vec<Expr> = subexprs.into_iter().flatten().collect();
        combine_exprs_with_and(valid_subexprs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // #[test]
    // fn test_search_request() {
    //     let mut sr = SearchRequest{search_string: None}.with_country("a").with_country("b");
    // }
}
