//! Search

use crate::metadata::Metadata;
use polars::lazy::dsl::{col, lit, Expr};
use polars::prelude::{DataFrame, LazyFrame};
use serde::{Deserialize, Serialize};
use log::debug;
use itertools::izip;
use comfy_table::{
    Table,
    Cell,
    Attribute,
    CellAlignment,
    ContentArrangement,
    presets::NOTHING
};

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
            .into_iter()
            .map(|field| {
                match field {
                    SearchContext::Hxl => col("hxl_tag"),
                    SearchContext::HumanReadableName => col("human_readable_name"),
                    SearchContext::Description => col("description"),
                }
                .eq(lit(val.text.clone()))
            })
            .collect();
        combine_exprs_with_or(queries)
    }
}

impl From<Year> for Option<Expr> {
    fn from(value: Year) -> Self {
        combine_exprs_with_or(
            value
                .0
                .into_iter()
                .map(|val| col("year").eq(lit(val)))
                .collect(),
        )
    }
}

impl From<DataPublisher> for Option<Expr> {
    fn from(value: DataPublisher) -> Self {
        combine_exprs_with_or(
            value
                .0
                .into_iter()
                .map(|val| col("data_publisher").eq(lit(val)))
                .collect(),
        )
    }
}

impl From<SourceDataRelease> for Option<Expr> {
    fn from(value: SourceDataRelease) -> Self {
        combine_exprs_with_or(
            value
                .0
                .into_iter()
                .map(|val| col("source_data_release").eq(lit(val)))
                .collect(),
        )
    }
}

impl From<GeometryLevel> for Option<Expr> {
    fn from(value: GeometryLevel) -> Self {
        combine_exprs_with_or(
            value
                .0
                .into_iter()
                .map(|val| col("geometry_level").eq(lit(val)))
                .collect(),
        )
    }
}

impl From<Country> for Option<Expr> {
    fn from(value: Country) -> Self {
        combine_exprs_with_or(
            value
                .0
                .into_iter()
                .map(|val| col("country").eq(lit(val)))
                .collect(),
        )
    }
}

impl From<SourceMetricId> for Option<Expr> {
    fn from(value: SourceMetricId) -> Self {
        combine_exprs_with_or(
            value
                .0
                .into_iter()
                .map(|val| col("source_metric_id").eq(lit(val)))
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

// Whether year is string or int has implications with how it's encoded in the dfs
// TODO: open ticket to capture how to progress this
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Year(pub Vec<String>);

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
    pub year: Option<Year>,
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
            year: None,
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

    pub fn with_year(mut self, year: &str) -> Self {
        self.year = Some(Year(vec![year.to_string()]));
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
        let full_results: LazyFrame = metadata.combined_metric_source_geometry();
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

impl std::fmt::Display for SearchResults {

    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // ["human_readable_name", "source_metric_id", "description", "hxl_tag", "metric_parquet_path", "parquet_column_name", "parquet_margin_of_error_column", "parquet_margin_of_error_file", "potential_denominator_ids", "parent_metric_id", "source_data_release_id", "source_download_url", "source_archive_file_path", "source_documentation_url", "id", "name", "date_published", "reference_period_start", "reference_period_end", "collection_period_start", "collection_period_end", "expect_next_update", "url", "data_publisher_id", "description_right", "geometry_metadata_id", "validity_period_start", "validity_period_end", "level", "hxl_tag_right", "filename_stem"]

        for (metric_id, hrn, desc, hxl, level) in izip!(
            self.0.column("id").unwrap().iter(),
            self.0.column("human_readable_name").unwrap().iter(),
            self.0.column("description").unwrap().iter(),
            self.0.column("hxl_tag").unwrap().iter(),
            self.0.column("level").unwrap().iter(),
        ) {
            let mut table = Table::new();
            table
                .load_preset(NOTHING)
                .set_content_arrangement(ContentArrangement::Dynamic)
                .add_row(vec![
                    Cell::new("Metric ID").add_attribute(Attribute::Bold),
                    metric_id.get_str().unwrap().into(), 
                ])
                .add_row(vec![
                    Cell::new("Human readable name").add_attribute(Attribute::Bold),
                    hrn.get_str().unwrap().into(),
                ])
                .add_row(vec![
                    Cell::new("Description").add_attribute(Attribute::Bold),
                    desc.get_str().unwrap().into(),
                ])
                .add_row(vec![
                    Cell::new("HXL tag").add_attribute(Attribute::Bold),
                    hxl.get_str().unwrap().into(),
                ])
                .add_row(vec![
                    Cell::new("Geometry level").add_attribute(Attribute::Bold),
                    level.get_str().unwrap().into(),
                ]);

            let column = table.column_mut(0).unwrap();
            column.set_cell_alignment(CellAlignment::Right);

            writeln!(f, "\n{}", table)?;
        }
        Ok(())
    }
}

impl From<SearchRequest> for Option<Expr> {
    fn from(value: SearchRequest) -> Self {
        let mut subexprs: Vec<Option<Expr>> =
            value.text.into_iter().map(|text| text.into()).collect();
        let other_subexprs: Vec<Option<Expr>> = vec![
            value.year.and_then(|v| v.into()),
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
