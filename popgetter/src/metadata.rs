use crate::{config::Config, data_request_spec::GeometrySpec, parquet::MetricRequest};
use anyhow::{anyhow, Result};
use futures::future::join_all;
use futures::try_join;
use log::debug;
use log::info;
use polars::{
    chunked_array::ops::SortMultipleOptions,
    frame::DataFrame,
    lazy::{
        dsl::{col, Expr},
        frame::{IntoLazy, LazyFrame, ScanArgsParquet},
    },
    prelude::{lit, JoinArgs, JoinType, NamedFrom, UnionArgs},
    series::Series,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, default::Default, fmt::Display};

/// This struct contains the base url and names of
/// the files that contain the metadata. It has a
/// default impl which give the version that we will
/// normally use but this allows us to customise it
/// if we need to.
pub struct CountryMetadataPaths {
    geometry: String,
    metrics: String,
    country: String,
    source_data: String,
    data_publishers: String,
}

/// Represents a way of refering to a metric id
/// can be converted into a polars expression for
/// selection
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub enum MetricId {
    /// Hxl (Humanitarian Exchange Language) tag
    Hxl(String),
    /// Internal UUID
    Id(String),
    /// Human Readable name
    CommonName(String),
}

impl MetricId {
    /// Returns the column in the metadata that this id type corrispondes to
    pub fn to_col_name(&self) -> String {
        match self {
            MetricId::Hxl(_) => "metric_hxl_tag".into(),
            MetricId::Id(_) => "metric_id".into(),
            MetricId::CommonName(_) => "human_readable_name".into(),
        }
    }
    /// Return a string representing the textual content of the ID
    pub fn to_query_string(&self) -> &str {
        match self {
            MetricId::CommonName(s) | MetricId::Id(s) | MetricId::Hxl(s) => s,
        }
    }

    /// Generate a polars Expr that will do
    /// an exact match on the MetricId
    pub fn to_polars_expr(&self) -> Expr {
        col(&self.to_col_name()).eq(self.to_query_string())
    }

    /// Generate a polars Expr that will generate
    /// a regex search for the content of the Id
    pub fn to_fuzzy_polars_expr(&self) -> Expr {
        col(&self.to_col_name())
            .str()
            .contains(lit(self.to_query_string()), false)
    }
}

impl From<MetricId> for Expr {
    fn from(value: MetricId) -> Self {
        value.to_polars_expr()
    }
}

impl Default for CountryMetadataPaths {
    fn default() -> Self {
        Self {
            geometry: "geometry_metadata.parquet".into(),
            metrics: "metric_metadata.parquet".into(),
            country: "country_metadata.parquet".into(),
            source_data: "source_data_releases.parquet".into(),
            data_publishers: "data_publishers.parquet".into(),
        }
    }
}

/// `CountryMetadataLoader` takes a country iso string
/// along with a CountryMetadataPaths and provides methods
/// for fetching and constructing a `Metadata` catalogue.
pub struct CountryMetadataLoader {
    country: String,
    paths: CountryMetadataPaths,
}

/// A structure that represents a full joined lazy data frame
/// containing all of the metadata
pub struct ExpandedMetadataTable(pub LazyFrame);

impl ExpandedMetadataTable {
    /// Get access to the lazy data frame
    pub fn as_df(&self) -> LazyFrame {
        self.0.clone()
    }

    /// Filter the dataframe by the specified metrics
    pub fn select_metrics(&self, metrics: &[MetricId]) -> Self {
        let mut id_collections: HashMap<String, Vec<String>> = HashMap::new();

        for metric in metrics {
            id_collections
                .entry(metric.to_col_name())
                .and_modify(|e| e.push(metric.to_query_string().into()))
                .or_insert(vec![metric.to_query_string().into()]);
        }

        let mut filter_expression: Option<Expr> = None;
        for (col_name, ids) in &id_collections {
            let filter_series = Series::new("filter", ids.clone());
            filter_expression = if let Some(expression) = filter_expression {
                Some(expression.or(col(col_name).is_in(lit(filter_series))))
            } else {
                Some(col(col_name).is_in(lit(filter_series)))
            };
        }

        ExpandedMetadataTable(self.as_df().filter(filter_expression.unwrap()))
    }

    /// Convert the metrics in the dataframe to MetricRequests
    pub fn to_metric_requests(&self) -> Result<Vec<MetricRequest>> {
        let df = self
            .as_df()
            .select([col("parquet_metric_file"), col("parquet_metric_id")])
            .collect()?;

        let metric_requests: Vec<MetricRequest> = df
            .column("parquet_metric_id")?
            .str()?
            .into_iter()
            .zip(df.column("parquet_metric_file")?.str()?)
            .filter_map(|(column, file)| {
                if let (Some(column), Some(file)) = (column, file) {
                    Some(MetricRequest {
                        column: column.to_owned(),
                        file: file.to_owned(),
                    })
                } else {
                    None
                }
            })
            .collect();
        Ok(metric_requests)
    }

    /// Select a specific geometry level in the dataframe filtering out all others
    pub fn select_geometry(&self, geometry: &str) -> Self {
        ExpandedMetadataTable(self.as_df().filter(col("geometry_level").eq(geometry)))
    }

    /// Select a specific set of years in the dataframe filtering out all others
    pub fn select_years<T>(&self, years: &[T]) -> Self
    where
        T: AsRef<str>,
    {
        let years: Vec<&str> = years.iter().map(std::convert::AsRef::as_ref).collect();
        let years_series = Series::new("years", years);
        ExpandedMetadataTable(self.as_df().filter(col("year").is_in(lit(years_series))))
    }

    /// Return a ranked list of avaliable geometries
    pub fn avaliable_geometries(&self) -> Result<Vec<String>> {
        let df = self.as_df();
        let counts: DataFrame = df
            .group_by([col("geometry_level")])
            .agg([col("goemetry_level").count().alias("count")])
            .sort(
                ["count"],
                SortMultipleOptions::new().with_order_descending(true),
            )
            .collect()?;

        Ok(counts
            .column("geometry_level")?
            .str()?
            .iter()
            .filter_map(|geom| geom.map(std::borrow::ToOwned::to_owned))
            .collect())
    }

    /// Return a ranked list of avaliable years
    pub fn avaliable_years(&self) -> Result<Vec<String>> {
        let df = self.as_df();
        let counts: DataFrame = df
            .group_by([col("year")])
            .agg([col("year").count().alias("count")])
            .sort(
                ["count"],
                SortMultipleOptions::new().with_order_descending(true),
            )
            .collect()?;

        Ok(counts
            .column("year")?
            .str()?
            .iter()
            .filter_map(|geom| geom.map(std::borrow::ToOwned::to_owned))
            .collect())
    }

    /// Get fully speced metric ids
    pub fn get_explicit_metric_ids(&self) -> Result<Vec<MetricId>> {
        let reamining: DataFrame = self.as_df().select([col("metric_id")]).collect()?;
        Ok(reamining
            .column("id")?
            .str()?
            .into_iter()
            .filter_map(|pos_id| pos_id.map(|id| MetricId::Id(id.to_owned())))
            .collect())
    }
}

/// The metadata struct contains the polars `DataFrames` for
/// the various different metadata tables. Can be constructed
/// from a single `CountryMetadataLoader` or for all countries.
/// It also provides the various functions for searching and
/// getting `MetricRequests` from the catalogue.
#[derive(Debug)]
pub struct Metadata {
    pub metrics: DataFrame,
    pub geometries: DataFrame,
    pub source_data_releases: DataFrame,
    pub data_publishers: DataFrame,
    pub countries: DataFrame,
}

/// Describes a fully specified selection plan. The MetricIds should all
/// be the ID variant. Geometry and years are backed in now.
/// Advice specifies and alternative options that the user should
/// be aware of.
pub struct FullSelectionPlan {
    pub explicit_metric_ids: Vec<MetricId>,
    pub geometry: String,
    pub year: Vec<String>,
    pub advice: String,
}

impl Display for FullSelectionPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Getting {} metrics \n, on {} geometries \n , for the years {}",
            self.explicit_metric_ids.len(),
            self.geometry,
            self.year.join(",")
        )
    }
}

impl Metadata {
    /// If our metric_id is a regex, expand it in to a list of explicit `MetricIds`
    pub fn expand_regex_metric(&self, metric_id: &MetricId) -> Result<Vec<MetricId>> {
        let col_name = metric_id.to_col_name();
        let catalogue = self.combined_metric_source_geometry();

        catalogue
            .as_df()
            .filter(metric_id.to_fuzzy_polars_expr())
            .collect()?
            .column(&col_name)?
            .str()?
            .iter()
            .map(|expanded_id| {
                if let Some(id) = expanded_id {
                    Ok(match metric_id {
                        MetricId::Hxl(_) => MetricId::Hxl(id.into()),
                        MetricId::Id(_) => MetricId::Id(id.into()),
                        MetricId::CommonName(_) => MetricId::CommonName(id.into()),
                    })
                } else {
                    Err(anyhow!("Failed to expand id"))
                }
            })
            .collect()
    }

    /// Generate a Lazy DataFrame which joins the metrics, source and geometry metadata
    pub fn combined_metric_source_geometry(&self) -> ExpandedMetadataTable {
        let df: LazyFrame = self
            .metrics
            .clone()
            .lazy()
            // Rename these first because they will cause clashes when merging
            .rename(
                ["id", "description", "hxl_tag"],
                ["metric_id", "metric_description", "metric_hxl_tag"],
            )
            // Join source data releases
            .join(
                self.source_data_releases.clone().lazy(),
                [col("source_data_release_id")],
                [col("id")],
                JoinArgs::new(JoinType::Inner),
            )
            .rename(
                [
                    "url",
                    "description",
                    "name",
                    "date_published",
                    "reference_period_start",
                    "reference_period_end",
                    "collection_period_start",
                    "collection_period_end",
                    "expect_next_update",
                ],
                [
                    "release_url",
                    "release_description",
                    "release_name",
                    "release_date_published",
                    "release_reference_period_start",
                    "release_reference_period_end",
                    "release_collection_period_start",
                    "release_collection_period_end",
                    "release_expect_next_update",
                ],
            )
            // Join geometry metadata
            .join(
                self.geometries.clone().lazy(),
                [col("geometry_metadata_id")],
                [col("id")],
                JoinArgs::new(JoinType::Inner),
            )
            .rename(
                [
                    "validity_period_start",
                    "validity_period_end",
                    "level",
                    "hxl_tag",
                    "filename_stem",
                ],
                [
                    "geometry_validity_period_start",
                    "geometry_validity_period_end",
                    "geometry_level",
                    "geometry_hxl_tag",
                    "geometry_filename_stem",
                ],
            )
            // Join data publishers
            .join(
                self.data_publishers.clone().lazy(),
                [col("data_publisher_id")],
                [col("id")],
                JoinArgs::new(JoinType::Inner),
            )
            .rename(
                ["url", "description", "name"],
                [
                    "data_publisher_url",
                    "data_publisher_description",
                    "data_publisher_name",
                ],
            )
            // Get rid of intermediate IDs as we don't need them
            .drop([
                "source_data_release_id",
                "geometry_metadata_id",
                "data_publisher_id",
            ]);
        // TODO: Add a country_id column to the metadata, and merge in the countries as well. See
        // https://github.com/Urban-Analytics-Technology-Platform/popgetter/issues/104

        // Debug print the column names so that we know what we can access
        let schema = df.schema().unwrap();
        let column_names = schema
            .iter_names()
            .map(|s| s.as_str())
            .collect::<Vec<&str>>();
        debug!("Column names in merged metadata: {:?}", column_names);

        ExpandedMetadataTable(df)
    }

    /// Return a list of MetricRequests for the given metrics_ids
    pub fn get_metric_requests(&self, metric_ids: Vec<MetricId>) -> Result<Vec<MetricRequest>> {
        self.combined_metric_source_geometry()
            .select_metrics(&metric_ids);
        Ok(vec![])
    }

    /// Generates a FullSelectionPlan which takes in to account
    /// what the user has requested with sane fallbacks if geography
    /// or years have not been specified.
    pub fn generate_selection_plan(
        &self,
        metrics: &[MetricId],
        geometry: &GeometrySpec,
        years: &Option<Vec<String>>,
    ) -> Result<FullSelectionPlan> {
        let mut advice: Vec<String> = vec![];
        // Find metadata for all specified metrics over all geoemtries and years
        let possible_metrics = self
            .combined_metric_source_geometry()
            .select_metrics(metrics);

        // If the user has selected a geometry, we will use it explicitly
        let selected_geometry = if let Some(geom) = &geometry.geometry_level {
            geom.clone()
        }
        // Otherwise we will get the geometry with the most matches to our
        // metrics
        else {
            // Get a ranked list of geometriesthat are avaliable for these
            // metrics
            let avaliable_geometries = possible_metrics.avaliable_geometries()?;
            if avaliable_geometries.is_empty() {
                return Err(anyhow!(
                    "No geometry specifed and non found for these metrics"
                ));
            }

            let geom = avaliable_geometries[0].to_owned();
            if avaliable_geometries.len() > 1 {
                let rest = avaliable_geometries[1..].join(",");
                advice.push(format!("We are selecting the geometry level {geom}. The requested metrics are also avaliable at the following levels: {rest}"));
            }
            geom
        };

        // If the user has selected a set of years, we will use them explicity
        let selected_years = if let Some(years) = years {
            years.clone()
        } else {
            let avaliable_years = possible_metrics
                .select_geometry(&selected_geometry)
                .avaliable_years()?;

            if avaliable_years.is_empty() {
                return Err(anyhow!(
                    "No year specified and no year matches found given the geometry level {selected_geometry}"
                ));
            }
            let year = avaliable_years[0].to_owned();
            if avaliable_years.len() > 1 {
                let rest = avaliable_years[1..].join(",");
                advice.push(format!("We automatically selected the year {year}. The requested metrics are also avaiable in the follow time spans {rest}"));
            }
            vec![year]
        };

        let metrics = possible_metrics
            .select_geometry(&selected_geometry)
            .select_years(&selected_years)
            .get_explicit_metric_ids()?;

        Ok(FullSelectionPlan {
            explicit_metric_ids: metrics,
            geometry: selected_geometry,
            year: selected_years,
            advice: advice.join("\n"),
        })
    }

    /// Given a geometry level return the path to the
    /// geometry file that it corresponds to
    pub fn get_geom_details(&self, geom_level: &str) -> Result<String> {
        let matches = self
            .geometries
            .clone()
            .lazy()
            .filter(col("level").eq(lit(geom_level)))
            .collect()?;

        let file = matches
            .column("filename_stem")?
            .str()?
            .get(0)
            .unwrap()
            .into();

        Ok(file)
    }
}

impl CountryMetadataLoader {
    /// Create a metadata loader for a specific Country
    pub fn new(country: &str) -> Self {
        let paths = CountryMetadataPaths::default();
        Self {
            country: country.into(),
            paths,
        }
    }
    /// Overwrite the Paths object to specifiy custom
    /// metadata filenames and `base_url`.
    pub fn with_paths(&mut self, paths: CountryMetadataPaths) -> &mut Self {
        self.paths = paths;
        self
    }

    /// Load the Metadata catalouge for this country with
    /// the specified metadata paths
    pub async fn load(self, config: &Config) -> Result<Metadata> {
        let t = try_join!(
            self.load_metadata(&self.paths.metrics, config),
            self.load_metadata(&self.paths.geometry, config),
            self.load_metadata(&self.paths.source_data, config),
            self.load_metadata(&self.paths.data_publishers, config),
            self.load_metadata(&self.paths.country, config),
        )?;
        Ok(Metadata {
            metrics: t.0,
            geometries: t.1,
            source_data_releases: t.2,
            data_publishers: t.3,
            countries: t.4,
        })
    }

    /// Performs a load of a given metadata parquet file
    async fn load_metadata(&self, path: &str, config: &Config) -> Result<DataFrame> {
        let full_path = format!("{}/{}/{path}", config.base_path, self.country);
        let args = ScanArgsParquet::default();
        info!("Attempting to load dataframe from {full_path}");
        tokio::task::spawn_blocking(move || {
            LazyFrame::scan_parquet(&full_path, args)?
                .collect()
                .map_err(|e| anyhow!("Failed to load '{full_path}': {e}"))
        })
        .await?
    }
}

/// Load the metadata for a list of countries and merge them into
/// a single `Metadata` catalouge.
pub async fn load_all(config: &Config) -> Result<Metadata> {
    let country_text_file = format!("{}/countries.txt", config.base_path);
    let country_names: Vec<String> = reqwest::Client::new()
        .get(&country_text_file)
        .send()
        .await?
        .text()
        .await?
        .lines()
        .map(|s| s.to_string())
        .collect();
    info!("Detected country names: {:?}", country_names);

    let metadata: Result<Vec<Metadata>> = join_all(
        country_names
            .iter()
            .map(|c| CountryMetadataLoader::new(c).load(config)),
    )
    .await
    .into_iter()
    .collect();
    let metadata = metadata?;

    // Merge metrics
    let metric_dfs: Vec<LazyFrame> = metadata.iter().map(|m| m.metrics.clone().lazy()).collect();
    let metrics = polars::prelude::concat(metric_dfs, UnionArgs::default())?.collect()?;
    info!("Merged metrics with shape: {:?}", metrics.shape());

    // Merge geometries
    let geometries_dfs: Vec<LazyFrame> = metadata
        .iter()
        .map(|m| m.geometries.clone().lazy())
        .collect();
    let geometries = polars::prelude::concat(geometries_dfs, UnionArgs::default())?.collect()?;
    info!("Merged geometries with shape: {:?}", geometries.shape());

    // Merge source data relaeses
    let source_data_dfs: Vec<LazyFrame> = metadata
        .iter()
        .map(|m| m.source_data_releases.clone().lazy())
        .collect();

    let source_data_releases =
        polars::prelude::concat(source_data_dfs, UnionArgs::default())?.collect()?;
    info!(
        "Merged source data releases with shape: {:?}",
        source_data_releases.shape()
    );

    // Merge source data publishers
    let data_publisher_dfs: Vec<LazyFrame> = metadata
        .iter()
        .map(|m| m.data_publishers.clone().lazy())
        .collect();

    let data_publishers =
        polars::prelude::concat(data_publisher_dfs, UnionArgs::default())?.collect()?;
    info!(
        "Merged data publishers with shape: {:?}",
        data_publishers.shape()
    );

    // Merge countries
    let countries_dfs: Vec<LazyFrame> = metadata
        .iter()
        .map(|m| m.countries.clone().lazy())
        .collect();
    let countries = polars::prelude::concat(countries_dfs, UnionArgs::default())?.collect()?;
    info!("Merged countries with shape: {:?}", countries.shape());

    Ok(Metadata {
        metrics,
        geometries,
        source_data_releases,
        data_publishers,
        countries,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    /// TODO stub out a mock here that we can use to test with.

    #[tokio::test]
    async fn country_metadata_should_load() {
        let config = Config::default();
        let metadata = CountryMetadataLoader::new("be").load(&config).await;
        println!("{metadata:#?}");
        assert!(metadata.is_ok(), "Data should have loaded ok");
    }

    #[tokio::test]
    async fn all_metadata_should_load() {
        let config = Config::default();
        let metadata = load_all(&config).await;
        println!("{metadata:#?}");
        assert!(metadata.is_ok(), "Data should have loaded ok");
    }

    #[tokio::test]
    async fn metric_ids_should_expand_properly() {
        let config = Config::default();
        let metadata = CountryMetadataLoader::new("be")
            .load(&config)
            .await
            .unwrap();
        let expanded_metrics = metadata.expand_regex_metric(&MetricId::Hxl("population-*".into()));
        assert!(
            expanded_metrics.is_ok(),
            "Should successfully expand metrics"
        );
        let expanded_metrics = expanded_metrics.unwrap();

        assert_eq!(
            expanded_metrics.len(),
            7,
            "should return the correct number of metrics"
        );

        let metric_names: Vec<&str> = expanded_metrics
            .iter()
            .map(MetricId::to_query_string)
            .collect();

        assert_eq!(
            metric_names,
            vec![
                "#population+children+age5_17",
                "#population+infants+age0_4",
                "#population+children+age0_17",
                "#population+adults+f",
                "#population+adults+m",
                "#population+adults",
                "#population+ind"
            ],
            "should get the correct metrics"
        );
    }

    #[tokio::test]
    async fn human_readable_metric_ids_should_expand_properly() {
        let config = Config::default();
        let metadata = CountryMetadataLoader::new("be")
            .load(&config)
            .await
            .unwrap();
        let expanded_metrics =
            metadata.expand_regex_metric(&MetricId::CommonName("Children*".into()));

        println!("{:#?}", expanded_metrics);

        assert!(
            expanded_metrics.is_ok(),
            "Should successfully expand metrics"
        );

        let expanded_metrics = expanded_metrics.unwrap();

        assert_eq!(
            expanded_metrics.len(),
            2,
            "should return the correct number of metrics"
        );

        let metric_names: Vec<&str> = expanded_metrics
            .iter()
            .map(MetricId::to_query_string)
            .collect();

        assert_eq!(
            metric_names,
            vec!["Children aged 5 to 17", "Children aged 0 to 17"],
            "should get the correct metrics"
        );
    }

    #[tokio::test]
    async fn fully_defined_metric_ids_should_expand_to_itself() {
        let config = Config::default();
        let metadata = CountryMetadataLoader::new("be")
            .load(&config)
            .await
            .unwrap();
        let expanded_metrics =
            metadata.expand_regex_metric(&MetricId::Hxl(r"#population\+infants\+age0\_4".into()));
        assert!(
            expanded_metrics.is_ok(),
            "Should successfully expand metrics"
        );
        let expanded_metrics = expanded_metrics.unwrap();

        assert_eq!(
            expanded_metrics.len(),
            1,
            "should return the correct number of metrics"
        );

        let metric_names: Vec<&str> = expanded_metrics
            .iter()
            .map(MetricId::to_query_string)
            .collect();

        assert_eq!(
            metric_names,
            vec!["#population+infants+age0_4",],
            "should get the correct metrics"
        );

        println!("{:#?}", expanded_metrics);
    }
}
