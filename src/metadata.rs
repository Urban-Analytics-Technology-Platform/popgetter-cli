use std::{collections::HashMap, default::Default, fmt::Display};

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

use crate::{config::Config, data_request_spec::GeometrySpec, parquet::MetricRequest, COL};

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
            MetricId::Hxl(_) => COL::METRIC_HXL_TAG.into(),
            MetricId::Id(_) => COL::METRIC_ID.into(),
            MetricId::CommonName(_) => COL::METRIC_HUMAN_READABLE_NAME.into(),
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
        debug!("metrics = {:#?}", metrics);
        let mut id_collections: HashMap<String, Vec<String>> = HashMap::new();

        for metric in metrics {
            id_collections
                .entry(metric.to_col_name())
                .and_modify(|e| e.push(metric.to_query_string().into()))
                .or_insert(vec![metric.to_query_string().into()]);
        }

        let mut filter_expression: Option<Expr> = None;
        debug!("id_collections = {:#?}", id_collections);
        for (col_name, ids) in &id_collections {
            let filter_series = Series::new("filter", ids.clone());
            debug!("filter_series = {:#?}", filter_series);
            filter_expression = if let Some(expression) = filter_expression {
                Some(expression.or(col(col_name).is_in(lit(filter_series))))
            } else {
                Some(col(col_name).is_in(lit(filter_series)))
            };
        }
        debug!("filter_expression = {:#?}", filter_expression);
        ExpandedMetadataTable(self.as_df().filter(filter_expression.unwrap()))
    }

    /// Convert the metrics in the dataframe to MetricRequests
    pub fn to_metric_requests(&self, config: &Config) -> Result<Vec<MetricRequest>> {
        let df = self
            .as_df()
            .select([
                col(COL::METRIC_PARQUET_PATH),
                col(COL::METRIC_PARQUET_COLUMN_NAME),
            ])
            .collect()?;
        debug!("{}", df);
        let metric_requests: Vec<MetricRequest> = df
            .column(COL::METRIC_PARQUET_COLUMN_NAME)?
            .str()?
            .into_iter()
            .zip(df.column(COL::METRIC_PARQUET_PATH)?.str()?)
            .filter_map(|(column, file)| {
                if let (Some(column), Some(file)) = (column, file) {
                    Some(MetricRequest {
                        column: column.to_owned(),
                        file: format!("{}/{file}", config.base_path),
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
        ExpandedMetadataTable(
            self.as_df()
                .filter(col(COL::GEOMETRY_LEVEL).eq(lit(geometry))),
        )
    }

    /// Select a specific set of years in the dataframe filtering out all others
    pub fn select_years<T>(&self, years: &[T]) -> Self
    where
        T: AsRef<str>,
    {
        let years: Vec<&str> = years.iter().map(std::convert::AsRef::as_ref).collect();
        let years_series = Series::new("years", years);
        // TODO: uncomment when years impl
        ExpandedMetadataTable(self.as_df())
        // ExpandedMetadataTable(self.as_df().filter(col("year").is_in(lit(years_series))))
    }

    /// Return a ranked list of avaliable geometries
    pub fn avaliable_geometries(&self) -> Result<Vec<String>> {
        let df = self.as_df();
        let counts: DataFrame = df
            .group_by([col(COL::GEOMETRY_LEVEL)])
            .agg([col(COL::GEOMETRY_LEVEL).count().alias("count")])
            .sort(
                ["count"],
                SortMultipleOptions::new().with_order_descending(true),
            )
            .collect()?;

        Ok(counts
            .column(COL::GEOMETRY_LEVEL)?
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
        debug!("{}", self.as_df().collect()?);
        let reamining: DataFrame = self.as_df().select([col(COL::METRIC_ID)]).collect()?;
        Ok(reamining
            .column(COL::METRIC_ID)?
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
            // Join source data releases
            .join(
                self.source_data_releases.clone().lazy(),
                [col(COL::METRIC_SOURCE_DATA_RELEASE_ID)],
                [col(COL::SOURCE_DATA_RELEASE_ID)],
                JoinArgs::new(JoinType::Inner),
            )
            // Join geometry metadata
            .join(
                self.geometries.clone().lazy(),
                [col(COL::SOURCE_DATA_RELEASE_GEOMETRY_METADATA_ID)],
                [col(COL::GEOMETRY_ID)],
                JoinArgs::new(JoinType::Inner),
            )
            // Join data publishers
            .join(
                self.data_publishers.clone().lazy(),
                [col(COL::SOURCE_DATA_RELEASE_DATA_PUBLISHER_ID)],
                [col(COL::DATA_PUBLISHER_ID)],
                JoinArgs::new(JoinType::Inner),
            );
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
    pub fn get_metric_requests(
        &self,
        metric_ids: Vec<MetricId>,
        config: &Config,
    ) -> Result<Vec<MetricRequest>> {
        self.combined_metric_source_geometry()
            .select_metrics(&metric_ids)
            .to_metric_requests(config)
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

        // TODO: uncomment when years impl
        // If the user has selected a set of years, we will use them explicity
        // let selected_years = if let Some(years) = years {
        //     years.clone()
        // } else {
        //     let avaliable_years = possible_metrics
        //         .select_geometry(&selected_geometry)
        //         // TODO: this currently expects column "year" and this is not present in metadata df
        //         .avaliable_years()?;

        //     if avaliable_years.is_empty() {
        //         return Err(anyhow!(
        //             "No year specified and no year matches found given the geometry level {selected_geometry}"
        //         ));
        //     }
        //     let year = avaliable_years[0].to_owned();
        //     if avaliable_years.len() > 1 {
        //         let rest = avaliable_years[1..].join(",");
        //         advice.push(format!("We automatically selected the year {year}. The requested metrics are also avaiable in the follow time spans {rest}"));
        //     }
        //     vec![year]
        // };

        let metrics = possible_metrics
            .select_geometry(&selected_geometry)
            // TODO: uncomment when years impl
            // .select_years(&selected_years)
            .get_explicit_metric_ids()?;

        Ok(FullSelectionPlan {
            explicit_metric_ids: metrics,
            geometry: selected_geometry,
            // TODO: uncomment when years impl
            // year: selected_years,
            year: vec!["2021".to_string()],
            advice: advice.join("\n"),
        })
    }

    /// Given a geometry level return the path to the
    /// geometry file that it corresponds to
    pub fn get_geom_details(&self, geom_level: &str, config: &Config) -> Result<String> {
        let matches = self
            .geometries
            .clone()
            .lazy()
            .filter(col("level").eq(lit(geom_level)))
            .collect()?;

        let file: String = matches
            .column("filename_stem")?
            .str()?
            .get(0)
            .unwrap()
            .into();

        let file_with_base_path = format!("{}/{}.fgb", config.base_path, file);
        Ok(file_with_base_path)
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
        let metadata = CountryMetadataLoader::new("bel").load(&config).await;
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
        let metadata = CountryMetadataLoader::new("bel")
            .load(&config)
            .await
            .unwrap();
        let expanded_metrics = metadata.expand_regex_metric(
            &MetricId::Hxl(r"population\+adm5".into())
        );
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
            vec![
                "#population+adm5+total+2023",
            ],
            "should get the correct metrics"
        );
    }

    #[tokio::test]
    async fn human_readable_metric_ids_should_expand_properly() {
        let config = Config::default();
        let metadata = CountryMetadataLoader::new("bel")
            .load(&config)
            .await
            .unwrap();
        let expanded_metrics =
            metadata.expand_regex_metric(&MetricId::CommonName("Population, total".into()));

        println!("{:#?}", expanded_metrics);

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
            vec!["Population, total, 2023"],
            "should get the correct metrics"
        );
    }

    #[tokio::test]
    async fn fully_defined_metric_ids_should_expand_to_itself() {
        let config = Config::default();
        let metadata = CountryMetadataLoader::new("bel")
            .load(&config)
            .await
            .unwrap();
        let expanded_metrics =
            metadata.expand_regex_metric(&MetricId::Hxl(r"#population\+adm5\+total\+2023".into()));
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
            vec!["#population+adm5+total+2023"],
            "should get the correct metrics"
        );

        println!("{:#?}", expanded_metrics);
    }
}
