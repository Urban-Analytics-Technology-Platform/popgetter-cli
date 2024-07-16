use std::default::Default;
use std::fmt::Display;

use anyhow::{anyhow, Result};
use futures::future::join_all;
use log::debug;
use log::info;
use polars::{
    frame::DataFrame,
    lazy::{
        dsl::col,
        frame::{IntoLazy, LazyFrame, ScanArgsParquet},
    },
    prelude::{JoinArgs, JoinType, UnionArgs},
};
use tokio::try_join;

use crate::{config::Config, search::MetricId, COL};

/// This struct contains the base url and names of the files that contain the metadata.
pub struct PATHS {}

impl PATHS {
    pub const GEOMETRY_METADATA: &'static str = "geometry_metadata.parquet";
    pub const METRIC_METADATA: &'static str = "metric_metadata.parquet";
    pub const COUNTRY: &'static str = "country_metadata.parquet";
    pub const SOURCE: &'static str = "source_data_releases.parquet";
    pub const PUBLISHER: &'static str = "data_publishers.parquet";
}

/// `CountryMetadataLoader` takes a country iso string
/// along with a CountryMetadataPaths and provides methods
/// for fetching and constructing a `Metadata` catalogue.
pub struct CountryMetadataLoader {
    country: String,
}

/// A structure that represents a full joined lazy data frame containing all of the metadata
pub struct ExpandedMetadata(pub LazyFrame);

impl ExpandedMetadata {
    /// Get access to the lazy data frame
    pub fn as_df(&self) -> LazyFrame {
        self.0.clone()
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
    /// Generate a Lazy DataFrame which joins the metrics, source and geometry metadata
    pub fn combined_metric_source_geometry(&self) -> ExpandedMetadata {
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
            )
            // TODO: consider case when many countries
            .explode([col(COL::DATA_PUBLISHER_COUNTRIES_OF_INTEREST)])
            .join(
                self.countries.clone().lazy(),
                [col(COL::DATA_PUBLISHER_COUNTRIES_OF_INTEREST)],
                [col(COL::COUNTRY_ID)],
                JoinArgs::new(JoinType::Inner),
            );

        // Debug print the column names so that we know what we can access
        let schema = df.schema().unwrap();
        let column_names = schema
            .iter_names()
            .map(|s| s.as_str())
            .collect::<Vec<&str>>();
        debug!("Column names in merged metadata: {:?}", column_names);

        ExpandedMetadata(df)
    }
}

impl CountryMetadataLoader {
    /// Create a metadata loader for a specific Country
    pub fn new(country: &str) -> Self {
        Self {
            country: country.into(),
        }
    }

    /// Load the Metadata catalouge for this country with
    /// the specified metadata paths
    pub async fn load(self, config: &Config) -> Result<Metadata> {
        let t = try_join!(
            self.load_metadata(PATHS::METRIC_METADATA, config),
            self.load_metadata(PATHS::GEOMETRY_METADATA, config),
            self.load_metadata(PATHS::SOURCE, config),
            self.load_metadata(PATHS::PUBLISHER, config),
            self.load_metadata(PATHS::COUNTRY, config),
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

async fn get_country_names(config: &Config) -> anyhow::Result<Vec<String>> {
    Ok(reqwest::Client::new()
        .get(&format!("{}/countries.txt", config.base_path))
        .send()
        .await?
        .text()
        .await?
        .lines()
        .map(|s| s.to_string())
        .collect())
}

/// Load the metadata for a list of countries and merge them into
/// a single `Metadata` catalogue.
pub async fn load_all(config: &Config) -> Result<Metadata> {
    let country_names = get_country_names(config).await?;

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
}
