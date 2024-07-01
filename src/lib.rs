use anyhow::Result;
use data_request_spec::DataRequestSpec;
use log::debug;
use metadata::Metadata;
use parquet::get_metrics;
use polars::{frame::DataFrame, prelude::DataFrameJoinOps};
use tokio::try_join;

use crate::{config::Config, geo::get_geometries};
pub mod config;
pub mod data_request_spec;
pub mod error;
pub mod geo;
pub mod metadata;
pub mod parquet;
pub mod search;

/// This class stores the eventual column names of all the metadata classes, which are used when
/// serialising the metadata to a dataframe. Note that this must be synchronised with column names
/// defined in the upstream metadata classes!
pub struct COL;

impl COL {
    pub const GEO_ID: &'static str = "GEO_ID";

    pub const COUNTRY_ID: &'static str = "country_id";
    pub const COUNTRY_NAME_SHORT_EN: &'static str = "country_name_short_en";
    pub const COUNTRY_NAME_OFFICIAL: &'static str = "country_name_official";
    pub const COUNTRY_ISO3: &'static str = "country_iso3";
    pub const COUNTRY_ISO2: &'static str = "country_iso2";
    pub const COUNTRY_ISO3166_2: &'static str = "country_iso3166_2";

    pub const PUBLISHER_ID: &'static str = "data_publisher_id";
    pub const PUBLISHER_NAME: &'static str = "data_publisher_name";
    pub const PUBLISHER_URL: &'static str = "data_publisher_url";
    pub const PUBLISHER_DESCRIPTION: &'static str = "data_publisher_description";
    pub const PUBLISHER_COUNTRIES_OF_INTEREST: &'static str =
        "data_publisher_countries_of_interest";

    pub const GEOMETRY_ID: &'static str = "geometry_id";
    pub const GEOMETRY_FILENAME_STEM: &'static str = "geometry_filename_stem";
    pub const GEOMETRY_VALIDITY_PERIOD_START: &'static str = "geometry_validity_period_start";
    pub const GEOMETRY_VALIDITY_PERIOD_END: &'static str = "geometry_validity_period_end";
    pub const GEOMETRY_LEVEL: &'static str = "geometry_level";
    pub const GEOMETRY_HXL_TAG: &'static str = "geometry_hxl_tag";

    pub const SOURCE_ID: &'static str = "source_id";
    pub const SOURCE_NAME: &'static str = "source_name";
    pub const SOURCE_DATE_PUBLISHED: &'static str = "source_date_published";
    pub const SOURCE_REFERENCE_PERIOD_START: &'static str = "source_reference_period_start";
    pub const SOURCE_REFERENCE_PERIOD_END: &'static str = "source_reference_period_end";
    pub const SOURCE_COLLECTION_PERIOD_START: &'static str = "source_collection_period_start";
    pub const SOURCE_COLLECTION_PERIOD_END: &'static str = "source_collection_period_end";
    pub const SOURCE_EXPECT_NEXT_UPDATE: &'static str = "source_expect_next_update";
    pub const SOURCE_URL: &'static str = "source_url";
    pub const SOURCE_DATA_PUBLISHER_ID: &'static str = "source_data_publisher_id";
    pub const SOURCE_DESCRIPTION: &'static str = "source_description";
    pub const SOURCE_GEOMETRY_METADATA_ID: &'static str = "source_geometry_metadata_id";

    pub const METRIC_ID: &'static str = "metric_id";
    pub const METRIC_HUMAN_READABLE_NAME: &'static str = "metric_human_readable_name";
    pub const METRIC_SOURCE_METRIC_ID: &'static str = "metric_source_id";
    pub const METRIC_DESCRIPTION: &'static str = "metric_description";
    pub const METRIC_HXL_TAG: &'static str = "metric_hxl_tag";
    pub const METRIC_PARQUET_PATH: &'static str = "metric_parquet_path";
    pub const METRIC_PARQUET_COLUMN_NAME: &'static str = "metric_parquet_column_name";
    pub const METRIC_PARQUET_MARGIN_OF_ERROR_COLUMN: &'static str =
        "metric_parquet_margin_of_error_column";
    pub const METRIC_PARQUET_MARGIN_OF_ERROR_FILE: &'static str =
        "metric_parquet_margin_of_error_file";
    pub const METRIC_POTENTIAL_DENOMINATOR_IDS: &'static str = "metric_potential_denominator_ids";
    pub const METRIC_PARENT_METRIC_ID: &'static str = "metric_parent_id";
    pub const METRIC_SOURCE_DATA_RELEASE_ID: &'static str = "metric_source_data_release_id";
    pub const METRIC_SOURCE_DOWNLOAD_URL: &'static str = "metric_source_download_url";
    pub const METRIC_SOURCE_ARCHIVE_FILE_PATH: &'static str = "metric_source_archive_file_path";
    pub const METRIC_SOURCE_DOCUMENTATION_URL: &'static str = "metric_source_documentation_url";
}

#[cfg(feature = "formatters")]
pub mod formatters;

pub struct Popgetter {
    pub metadata: Metadata,
    pub config: Config,
}

impl Popgetter {
    /// Setup the Popgetter object with default configuration
    pub async fn new() -> Result<Self> {
        Self::new_with_config(Config::default()).await
    }

    /// Setup the Popgetter object with custom configuration
    pub async fn new_with_config(config: Config) -> Result<Self> {
        debug!("config: {config:?}");
        let metadata = metadata::load_all(&config).await?;
        Ok(Self { metadata, config })
    }

    // Given a Data Request Spec
    // Return a DataFrame of the selected dataset
    pub async fn get_data_request(&self, data_request: &DataRequestSpec) -> Result<DataFrame> {
        let metric_requests = data_request.metric_requests(&self.metadata, &self.config)?;
        debug!("{:#?}", metric_requests);
        // Required because polars is blocking
        let metrics =
            tokio::task::spawn_blocking(move || get_metrics(&metric_requests.metrics, None));

        let geom_file = self
            .metadata
            .get_geom_details(&metric_requests.selected_geometry, &self.config)?;
        let geoms = get_geometries(&geom_file, None, None);

        // try_join requires us to have the errors from all futures be the same.
        // We use anyhow to get it back properly
        let (metrics, geoms) = try_join!(
            async move { metrics.await.map_err(anyhow::Error::from) },
            geoms
        )?;
        debug!("geoms: {geoms:#?}");
        debug!("metrics: {metrics:#?}");

        let result = geoms.inner_join(&metrics?, [COL::GEO_ID], [COL::GEO_ID])?;
        Ok(result)
    }
}
