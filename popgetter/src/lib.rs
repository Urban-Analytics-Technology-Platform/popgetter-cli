use anyhow::Result;
use log::debug;
use metadata::Metadata;
use search::{SearchParams, SearchResults};

use crate::config::Config;

// Re-exports
pub use column_names as COL;

// Modules
pub mod column_names;
pub mod config;
pub mod data_request_spec;
pub mod error;
#[cfg(feature = "formatters")]
pub mod formatters;
pub mod geo;
pub mod metadata;
pub mod parquet;
pub mod search;

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

    /// Generates `SearchResults` using popgetter given `SearchParams`
    pub fn search(&self, search_params: SearchParams) -> SearchResults {
        search_params.search(&self.metadata.combined_metric_source_geometry())
    }
}
