use std::collections::HashSet;
#[cfg(feature = "cache")]
use std::path::Path;

#[cfg(feature = "cache")]
use anyhow::{anyhow, Context};
use anyhow::{bail, Result};
use data_request_spec::DataRequestSpec;
use geo::get_geometries;
use itertools::Itertools;
use log::debug;
#[cfg(feature = "cache")]
use log::error;
use metadata::Metadata;
use parquet::get_metrics_sql;
use polars::frame::DataFrame;
use search::{Params, SearchParams, SearchResults};

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

/// Type for popgetter metadata, config and API
#[derive(Debug, PartialEq)]
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

    // Only include method with "cache" feature since it requires a filesystem
    #[cfg(feature = "cache")]
    /// Setup the Popgetter object with custom configuration from cache
    pub async fn new_with_config_and_cache(config: Config) -> Result<Self> {
        // On macOS: ~/Library/Caches
        let path = dirs::cache_dir()
            .ok_or(anyhow!("Failed to get cache directory"))?
            .join("popgetter");
        Popgetter::new_with_config_and_cache_path(config, path).await
    }

    // Only include method with "cache" feature since it requires a filesystem
    #[cfg(feature = "cache")]
    async fn new_with_config_and_cache_path<P: AsRef<Path>>(
        config: Config,
        path: P,
    ) -> Result<Self> {
        // Try to read metadata from cache
        if path.as_ref().exists() {
            match Popgetter::new_from_cache_path(config.clone(), &path) {
                Ok(popgetter) => return Ok(popgetter),
                Err(err) => {
                    // Log error, continue without cache and attempt to create one
                    error!("Failed to read metadata from cache with error: {err}");
                }
            }
        }
        // If no metadata cache, get metadata and try to cache
        std::fs::create_dir_all(&path)?;
        let popgetter = Popgetter::new_with_config(config).await?;
        // If error creating cache, remove cache path
        if let Err(err) = popgetter.metadata.write_cache(&path) {
            std::fs::remove_dir_all(&path).with_context(|| {
                "Failed to remove cache dir following error writing cache: {err}"
            })?;
            Err(err)?
        }
        Ok(popgetter)
    }

    // Only include method with "cache" feature since it requires a filesystem
    #[cfg(feature = "cache")]
    fn new_from_cache_path<P: AsRef<Path>>(config: Config, path: P) -> Result<Self> {
        let metadata = Metadata::from_cache(path)?;
        Ok(Self { metadata, config })
    }

    /// Generates `SearchResults` using popgetter given `SearchParams`
    // TODO: consider reverting to an API where `SearchParams` are moved, add benches
    pub fn search(&self, search_params: &SearchParams) -> SearchResults {
        search_params
            .clone()
            .search(&self.metadata.combined_metric_source_geometry())
    }

    /// Downloads data using popgetter given a `DataRequestSpec`
    pub async fn download_data_request_spec(
        &self,
        data_request_spec: &DataRequestSpec,
    ) -> Result<DataFrame> {
        let params: Params = data_request_spec.clone().try_into()?;
        let search_results = self.search(&params.search);
        search_results
            .download(&self.config, &params.download)
            .await
    }

    /// Downloads data using popgetter given `Params`
    pub async fn download_params(&self, params: &Params) -> Result<DataFrame> {
        self.search(&params.search)
            .download(&self.config, &params.download)
            .await
    }

    pub async fn download_metrics_sql(&self, params: &Params) -> Result<String> {
        let metric_requests = self.search(&params.search).to_metric_requests(&self.config);
        get_metrics_sql(&metric_requests, None)
    }

    pub async fn download_geoms(&self, params: &Params) -> Result<DataFrame> {
        let metric_requests = self.search(&params.search).to_metric_requests(&self.config);
        let all_geom_files: HashSet<String> = metric_requests
            .iter()
            .map(|m| m.geom_file.clone())
            .collect();
        let bbox = params
            .download
            .region_spec
            .first()
            .and_then(|region_spec| region_spec.bbox().clone());
        if all_geom_files.len().ne(&1) {
            bail!(
                "Exactly 1 geom file is currently supported, {} included in metric requests: {:?}",
                all_geom_files.len(),
                all_geom_files.into_iter().collect_vec().join(", ")
            );
        }
        get_geometries(all_geom_files.iter().next().unwrap(), bbox).await
    }
}

#[cfg(test)]
#[cfg(feature = "cache")]
mod tests {

    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn test_popgetter_cache() -> anyhow::Result<()> {
        let tempdir = TempDir::new()?;
        let config = Config::default();
        let popgetter = Popgetter::new_with_config(config.clone()).await?;
        popgetter.metadata.write_cache(&tempdir)?;
        let popgetter_from_cache =
            Popgetter::new_with_config_and_cache_path(config, tempdir).await?;
        assert_eq!(popgetter, popgetter_from_cache);
        Ok(())
    }
}
