//! This module stores the eventual column names of all the metadata classes, which are used when
//! serialising the metadata to a dataframe. Note that this must be synchronised with column names
//! defined in the upstream metadata classes!

pub const GEO_ID: &str = "GEO_ID";

pub const COUNTRY_ID: &str = "country_id";
pub const COUNTRY_NAME_SHORT_EN: &str = "country_name_short_en";
pub const COUNTRY_NAME_OFFICIAL: &str = "country_name_official";
pub const COUNTRY_ISO3: &str = "country_iso3";
pub const COUNTRY_ISO2: &str = "country_iso2";
pub const COUNTRY_ISO3166_2: &str = "country_iso3166_2";

pub const DATA_PUBLISHER_ID: &str = "data_publisher_id";
pub const DATA_PUBLISHER_NAME: &str = "data_publisher_name";
pub const DATA_PUBLISHER_URL: &str = "data_publisher_url";
pub const DATA_PUBLISHER_DESCRIPTION: &str = "data_publisher_description";
pub const DATA_PUBLISHER_COUNTRIES_OF_INTEREST: &str = "data_publisher_countries_of_interest";

pub const GEOMETRY_ID: &str = "geometry_id";
pub const GEOMETRY_FILEPATH_STEM: &str = "geometry_filepath_stem";
pub const GEOMETRY_VALIDITY_PERIOD_START: &str = "geometry_validity_period_start";
pub const GEOMETRY_VALIDITY_PERIOD_END: &str = "geometry_validity_period_end";
pub const GEOMETRY_LEVEL: &str = "geometry_level";
pub const GEOMETRY_HXL_TAG: &str = "geometry_hxl_tag";

pub const SOURCE_DATA_RELEASE_ID: &str = "source_data_release_id";
pub const SOURCE_DATA_RELEASE_NAME: &str = "source_data_release_name";
pub const SOURCE_DATA_RELEASE_DATE_PUBLISHED: &str = "source_data_release_date_published";
pub const SOURCE_DATA_RELEASE_REFERENCE_PERIOD_START: &str =
    "source_data_release_reference_period_start";
pub const SOURCE_DATA_RELEASE_REFERENCE_PERIOD_END: &str =
    "source_data_release_reference_period_end";
pub const SOURCE_DATA_RELEASE_COLLECTION_PERIOD_START: &str =
    "source_data_release_collection_period_start";
pub const SOURCE_DATA_RELEASE_COLLECTION_PERIOD_END: &str =
    "source_data_release_collection_period_end";
pub const SOURCE_DATA_RELEASE_EXPECT_NEXT_UPDATE: &str = "source_data_release_expect_next_update";
pub const SOURCE_DATA_RELEASE_URL: &str = "source_data_release_url";
pub const SOURCE_DATA_RELEASE_DATA_PUBLISHER_ID: &str = "source_data_release_data_publisher_id";
pub const SOURCE_DATA_RELEASE_DESCRIPTION: &str = "source_data_release_description";
pub const SOURCE_DATA_RELEASE_GEOMETRY_METADATA_ID: &str =
    "source_data_release_geometry_metadata_id";

pub const METRIC_ID: &str = "metric_id";
pub const METRIC_HUMAN_READABLE_NAME: &str = "metric_human_readable_name";
pub const METRIC_SOURCE_METRIC_ID: &str = "metric_source_id";
pub const METRIC_DESCRIPTION: &str = "metric_description";
pub const METRIC_HXL_TAG: &str = "metric_hxl_tag";
pub const METRIC_PARQUET_PATH: &str = "metric_parquet_path";
pub const METRIC_PARQUET_COLUMN_NAME: &str = "metric_parquet_column_name";
pub const METRIC_PARQUET_MARGIN_OF_ERROR_COLUMN: &str = "metric_parquet_margin_of_error_column";
pub const METRIC_PARQUET_MARGIN_OF_ERROR_FILE: &str = "metric_parquet_margin_of_error_file";
pub const METRIC_POTENTIAL_DENOMINATOR_IDS: &str = "metric_potential_denominator_ids";
pub const METRIC_PARENT_METRIC_ID: &str = "metric_parent_metric_id";
pub const METRIC_SOURCE_DATA_RELEASE_ID: &str = "metric_source_data_release_id";
pub const METRIC_SOURCE_DOWNLOAD_URL: &str = "metric_source_download_url";
pub const METRIC_SOURCE_ARCHIVE_FILE_PATH: &str = "metric_source_archive_file_path";
pub const METRIC_SOURCE_DOCUMENTATION_URL: &str = "metric_source_documentation_url";
