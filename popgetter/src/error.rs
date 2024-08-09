//! Error types.

#[derive(thiserror::Error, Debug)]
pub enum PopgetterError {
    #[error("Connection failure.")]
    FailedConnection,
    #[error("Metric not found.")]
    MetricNotFound(String),
    #[error("Invalid search syntax: {0}")]
    InvalidSearchQuery(String),
    #[error("Non-existent geometry for metric requested: {0}")]
    NonExistentGeometry(String),
    #[error("Wrapped polars error: {0}")]
    PolarsError(#[from] polars::error::PolarsError),
    #[error("Unknown error.")]
    Unknown,
}

#[cfg(test)]
mod tests {
    use polars::error::{ErrString, PolarsError};

    use super::*;

    #[test]
    fn test_from_polars_error() {
        let polars_error = PolarsError::ShapeMismatch(ErrString::from("An example polars error"));
        let popgetter_error: PopgetterError = polars_error.into();
        println!("{}", popgetter_error);
    }
}
