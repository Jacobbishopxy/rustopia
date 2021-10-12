//! fabrix error type
//!
//! errors

use thiserror::Error;

pub type FabrixResult<T> = Result<T, FabrixError>;

#[derive(Error, Debug)]
pub enum FabrixError {
    #[error("common error {0}")]
    Common(&'static str),

    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),

    #[error(transparent)]
    Polars(#[from] polars::error::PolarsError),

    #[error(transparent)]
    ParseIntError(#[from] std::num::TryFromIntError),

    #[error("unknown error")]
    Unknown,
}
