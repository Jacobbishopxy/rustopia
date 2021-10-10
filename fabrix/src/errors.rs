//! fabrix error type
//!
//! errors

use thiserror::Error;

pub type FResult<T> = Result<T, FError>;

#[derive(Error, Debug)]
pub enum FError {
    #[error("common error {0}")]
    Common(&'static str),

    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),

    #[error(transparent)]
    Polars(#[from] polars::error::PolarsError),

    #[error("unknown error")]
    Unknown,
}
