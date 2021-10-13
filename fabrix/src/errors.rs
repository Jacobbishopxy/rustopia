//! fabrix error type
//!
//! errors

use thiserror::Error;

pub type FabrixResult<T> = Result<T, FabrixError>;

#[derive(Debug)]
pub enum CommonError {
    Str(&'static str),
    String(String),
}

impl std::fmt::Display for CommonError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CommonError::Str(v) => write!(f, "{:?}", v),
            CommonError::String(v) => write!(f, "{:?}", v),
        }
    }
}

impl From<&'static str> for CommonError {
    fn from(v: &'static str) -> Self {
        CommonError::Str(v)
    }
}

impl From<String> for CommonError {
    fn from(v: String) -> Self {
        CommonError::String(v)
    }
}

#[derive(Error, Debug)]
pub enum FabrixError {
    #[error("common error {0}")]
    Common(CommonError),

    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),

    #[error(transparent)]
    Polars(#[from] polars::error::PolarsError),

    #[error(transparent)]
    ParseIntError(#[from] std::num::TryFromIntError),

    #[error("unknown error")]
    Unknown,
}

impl FabrixError {
    pub fn new_common_error<T>(msg: T) -> FabrixError
    where
        T: Into<CommonError>,
    {
        FabrixError::Common(msg.into())
    }
}
