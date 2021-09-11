//! tiny-df db error
//!
//! errors

use thiserror::Error;

// use crate::prelude::*;

pub type TdDbResult<T> = Result<T, TdDbError>;

#[derive(Error, Debug)]
pub enum TdDbError {
    #[error("common error {0}")]
    Common(&'static str),

    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),

    #[error("unknown error")]
    Unknown,
}
