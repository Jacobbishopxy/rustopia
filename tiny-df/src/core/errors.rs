//! tiny-df core error
//!
//! errors

use thiserror::Error;

pub type TdCoreResult<T> = Result<T, TdCoreError>;

#[derive(Error, Debug)]
pub enum TdCoreError {
    #[error("common error {0}")]
    Common(&'static str),

    #[error("data convert error")]
    DataConvert,
}
