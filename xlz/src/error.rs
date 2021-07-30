use thiserror::Error;

pub type XlzResult<T> = Result<T, XlzError>;

#[derive(Error, Debug)]
pub enum XlzError {
    #[error("common error {0}")]
    CommonError(String),
    #[error("std io error")]
    StdIOError(#[from] std::io::Error),
    #[error("zip error")]
    ZipError(#[from] zip::result::ZipError),
    #[error("unknown error")]
    Unknown,
}
