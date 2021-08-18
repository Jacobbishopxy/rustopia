use thiserror::Error;

pub type XlzResult<T> = Result<T, XlzError>;

#[derive(Error, Debug)]
pub enum XlzError {
    #[error("common error {0}")]
    CommonError(String),
    #[error(transparent)]
    StdIOError(#[from] std::io::Error),
    #[error(transparent)]
    ZipError(#[from] zip::result::ZipError),
    #[error("unknown error")]
    Unknown,
}
