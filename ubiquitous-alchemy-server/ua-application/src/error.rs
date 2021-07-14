//!

use actix_web::{dev, error::ResponseError, HttpResponse};
use derive_more::Display;

use dyn_conn::ConnStoreError;
use ua_service::DaoError;

#[derive(Debug, Display)]
pub enum ServiceError {
    #[display(fmt = "Dao error {}", _0)]
    DaoError(DaoError),

    #[display(fmt = "Dao not found {}", _0)]
    DaoNotFoundError(String),

    #[display(fmt = "Dao already exist {}", _0)]
    DaoAlreadyExistError(String),

    #[display(fmt = "Internal server error")]
    InternalServerError,

    #[display(fmt = "Bad Request: {}", _0)]
    BadRequest(String),
}

impl From<DaoError> for ServiceError {
    fn from(error: DaoError) -> Self {
        match error {
            e @ DaoError::DatabaseGeneralError(_) => ServiceError::DaoError(e),
            e @ DaoError::DatabaseConnectionError(_) => ServiceError::DaoError(e),
            e @ DaoError::DatabaseOperationError(_) => ServiceError::DaoError(e),
        }
    }
}

impl From<ConnStoreError> for ServiceError {
    fn from(error: ConnStoreError) -> Self {
        match error {
            ConnStoreError::ConnNotFound(e) => ServiceError::DaoNotFoundError(e),
            ConnStoreError::ConnAlreadyExists(e) => ServiceError::DaoAlreadyExistError(e),
            ConnStoreError::ConnFailed(e) => {
                ServiceError::DaoError(DaoError::DatabaseConnectionError(e))
            }
            ConnStoreError::Exception(e) => ServiceError::BadRequest(e),
        }
    }
}

impl ResponseError for ServiceError {
    fn error_response(&self) -> HttpResponse<actix_web::dev::Body> {
        match self {
            ServiceError::DaoError(e) => {
                let e_s = serde_json::to_string(e)
                    .unwrap_or("message cannot be converted to string".to_owned());
                HttpResponse::InternalServerError().body(dev::Body::from_message(e_s))
            }
            ServiceError::DaoNotFoundError(s) => {
                HttpResponse::BadRequest().body(dev::Body::from_message(s.to_owned()))
            }
            ServiceError::DaoAlreadyExistError(s) => {
                HttpResponse::BadRequest().body(dev::Body::from_message(s.to_owned()))
            }
            ServiceError::InternalServerError => HttpResponse::InternalServerError()
                .body(dev::Body::from_message("Internal Server Error")),
            ServiceError::BadRequest(e) => {
                HttpResponse::BadRequest().body(dev::Body::from_message(e.to_owned()))
            }
        }
    }
}
