//!

use derive_more::Display;
use serde::{Deserialize, Serialize};
use sqlx::error::Error as DBError;

#[derive(Debug, Display, Serialize, Deserialize)]
pub enum DaoError {
    #[display(fmt = "Database general error: {}", _0)]
    DatabaseGeneralError(String),

    #[display(fmt = "Database connection error: {}", _0)]
    DatabaseConnectionError(String),

    #[display(fmt = "Database operation error: {}", _0)]
    DatabaseOperationError(String),
}

impl From<DBError> for DaoError {
    fn from(error: DBError) -> Self {
        match error {
            DBError::Configuration(e) => DaoError::DatabaseConnectionError(e.to_string()),
            DBError::Database(e) => DaoError::DatabaseConnectionError(e.to_string()),
            DBError::Io(e) => DaoError::DatabaseConnectionError(e.to_string()),
            DBError::Tls(e) => DaoError::DatabaseConnectionError(e.to_string()),
            DBError::Protocol(e) => DaoError::DatabaseConnectionError(e.to_string()),
            DBError::RowNotFound => DaoError::DatabaseOperationError("row not found".to_owned()),
            DBError::TypeNotFound { type_name } => DaoError::DatabaseOperationError(type_name),
            DBError::ColumnIndexOutOfBounds { index, len } => DaoError::DatabaseOperationError(
                format!("index ({:?}) out of bounds ({:?})", index, len),
            ),
            DBError::ColumnNotFound(e) => {
                DaoError::DatabaseOperationError(format!("column not found ({:?})", e))
            }
            DBError::ColumnDecode { index, source } => DaoError::DatabaseOperationError(format!(
                "column ({:?}) decode error, {:?}",
                index,
                source.to_string()
            )),
            DBError::Decode(e) => {
                DaoError::DatabaseOperationError(format!("column decode error {:?}", e.to_string()))
            }
            DBError::PoolTimedOut => DaoError::DatabaseConnectionError("pool timed out".to_owned()),
            DBError::PoolClosed => DaoError::DatabaseConnectionError("pool closed".to_owned()),
            DBError::WorkerCrashed => {
                DaoError::DatabaseConnectionError("worker crashed".to_owned())
            }
            DBError::Migrate(_) => DaoError::DatabaseGeneralError("migration error".to_owned()),
            _ => DaoError::DatabaseGeneralError("Undefined error".to_owned()),
        }
    }
}
