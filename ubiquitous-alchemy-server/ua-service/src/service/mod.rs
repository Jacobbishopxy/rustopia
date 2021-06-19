pub mod query;
pub mod schema;

use std::collections::HashMap;

use dyn_conn::{Conn, DynConn, DynPoolOptions};
use std::sync::Mutex;
use ua_dao::{DaoMY, DaoOptions, DaoPG};

use crate::error::ServiceError;

/// Conn's facade mode
pub struct ServiceConn(Conn);

/// conversion from DynPoolOptions to DaoOptions
impl From<ServiceConn> for DaoOptions {
    fn from(conn: ServiceConn) -> Self {
        match conn.0.pool {
            DynPoolOptions::Mysql(pool) => DaoOptions::MY(DaoMY { pool }),
            DynPoolOptions::Postgres(pool) => DaoOptions::PG(DaoPG { pool }),
        }
    }
}

/// main struct using as shared data among cross threads
pub struct ServiceDynConn {
    pub store: HashMap<String, DaoOptions>,
}

impl From<DynConn> for ServiceDynConn {
    fn from(dyn_conn: DynConn) -> Self {
        ServiceDynConn {
            store: dyn_conn
                .store
                .into_iter()
                .map(|(k, c)| (k, DaoOptions::from(ServiceConn(c))))
                .collect(),
        }
    }
}

// TODO: needs functions from DynConn: delete_conn, create_conn, update_conn, and etc.
impl ServiceDynConn {
    pub fn new(dyn_conn: DynConn) -> Self {
        ServiceDynConn::from(dyn_conn)
    }

    pub fn get_dao(&self, key: &str) -> Result<&DaoOptions, ServiceError> {
        if let Some(d) = self.store.get(key) {
            return Ok(d);
        }
        Err(ServiceError::DaoNotFoundError(key.to_owned()))
    }
}

pub type MutexServiceDynConn = Mutex<ServiceDynConn>;
