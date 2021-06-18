use std::collections::HashMap;

use dyn_conn::{Conn, DynConn, DynPoolOptions};
use std::sync::Mutex;
use ua_dao::{DaoMY, DaoOptions, DaoPG};

use crate::error::ServiceError;

pub struct ServiceConn(Conn);

impl From<ServiceConn> for DaoOptions {
    fn from(conn: ServiceConn) -> Self {
        match conn.0.pool {
            DynPoolOptions::Mysql(pool) => DaoOptions::MY(DaoMY { pool }),
            DynPoolOptions::Postgres(pool) => DaoOptions::PG(DaoPG { pool }),
        }
    }
}

pub struct ServiceDynConn {
    pub store: HashMap<String, DaoOptions>,
}

impl ServiceDynConn {
    pub fn new(dyn_conn: DynConn) -> Self {
        ServiceDynConn {
            store: dyn_conn
                .store
                .into_iter()
                .map(|(k, c)| (k, DaoOptions::from(ServiceConn(c))))
                .collect(),
        }
    }

    pub fn get_dao(&self, key: &str) -> Result<&DaoOptions, ServiceError> {
        if let Some(d) = self.store.get(key) {
            return Ok(d);
        }
        Err(ServiceError::DaoNotFoundError(key.to_owned()))
    }
}

pub type MutexServiceDynConn = Mutex<ServiceDynConn>;
