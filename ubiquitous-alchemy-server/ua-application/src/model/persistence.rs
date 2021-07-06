//! concrete methods implements persistence's interface

use async_trait::async_trait;

use dyn_conn::{ConnInfo, ConnStoreError, Driver, PersistenceFunctionality};
use ua_persistence::{ConnectionInformation, PersistenceDao};
pub struct UaPersistence(PersistenceDao);

impl UaPersistence {
    pub async fn new(conn: &str) -> Self {
        UaPersistence(PersistenceDao::new(conn).await)
    }
}

pub struct CI(ConnInfo);

impl From<ConnectionInformation> for CI {
    fn from(ci: ConnectionInformation) -> Self {
        let drv = if ci.driver == "postgres" {
            Driver::Postgres
        } else {
            Driver::Mysql
        };
        CI(ConnInfo {
            driver: drv,
            username: ci.username,
            password: ci.password,
            host: ci.host,
            port: ci.port,
            database: ci.database,
        })
    }
}

// TODO: 1. key/name/description 2. better type

#[async_trait]
impl PersistenceFunctionality for UaPersistence {
    async fn load(&self, key: &str) -> Result<ConnInfo, ConnStoreError> {
        let id = PersistenceDao::str_id_to_uuid(key)
            .map_err(|e| ConnStoreError::Exception(e.to_string()))?;

        if let Ok(oc) = self.0.load(&id).await {
            if let Some(c) = oc {
                return Ok(CI::from(c).0);
            }
        };

        Err(ConnStoreError::ConnNotFound(key.to_owned()))
    }

    async fn load_all(
        &self,
    ) -> Result<std::collections::HashMap<String, ConnInfo>, ConnStoreError> {
        if let Ok(vc) = self.0.load_all().await {
            let res = vc
                .into_iter()
                .map(|ci| (ci.id.unwrap().to_string(), CI::from(ci).0))
                .collect();
            return Ok(res);
        }

        Err(ConnStoreError::ConnFailed("Load all failed".to_owned()))
    }

    async fn save(&self, _: &str, conn: &ConnInfo) -> Result<(), ConnStoreError> {
        let c = conn.clone();
        let ci = ConnectionInformation {
            id: None,
            name: "".to_owned(),
            description: None,
            driver: if c.driver == Driver::Postgres {
                "postgres".to_owned()
            } else {
                "mysql".to_owned()
            },
            username: c.username,
            password: c.password,
            host: c.host,
            port: c.port,
            database: c.database,
        };

        if let Ok(_) = self.0.save(&ci).await {
            return Ok(());
        }

        Err(ConnStoreError::ConnFailed("Failed to save".to_owned()))
    }

    async fn update(&self, key: &str, conn: &ConnInfo) -> Result<(), ConnStoreError> {
        let id = PersistenceDao::str_id_to_uuid(key)
            .map_err(|e| ConnStoreError::Exception(e.to_string()))?;

        let c = conn.clone();
        let ci = ConnectionInformation {
            id: Some(id),
            name: "".to_owned(),
            description: None,
            driver: if c.driver == Driver::Postgres {
                "postgres".to_owned()
            } else {
                "mysql".to_owned()
            },
            username: c.username,
            password: c.password,
            host: c.host,
            port: c.port,
            database: c.database,
        };

        if let Ok(_) = self.0.update(&ci).await {
            return Ok(());
        }

        Err(ConnStoreError::ConnFailed("Failed to update".to_owned()))
    }

    async fn delete(&self, key: &str) -> Result<(), ConnStoreError> {
        let id = PersistenceDao::str_id_to_uuid(key)
            .map_err(|e| ConnStoreError::Exception(e.to_string()))?;

        if let Ok(_) = self.0.delete(&id).await {
            return Ok(());
        }

        Err(ConnStoreError::ConnNotFound(key.to_owned()))
    }
}
