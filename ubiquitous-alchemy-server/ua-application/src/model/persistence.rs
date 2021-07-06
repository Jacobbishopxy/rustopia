//! concrete methods implements persistence's interface

use async_trait::async_trait;

use dyn_conn::{ConnStoreError, PersistenceFunctionality};
use ua_persistence::PersistenceDao;

use super::biz_model::CI;

pub struct UaPersistence(PersistenceDao);

impl UaPersistence {
    pub async fn new(conn: &str) -> Self {
        UaPersistence(PersistenceDao::new(conn).await)
    }

    pub async fn init_table(&self) -> Result<(), ConnStoreError> {
        self.0
            .init_table()
            .await
            .map(|_| ())
            .map_err(|_| ConnStoreError::Exception("Init table failed".to_owned()))
    }
}

#[async_trait]
impl PersistenceFunctionality<CI> for UaPersistence {
    async fn load(&self, key: &str) -> Result<CI, ConnStoreError> {
        let id = PersistenceDao::str_id_to_uuid(key)
            .map_err(|e| ConnStoreError::Exception(e.to_string()))?;

        if let Ok(oc) = self.0.load(&id).await {
            if let Some(c) = oc {
                return Ok(CI::from(c));
            }
        };

        Err(ConnStoreError::ConnNotFound(key.to_owned()))
    }

    async fn load_all(&self) -> Result<std::collections::HashMap<String, CI>, ConnStoreError> {
        if let Ok(vc) = self.0.load_all().await {
            let res = vc
                .into_iter()
                .map(|ci| (ci.id.unwrap().to_string(), CI::from(ci)))
                .collect();
            return Ok(res);
        }

        Err(ConnStoreError::ConnFailed("Load all failed".to_owned()))
    }

    async fn save(&self, conn: &CI) -> Result<(), ConnStoreError> {
        if let Ok(_) = self.0.save(&conn.ci()).await {
            return Ok(());
        }

        Err(ConnStoreError::ConnFailed("Failed to save".to_owned()))
    }

    async fn update(&self, conn: &CI) -> Result<(), ConnStoreError> {
        if let Ok(_) = self.0.update(&conn.ci()).await {
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
