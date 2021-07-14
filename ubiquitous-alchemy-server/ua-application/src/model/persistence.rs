//! concrete methods implements persistence's interface

use std::collections::HashMap;
use uuid::Uuid;

use async_trait::async_trait;

use dyn_conn::{ConnStoreError, PersistenceFunctionality};
use ua_persistence::PersistenceDao;

use super::biz_model::CI;

pub struct UaPersistence(PersistenceDao);

impl UaPersistence {
    pub async fn new(conn: &str) -> Result<Self, ConnStoreError> {
        let pd = PersistenceDao::new(conn).await.map_err(|_| {
            ConnStoreError::ConnFailed("Persistence storage connection failed".to_owned())
        })?;
        Ok(UaPersistence(pd))
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
    async fn load_all(&self) -> Result<HashMap<Uuid, CI>, ConnStoreError> {
        if let Ok(vc) = self.0.load_all().await {
            let res = vc
                .into_iter()
                .map(|ci| (ci.id.unwrap(), CI::from(ci)))
                .collect();
            return Ok(res);
        }

        Err(ConnStoreError::ConnFailed("Load all failed".to_owned()))
    }

    async fn save(&self, key: &Uuid, conn: &CI) -> Result<(), ConnStoreError> {
        if let Ok(_) = self.0.save(key, &conn.ci()).await {
            return Ok(());
        }

        Err(ConnStoreError::ConnFailed("Failed to save".to_owned()))
    }

    async fn update(&self, key: &Uuid, conn: &CI) -> Result<(), ConnStoreError> {
        if let Ok(_) = self.0.update(key, &conn.ci()).await {
            return Ok(());
        }

        Err(ConnStoreError::ConnFailed("Failed to update".to_owned()))
    }

    async fn delete(&self, key: &Uuid) -> Result<(), ConnStoreError> {
        if let Ok(_) = self.0.delete(key).await {
            return Ok(());
        }

        Err(ConnStoreError::ConnNotFound(key.to_string()))
    }
}
