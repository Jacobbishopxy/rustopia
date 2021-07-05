//! data persistence

use rbatis::{
    core::db::DBExecResult, crud::CRUD, crud_table, executor::Executor, rbatis::Rbatis, Error,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[crud_table(table_name:conn_info | formats_pg:"id:{}::uuid")]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ConnectionInformation {
    pub id: Option<Uuid>,
    pub name: String,
    pub description: Option<String>,
    pub driver: String,
    pub username: String,
    pub password: String,
    pub host: String,
    pub port: i32,
    pub database: String,
}

pub struct PersistenceDao {
    pub conn: String,
    rb: Rbatis,
}

impl PersistenceDao {
    /// initialization
    pub async fn new(conn: &str) -> Self {
        let rb = Rbatis::new();
        rb.link(conn)
            .await
            .expect("Persistence init DB connection failed");

        // TODO: move table initiation to build-dependencies
        let init_table = r##"
        CREATE TABLE IF NOT EXISTS
        conn_info(
            id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
            name VARCHAR,
            description TEXT,
            driver VARCHAR,
            username VARCHAR,
            password VARCHAR,
            host VARCHAR,
            port INT,
            database VARCHAR
        );
        "##;

        rb.exec(init_table, &vec![])
            .await
            .expect("Init table failed");

        PersistenceDao {
            conn: conn.to_owned(),
            rb,
        }
    }

    /// save info to DB
    pub async fn save(&self, conn_info: &ConnectionInformation) -> Result<DBExecResult, Error> {
        self.rb.save(conn_info).await
    }

    /// load all info from DB
    pub async fn load_all(&self) -> Result<Vec<ConnectionInformation>, Error> {
        let res: Vec<ConnectionInformation> = self.rb.fetch_list().await?;
        Ok(res)
    }

    /// load an info by id
    pub async fn load(&self, id: &str) -> Result<Option<ConnectionInformation>, Error> {
        let res: Option<ConnectionInformation> =
            self.rb.fetch_by_column("id", &id.to_string()).await?;
        Ok(res)
    }

    /// update an existing info
    pub async fn update(&self, id: &str, conn_info: &ConnectionInformation) -> Result<u64, Error> {
        let mut conn_info = conn_info.clone();
        let w = self.rb.new_wrapper().eq("id", id);
        self.rb
            .update_by_wrapper(&mut conn_info, &w, &["id", "null"])
            .await
    }

    /// delete an info from DB
    pub async fn delete(&self, id: &str) -> Result<u64, Error> {
        self.rb
            .remove_by_column::<ConnectionInformation, _>("id", &id.to_string())
            .await
    }
}
