//! data persistence

use std::time::Duration;

use rbatis::{
    core::db::{DBExecResult, DBPoolOptions},
    crud::CRUD,
    crud_table,
    executor::Executor,
    rbatis::Rbatis,
    Error,
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
    /// constructor
    pub async fn new(conn: &str) -> Result<Self, Error> {
        let rb = Rbatis::new();
        let mut opt = DBPoolOptions::new();

        // TODO: expose opt
        opt.connect_timeout = Duration::new(5, 0);
        rb.link_opt(conn, &opt).await?;

        let res = PersistenceDao {
            conn: conn.to_owned(),
            rb,
        };

        Ok(res)
    }

    /// initialize table
    pub async fn init_table(&self) -> Result<DBExecResult, Error> {
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

        self.rb.exec(init_table, &vec![]).await
    }

    /// save info to DB
    pub async fn save(
        &self,
        id: &Uuid,
        conn_info: &ConnectionInformation,
    ) -> Result<DBExecResult, Error> {
        let conn_info = ConnectionInformation {
            id: Some(id.clone()),
            ..conn_info.clone()
        };
        self.rb.save(&conn_info).await
    }

    /// load all info from DB
    pub async fn load_all(&self) -> Result<Vec<ConnectionInformation>, Error> {
        let res: Vec<ConnectionInformation> = self.rb.fetch_list().await?;
        Ok(res)
    }

    /// load an info by id
    pub async fn load(&self, id: &Uuid) -> Result<Option<ConnectionInformation>, Error> {
        let res: Option<ConnectionInformation> = self.rb.fetch_by_column("id", id).await?;
        Ok(res)
    }

    /// update an existing info
    pub async fn update(&self, id: &Uuid, conn_info: &ConnectionInformation) -> Result<u64, Error> {
        let conn_info = ConnectionInformation {
            id: Some(id.clone()),
            ..conn_info.clone()
        };
        let mut conn_info = conn_info.clone();
        self.rb.update_by_column("id", &mut conn_info).await
    }

    /// delete an info from DB
    pub async fn delete(&self, id: &Uuid) -> Result<u64, Error> {
        self.rb
            .remove_by_column::<ConnectionInformation, _>("id", id)
            .await
    }
}

#[cfg(test)]
mod persistence_test {

    use super::*;

    // replace it to your own connection string
    const CONN: &'static str = "postgres://root:secret@localhost:5432/ubiquitous_alchemy";
    const TEST_UUID: &'static str = "02207087-ab01-4a57-ad8a-bcbcddf500ea";

    #[actix_rt::test]
    async fn init_table_test() {
        let pd = PersistenceDao::new(CONN).await.unwrap();

        let res = pd.init_table().await;

        assert_matches!(res, Ok(_));
    }

    #[actix_rt::test]
    async fn save_test() {
        let pd = PersistenceDao::new(CONN).await.unwrap();
        let id = Uuid::parse_str(TEST_UUID).unwrap();

        let conn_info = ConnectionInformation {
            id: Some(id.clone()),
            name: "Dev".to_owned(),
            description: None,
            driver: "postgres".to_owned(),
            username: "dev".to_owned(),
            password: "secret".to_owned(),
            host: "localhost".to_owned(),
            port: 5432,
            database: "dev".to_owned(),
        };

        assert_matches!(pd.save(&id, &conn_info).await, Ok(_));
    }

    #[actix_rt::test]
    async fn load_all_test() {
        let pd = PersistenceDao::new(CONN).await.unwrap();

        let res = pd.load_all().await;

        println!("{:?}", res);

        assert_matches!(res, Ok(_));
    }

    #[actix_rt::test]
    async fn load_test() {
        let pd = PersistenceDao::new(CONN).await.unwrap();
        let id = Uuid::parse_str(TEST_UUID).unwrap();

        let res = pd.load(&id).await;

        println!("{:?}", res);

        assert_matches!(res, Ok(_));
    }

    #[actix_rt::test]
    async fn update_test() {
        let pd = PersistenceDao::new(CONN).await.unwrap();
        let id = Uuid::parse_str(TEST_UUID).unwrap();

        let conn_info = ConnectionInformation {
            id: Some(id.clone()),
            name: "Dev".to_owned(),
            description: Some("dev dev dev...".to_owned()),
            driver: "postgres".to_owned(),
            username: "dev_user".to_owned(),
            password: "secret".to_owned(),
            host: "localhost".to_owned(),
            port: 5432,
            database: "dev".to_owned(),
        };

        assert_matches!(pd.update(&id, &conn_info).await, Ok(_));
    }

    #[actix_rt::test]
    async fn delete_test() {
        let pd = PersistenceDao::new(CONN).await.unwrap();

        let id = Uuid::parse_str(TEST_UUID).unwrap();

        assert_matches!(pd.delete(&id).await, Ok(_));
    }
}
