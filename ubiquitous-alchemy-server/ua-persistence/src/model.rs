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
    /// constructor
    pub async fn new(conn: &str) -> Self {
        let rb = Rbatis::new();
        rb.link(conn)
            .await
            .expect("Persistence init DB connection failed");

        PersistenceDao {
            conn: conn.to_owned(),
            rb,
        }
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

    // string to uuid
    pub fn str_id_to_uuid(id: &str) -> Result<Uuid, Error> {
        Uuid::parse_str(id).map_err(|_| Error::E("uuid conversion error".to_owned()))
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
    pub async fn load(&self, id: &Uuid) -> Result<Option<ConnectionInformation>, Error> {
        let res: Option<ConnectionInformation> = self.rb.fetch_by_column("id", id).await?;
        Ok(res)
    }

    /// update an existing info
    pub async fn update(&self, conn_info: &ConnectionInformation) -> Result<u64, Error> {
        let mut conn_info = conn_info.clone();
        // let uuid = Uuid::parse_str(id).map_err(|_| Error::E("parse uuid error".to_owned()));
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
    const CONN: &'static str = "postgres://postgres:postgres@localhost:5432/dev";
    const TEST_UUID: &'static str = "02207087-ab01-4a57-ad8a-bcbcddf500ea";

    #[actix_rt::test]
    async fn init_table_test() {
        let pd = PersistenceDao::new(CONN).await;

        let res = pd.init_table().await;

        assert_matches!(res, Ok(_));
    }

    #[actix_rt::test]
    async fn save_test() {
        let pd = PersistenceDao::new(CONN).await;

        let conn_info = ConnectionInformation {
            id: Some(Uuid::parse_str(TEST_UUID).unwrap()),
            name: "Dev".to_owned(),
            description: None,
            driver: "postgres".to_owned(),
            username: "dev".to_owned(),
            password: "secret".to_owned(),
            host: "localhost".to_owned(),
            port: 5432,
            database: "dev".to_owned(),
        };

        assert_matches!(pd.save(&conn_info).await, Ok(_));
    }

    #[actix_rt::test]
    async fn load_all_test() {
        let pd = PersistenceDao::new(CONN).await;

        let res = pd.load_all().await;

        println!("{:?}", res);

        assert_matches!(res, Ok(_));
    }

    #[actix_rt::test]
    async fn load_test() {
        let pd = PersistenceDao::new(CONN).await;
        let id = Uuid::parse_str(TEST_UUID).unwrap();

        let res = pd.load(&id).await;

        println!("{:?}", res);

        assert_matches!(res, Ok(_));
    }

    #[actix_rt::test]
    async fn update_test() {
        let pd = PersistenceDao::new(CONN).await;

        let conn_info = ConnectionInformation {
            name: "Dev".to_owned(),
            description: Some("dev dev dev...".to_owned()),
            driver: "postgres".to_owned(),
            username: "dev_user".to_owned(),
            password: "secret".to_owned(),
            host: "localhost".to_owned(),
            port: 5432,
            database: "dev".to_owned(),
            id: Some(Uuid::parse_str(TEST_UUID).unwrap()),
        };

        assert_matches!(pd.update(&conn_info).await, Ok(_));
    }

    #[actix_rt::test]
    async fn delete_test() {
        let pd = PersistenceDao::new(CONN).await;

        let id = Uuid::parse_str(TEST_UUID).unwrap();

        assert_matches!(pd.delete(&id).await, Ok(_));
    }
}
