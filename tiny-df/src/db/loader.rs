//! tiny-df sql engine
//!
//! Similar to Python's pandas dataframe: `pd.Dataframe.to_sql`

use async_trait::async_trait;
use sqlx::mysql::MySqlRow;
use sqlx::postgres::PgRow;
use sqlx::sqlite::SqliteRow;
use sqlx::{MySqlPool, PgPool, Row, SqlitePool};

use super::types::*;
use crate::db::{ConnInfo, TdDbError, TdDbResult};
use crate::prelude::*;
use crate::se::Sql;

/// Loader's engine
/// fetching data from database
#[async_trait]
pub trait Engine<DF, COL> {
    async fn get_table_schema(&self, table: &str) -> TdDbResult<Vec<COL>>;

    /// fetch all data by a query string, and turn result into a `Dataframe` (strict mode)
    async fn fetch_all(&self, query: &str) -> TdDbResult<Option<DF>>;

    async fn insert(&self, table_name: &str, dataframe: Dataframe) -> TdDbResult<()>;

    // async fn update(&self, dataframe: Dataframe) -> TdDbResult<()>;

    // async fn upsert(&self, dataframe: Dataframe) -> TdDbResult<()>;

    // async fn transaction(&self)
}

#[async_trait]
impl Engine<Dataframe, DataframeColumn> for MySqlPool {
    async fn get_table_schema(&self, table: &str) -> TdDbResult<Vec<DataframeColumn>> {
        // get query string for mysql
        let query = Sql::Mysql.check_table_schema(table);

        let schema = sqlx::query(&query)
            .map(|row: MySqlRow| -> DataframeColumn {
                // get column name & type
                let name: String = row.get(0);
                let data_type: String = row.get(1);
                let data_type: DataType = SqlColumnType::Mysql(&data_type).into();

                DataframeColumn::new(name, data_type)
            })
            .fetch_all(self)
            .await?;

        Ok(schema)
    }

    async fn fetch_all(&self, query: &str) -> TdDbResult<Option<Dataframe>> {
        let mut columns = vec![];
        let mut should_update_col = true;

        // `Vec<RowVec>`
        let mut d2: D2 = sqlx::query(query)
            .try_map(|row: MySqlRow| {
                if should_update_col {
                    columns = row_cols_name_mysql(&row);
                    should_update_col = false;
                }
                row_to_d1_mysql(row)
            })
            .fetch_all(self)
            .await?;

        d2.insert(0, columns);

        Ok(Some(Dataframe::from_vec(d2, "h")))
    }

    async fn insert(&self, table_name: &str, dataframe: Dataframe) -> TdDbResult<()> {
        todo!()
    }
}

#[async_trait]
impl Engine<Dataframe, DataframeColumn> for PgPool {
    async fn get_table_schema(&self, table: &str) -> TdDbResult<Vec<DataframeColumn>> {
        // get query string for pg
        let query = Sql::Postgres.check_table_schema(table);

        let schema = sqlx::query(&query)
            .map(|row: PgRow| -> DataframeColumn {
                // get column name & type
                let name: String = row.get(0);
                let data_type: String = row.get(1);
                let data_type: DataType = SqlColumnType::Postgres(&data_type).into();

                DataframeColumn::new(name, data_type)
            })
            .fetch_all(self)
            .await?;

        Ok(schema)
    }

    async fn fetch_all(&self, query: &str) -> TdDbResult<Option<Dataframe>> {
        let mut columns = vec![];
        let mut should_update_col = true;

        let mut d2 = sqlx::query(query)
            .try_map(|row: PgRow| {
                if should_update_col {
                    columns = row_cols_name_pg(&row);
                    should_update_col = false;
                }
                row_to_d1_pg(row)
            })
            .fetch_all(self)
            .await?;

        d2.insert(0, columns);

        Ok(Some(Dataframe::from_vec(d2, "h")))
    }

    async fn insert(&self, table_name: &str, dataframe: Dataframe) -> TdDbResult<()> {
        todo!()
    }
}

#[async_trait]
impl Engine<Dataframe, DataframeColumn> for SqlitePool {
    async fn get_table_schema(&self, table: &str) -> TdDbResult<Vec<DataframeColumn>> {
        // get query string for sqlite
        let query = Sql::Sqlite.check_table_schema(table);

        let schema = sqlx::query(&query)
            .map(|row: SqliteRow| -> DataframeColumn {
                // get column name & type
                let name: String = row.get(0);
                let data_type: String = row.get(1);
                let data_type: DataType = SqlColumnType::Sqlite(&data_type).into();

                DataframeColumn::new(name, data_type)
            })
            .fetch_all(self)
            .await?;

        Ok(schema)
    }

    async fn fetch_all(&self, query: &str) -> TdDbResult<Option<Dataframe>> {
        let mut columns = vec![];
        let mut should_update_col = true;

        let mut d2 = sqlx::query(query)
            .try_map(|row: SqliteRow| {
                if should_update_col {
                    columns = row_cols_name_sqlite(&row);
                    should_update_col = false;
                }
                row_to_d1_sqlite(row)
            })
            .fetch_all(self)
            .await?;

        d2.insert(0, columns);

        Ok(Some(Dataframe::from_vec(d2, "h")))
    }

    async fn insert(&self, table_name: &str, dataframe: Dataframe) -> TdDbResult<()> {
        todo!()
    }
}

pub struct Loader {
    driver: Sql,
    conn: String,
    pool: Option<Box<dyn Engine<Dataframe, DataframeColumn>>>,
}

const LOADER_MSG_NO_POOL: &'static str = "Loader pool not set";

// TODO: transaction functionality
impl Loader {
    /// create a loader from `ConnInfo`
    pub fn new(conn_info: ConnInfo) -> Self {
        Loader {
            driver: conn_info.driver.clone(),
            conn: conn_info.to_string(),
            pool: None,
        }
    }

    /// create a loader from `&str`
    pub fn from_str(conn_str: &str) -> Self {
        let mut s = conn_str.split(":");
        let driver = match s.next() {
            Some(v) => v.into(),
            None => Sql::Sqlite,
        };
        Loader {
            driver,
            conn: conn_str.to_string(),
            pool: None,
        }
    }

    /// manual establish connection pool
    pub async fn connect(&mut self) -> TdDbResult<()> {
        match self.driver {
            Sql::Mysql => match MySqlPool::connect(&self.conn).await {
                Ok(op) => {
                    self.pool = Some(Box::new(op));
                    Ok(())
                }
                Err(e) => Err(e.into()),
            },
            Sql::Postgres => match PgPool::connect(&self.conn).await {
                Ok(op) => {
                    self.pool = Some(Box::new(op));
                    Ok(())
                }
                Err(e) => Err(e.into()),
            },
            Sql::Sqlite => match SqlitePool::connect(&self.conn).await {
                Ok(op) => {
                    self.pool = Some(Box::new(op));
                    Ok(())
                }
                Err(e) => Err(e.into()),
            },
        }
    }

    /// get a table's schema
    pub async fn get_table_schema(&self, table: &str) -> TdDbResult<Vec<DataframeColumn>> {
        match &self.pool {
            Some(p) => Ok(p.get_table_schema(table).await?),
            None => Err(TdDbError::Common(LOADER_MSG_NO_POOL)),
        }
    }

    /// fetch all data
    pub async fn fetch_all(&self, query: &str) -> TdDbResult<Option<Dataframe>> {
        match &self.pool {
            Some(p) => Ok(p.fetch_all(query).await?),
            None => Err(TdDbError::Common(LOADER_MSG_NO_POOL)),
        }
    }
}

#[cfg(test)]
mod test_loader {

    use super::*;

    const CONN1: &'static str = "mysql://root:secret@localhost:3306/dev";
    const CONN2: &'static str = "postgres://root:secret@localhost:5432/dev";
    const CONN3: &'static str = "sqlite:cache/dev.sqlite";

    #[test]
    fn test_new() {
        let loader1 = Loader::from_str(CONN1);
        println!("{:?}", loader1.conn);

        let conn_info = ConnInfo::new(Sql::Mysql, "root", "secret", "localhost", 3306, "dev");
        let loader2 = Loader::new(conn_info);
        println!("{:?}", loader2.conn);

        assert_eq!(loader1.conn, loader2.conn);
    }

    #[tokio::test]
    async fn test_connection_mysql() {
        let mut loader = Loader::from_str(CONN1);
        loader.connect().await.unwrap();

        let df = loader
            .fetch_all("select * from 'dev' limit 1")
            .await
            .unwrap();

        println!("{:#?}", df);
    }

    #[tokio::test]
    async fn test_connection_pg() {
        let mut loader = Loader::from_str(CONN2);
        loader.connect().await.unwrap();

        let df = loader
            .fetch_all("select * from 'dev' limit 1")
            .await
            .unwrap();

        println!("{:#?}", df);
    }

    #[tokio::test]
    async fn test_connection_sqlite() {
        let mut loader = Loader::from_str(CONN3);
        loader.connect().await.unwrap();

        let df = loader
            .fetch_all("select * from 'dev' limit 1")
            .await
            .unwrap();

        println!("{:#?}", df);
    }

    #[tokio::test]
    async fn test_get_table_schema_mysql() {
        let mut loader = Loader::from_str(CONN3);
        loader.connect().await.unwrap();

        let scm = loader.get_table_schema("dev").await.unwrap();

        println!("{:#?}", scm);
    }

    #[tokio::test]
    async fn test_get_table_schema_pg() {
        let mut loader = Loader::from_str(CONN3);
        loader.connect().await.unwrap();

        let scm = loader.get_table_schema("dev").await.unwrap();

        println!("{:#?}", scm);
    }

    #[tokio::test]
    async fn test_get_table_schema_sqlite() {
        let mut loader = Loader::from_str(CONN3);
        loader.connect().await.unwrap();

        let scm = loader.get_table_schema("dev").await.unwrap();

        println!("{:#?}", scm);
    }
}
