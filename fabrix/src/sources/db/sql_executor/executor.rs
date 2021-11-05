//! Database executor

use async_trait::async_trait;
use sqlx::{MySqlPool, PgPool, SqlitePool};

use super::{ConnInfo, FabrixDatabaseLoader, LoaderPool};
use crate::{
    adt, DataFrame, DdlQuery, DmlMutation, DmlQuery, FabrixError, FabrixResult, Series, SqlBuilder,
    Value,
};

/// An engin is an interface to describe sql executor's business logic
#[async_trait]
pub trait Engine {
    /// connect to the database
    async fn connect(&mut self) -> FabrixResult<()>;

    /// disconnect from the database
    async fn disconnect(&mut self) -> FabrixResult<()>;

    /// get primary key from a table
    async fn get_primary_key(&self, table_name: &str) -> FabrixResult<String>;

    /// insert data into a table
    async fn insert(&self, table_name: &str, data: DataFrame) -> FabrixResult<u64>;

    /// update data in a table
    async fn update(&self, table_name: &str, data: DataFrame) -> FabrixResult<u64>;

    /// save data into a table
    /// saving strategy:
    /// 1. Replace: no matter the table is exist, create a new table
    /// 1. Append: if the table is exist, append data to the table, otherwise failed
    /// 1. Upsert: update and insert
    /// 1. Fail: if the table is exist, do nothing, otherwise create a new table
    async fn save(
        &self,
        table_name: &str,
        data: DataFrame,
        strategy: &adt::SaveStrategy,
    ) -> FabrixResult<usize>;

    /// delete data from an existing table. TODO: multiple delete methods
    async fn delete(&self, table_name: &str, data: Series) -> FabrixResult<u64>;

    /// get data from db. If the table has primary key, DataFrame's index will be the primary key
    async fn select(&self, select: &adt::Select) -> FabrixResult<DataFrame>;
}

/// Executor is the core struct of db mod.
/// It plays a role of CRUD and provides data manipulation functionality.
pub struct Executor {
    driver: SqlBuilder,
    conn_str: String,
    pool: Option<Box<dyn FabrixDatabaseLoader>>,
}

impl Executor {
    /// constructor
    pub fn new(conn_info: ConnInfo) -> Self {
        Executor {
            driver: conn_info.driver.clone(),
            conn_str: conn_info.to_string(),
            pool: None,
        }
    }

    /// constructor, from str
    pub fn from_str(conn_str: &str) -> Self {
        let mut s = conn_str.split(":");
        let driver = match s.next() {
            Some(v) => v.into(),
            None => SqlBuilder::Sqlite,
        };
        Executor {
            driver,
            conn_str: conn_str.to_string(),
            pool: None,
        }
    }
}

macro_rules! conn_e_err {
    ($pool:expr) => {
        if $pool.is_some() {
            return Err($crate::FabrixError::new_common_error(
                "connection has already been established",
            ));
        }
    };
}

macro_rules! conn_n_err {
    ($pool:expr) => {
        if $pool.is_none() {
            return Err($crate::FabrixError::new_common_error(
                "connection has not been established yet",
            ));
        }
    };
}

#[async_trait]
impl Engine for Executor {
    async fn connect(&mut self) -> FabrixResult<()> {
        conn_e_err!(self.pool);
        match self.driver {
            SqlBuilder::Mysql => MySqlPool::connect(&self.conn_str).await.map(|pool| {
                self.pool = Some(Box::new(LoaderPool::from(pool)));
            })?,
            SqlBuilder::Postgres => PgPool::connect(&self.conn_str).await.map(|pool| {
                self.pool = Some(Box::new(LoaderPool::from(pool)));
            })?,
            SqlBuilder::Sqlite => SqlitePool::connect(&self.conn_str).await.map(|pool| {
                self.pool = Some(Box::new(LoaderPool::from(pool)));
            })?,
        }
        Ok(())
    }

    async fn disconnect(&mut self) -> FabrixResult<()> {
        conn_n_err!(self.pool);
        self.pool.as_ref().unwrap().disconnect().await;
        Ok(())
    }

    async fn get_primary_key(&self, table_name: &str) -> FabrixResult<String> {
        conn_n_err!(self.pool);
        let que = self.driver.get_primary_key(table_name);
        let res = self.pool.as_ref().unwrap().fetch_optional(&que).await?;

        if let Some(v) = res {
            if let Some(k) = v.first() {
                return Ok(try_value_into_string(k)?);
            }
        }

        Err(FabrixError::new_common_error("primary key not found"))
    }

    async fn insert(&self, table_name: &str, data: DataFrame) -> FabrixResult<u64> {
        conn_n_err!(self.pool);
        let que = self.driver.insert(table_name, data)?;
        let res = self.pool.as_ref().unwrap().execute(&que).await?;

        Ok(res.rows_affected)
    }

    async fn update(&self, table_name: &str, data: DataFrame) -> FabrixResult<u64> {
        conn_n_err!(self.pool);
        let index_field = data.index_field();
        let index_option = adt::IndexOption::try_from(&index_field)?;
        let que = self.driver.update(table_name, data, &index_option)?;

        let res = self
            .pool
            .as_ref()
            .unwrap()
            .execute_many(&que)
            .await?
            .rows_affected;

        Ok(res)
    }

    async fn save(
        &self,
        table_name: &str,
        data: DataFrame,
        strategy: &adt::SaveStrategy,
    ) -> FabrixResult<usize> {
        conn_n_err!(self.pool);

        match strategy {
            adt::SaveStrategy::FailIfExists => todo!(),
            adt::SaveStrategy::Replace => todo!(),
            adt::SaveStrategy::Append => todo!(),
            adt::SaveStrategy::Upsert => todo!(),
        }
    }

    async fn delete(&self, table_name: &str, data: Series) -> FabrixResult<u64> {
        conn_n_err!(self.pool);
        let que = self.driver.delete(table_name, data)?;
        let res = self.pool.as_ref().unwrap().execute(&que).await?;

        Ok(res.rows_affected)
    }

    async fn select(&self, select: &adt::Select) -> FabrixResult<DataFrame> {
        conn_n_err!(self.pool);

        // Generally, primary key always exists, and in this case, use it as index.
        // Otherwise, use default index.
        let mut df = match self.get_primary_key(&select.table).await {
            Ok(pk) => {
                let mut new_select = select.clone();
                add_primary_key_to_select(&pk, &mut new_select);
                let que = self.driver.select(&new_select);
                let res = self.pool.as_ref().unwrap().fetch_all_with_key(&que).await?;
                DataFrame::from_rows(res)?
            }
            Err(_) => {
                let que = self.driver.select(select);
                let res = self.pool.as_ref().unwrap().fetch_all(&que).await?;
                DataFrame::from_row_values(res)?
            }
        };
        df.set_column_names(&select.columns_name(true))?;

        Ok(df)
    }
}

fn add_primary_key_to_select(primary_key: &String, select: &mut adt::Select) {
    select
        .columns
        .insert(0, adt::ColumnAlias::Simple(primary_key.to_owned()));
}

fn try_value_into_string(value: &Value) -> FabrixResult<String> {
    match value {
        Value::String(v) => Ok(v.to_owned()),
        _ => Err(FabrixError::new_common_error("value is not a string")),
    }
}

#[cfg(test)]
mod test_executor {

    use super::*;

    const CONN1: &'static str = "mysql://root:secret@localhost:3306/dev";
    // const CONN2: &'static str = "postgres://root:secret@localhost:5432/dev";
    // const CONN3: &'static str = "sqlite:cache/dev.sqlite";

    #[tokio::test]
    async fn test_connection() {
        let mut exc = Executor::from_str(CONN1);

        exc.connect().await.expect("connection is ok");
    }

    #[tokio::test]
    async fn test_get_primary_key() {
        let mut exc = Executor::from_str(CONN1);

        exc.connect().await.expect("connection is ok");

        println!("{:?}", exc.get_primary_key("dev").await);
    }

    #[tokio::test]
    async fn test_select() {
        let mut exc = Executor::from_str(CONN1);

        exc.connect().await.expect("connection is ok");

        let select = adt::Select {
            table: "products".to_owned(),
            columns: vec![
                adt::ColumnAlias::Simple("url".to_owned()),
                adt::ColumnAlias::Simple("name".to_owned()),
                adt::ColumnAlias::Simple("description".to_owned()),
                adt::ColumnAlias::Simple("price".to_owned()),
                adt::ColumnAlias::Simple("visible".to_owned()),
                adt::ColumnAlias::Alias(adt::NameAlias {
                    from: "product_id".to_owned(),
                    to: "ID".to_owned(),
                }),
            ],
            filter: None,
            order: None,
            limit: None,
            offset: None,
        };

        let df = exc.select(&select).await.unwrap();

        println!("{:?}", df);
    }
}
