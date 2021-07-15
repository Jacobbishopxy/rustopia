use sqlx::{mysql::MySqlPoolOptions, postgres::PgPoolOptions, Database, MySql, Pool, Postgres};
use sqlx::{Connection, MySqlConnection, PgConnection};

use crate::util::DataEnum;
use crate::util::DbQueryResult;
use crate::DaoError;
use crate::QueryResult;

pub type DaoPG = Dao<Postgres>;
pub type DaoMY = Dao<MySql>;

pub struct Dao<T: Database> {
    pub info: String,
    pub pool: Pool<T>,
}

impl<T: Database> Clone for Dao<T> {
    fn clone(&self) -> Self {
        Dao {
            info: self.info.clone(),
            pool: self.pool.clone(),
        }
    }
}

impl Dao<Postgres> {
    pub async fn new(uri: &str, max_connections: u32) -> Result<Self, DaoError> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(uri)
            .await?;

        Ok(Dao {
            info: uri.to_owned(),
            pool,
        })
    }

    pub async fn connectable(uri: &str) -> bool {
        match PgConnection::connect(uri).await {
            Ok(_) => true,
            Err(_) => false,
        }
    }
}

impl Dao<MySql> {
    pub async fn new(uri: &str, max_connections: u32) -> Result<Self, DaoError> {
        let pool = MySqlPoolOptions::new()
            .max_connections(max_connections)
            .connect(uri)
            .await?;

        Ok(Dao {
            info: uri.to_owned(),
            pool,
        })
    }

    pub async fn connectable(uri: &str) -> bool {
        match MySqlConnection::connect(uri).await {
            Ok(_) => true,
            Err(_) => false,
        }
    }
}

#[derive(Clone)]
pub enum DaoOptions {
    PG(DaoPG),
    MY(DaoMY),
}

impl DaoOptions {
    pub fn info(&self) -> String {
        match self {
            DaoOptions::PG(p) => p.info.to_owned(),
            DaoOptions::MY(p) => p.info.to_owned(),
        }
    }

    pub async fn disconnect(&self) {
        match self {
            DaoOptions::MY(p) => p.pool.close().await,
            DaoOptions::PG(p) => p.pool.close().await,
        }
    }

    pub async fn exec(&self, query: &str) -> Result<Box<dyn QueryResult>, DaoError> {
        match self {
            DaoOptions::PG(p) => match sqlx::query(query).execute(&p.pool).await {
                Ok(r) => Ok(Box::new(DbQueryResult {
                    rows_affected: r.rows_affected(),
                    last_insert_id: None,
                })),
                Err(e) => Err(DaoError::from(e)),
            },
            DaoOptions::MY(p) => match sqlx::query(query).execute(&p.pool).await {
                Ok(r) => Ok(Box::new(DbQueryResult {
                    rows_affected: r.rows_affected(),
                    last_insert_id: Some(r.last_insert_id()),
                })),
                Err(e) => Err(DaoError::from(e)),
            },
        }
    }

    pub async fn seq_exec(
        &self,
        seq_query: &Vec<String>,
    ) -> Result<Box<dyn QueryResult>, DaoError> {
        match self {
            DaoOptions::PG(p) => {
                let mut tx = match p.pool.begin().await {
                    Ok(t) => t,
                    Err(e) => return Err(DaoError::from(e)),
                };

                for query in seq_query {
                    if let Err(e) = sqlx::query(query).execute(&mut tx).await {
                        return Err(DaoError::from(e));
                    }
                }

                match tx.commit().await {
                    Ok(_) => Ok(Box::new(DataEnum::Bool(true))),
                    Err(e) => Err(DaoError::from(e)),
                }
            }
            DaoOptions::MY(p) => {
                let mut tx = match p.pool.begin().await {
                    Ok(t) => t,
                    Err(e) => return Err(DaoError::from(e)),
                };

                for query in seq_query {
                    if let Err(e) = sqlx::query(query).execute(&mut tx).await {
                        return Err(DaoError::from(e));
                    }
                }

                match tx.commit().await {
                    Ok(_) => Ok(Box::new(DataEnum::Bool(true))),
                    Err(e) => Err(DaoError::from(e)),
                }
            }
        }
    }
}
