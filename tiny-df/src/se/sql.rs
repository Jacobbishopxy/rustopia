//! tiny-df sql builder
//!
//! turn dataframe to sql string

use std::fmt::Display;

use chrono::{NaiveDateTime, NaiveTime};
use sea_query::{
    Alias, ColumnDef, Expr, MysqlQueryBuilder, PostgresQueryBuilder, Query, SqliteQueryBuilder,
    Table, Value,
};

use crate::prelude::*;

pub enum IndexType {
    Int,
    BigInt,
    Uuid,
}

impl From<&str> for IndexType {
    fn from(v: &str) -> Self {
        match &v.to_lowercase()[..] {
            "uuid" | "u" => IndexType::Uuid,
            "bigint" | "b" => IndexType::BigInt,
            _ => IndexType::Int,
        }
    }
}

pub struct IndexOption<'a> {
    pub name: &'a str,
    pub index_type: IndexType,
}

impl<'a> IndexOption<'a> {
    pub fn new<T>(name: &'a str, index_type: T) -> Self
    where
        T: Into<IndexType>,
    {
        let index_type: IndexType = index_type.into();
        IndexOption { name, index_type }
    }
}

pub struct SaveOption<'a> {
    pub index: Option<IndexOption<'a>>,
    pub strategy: SaveStrategy,
}

impl<'a> SaveOption<'a> {
    pub fn new(index: Option<IndexOption<'a>>, strategy: SaveStrategy) -> Self {
        SaveOption { index, strategy }
    }
}

pub enum SaveStrategy {
    Replace,
    Append,
    Upsert,
    Fail,
}

#[derive(Debug, Clone)]
pub enum Sql {
    Mysql,
    Postgres,
    Sqlite,
}

impl Display for Sql {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mysql => write!(f, "mysql"),
            Self::Postgres => write!(f, "postgres"),
            Self::Sqlite => write!(f, "sqlite"),
        }
    }
}

impl From<&str> for Sql {
    fn from(v: &str) -> Self {
        match &v.to_lowercase()[..] {
            "mysql" | "m" => Sql::Mysql,
            "postgres" | "p" => Sql::Postgres,
            _ => Sql::Sqlite,
        }
    }
}

/// statement macro
macro_rules! statement {
    ($builder:expr, $statement:expr) => {{
        match $builder {
            Sql::Mysql => $statement.to_string(MysqlQueryBuilder),
            Sql::Postgres => $statement.to_string(PostgresQueryBuilder),
            Sql::Sqlite => $statement.to_string(SqliteQueryBuilder),
        }
    }};
    ($accumulator:expr; $builder:expr, $statement:expr) => {{
        match $builder {
            Sql::Postgres => {
                $accumulator.push($statement.to_string(PostgresQueryBuilder));
            }
            Sql::Mysql => {
                $accumulator.push($statement.to_string(MysqlQueryBuilder));
            }
            Sql::Sqlite => {
                $accumulator.push($statement.to_string(SqliteQueryBuilder));
            }
        }
    }};
}

impl Sql {
    /// check whether table exists
    pub fn check_table(&self, table_name: &str) -> String {
        let que: &str;
        match self {
            Sql::Postgres => {
                que = r#"
                SELECT EXISTS(
                    SELECT 1
                    FROM information_schema.tables
                    WHERE TABLE_NAME = 'table_name'
                )::int"#;
            }
            Sql::Mysql => {
                que = r#"
                SELECT EXISTS(
                    SELECT 1
                    FROM information_schema.TABLES
                    WHERE TABLE_NAME = 'table_name'
                )"#;
            }
            Sql::Sqlite => {
                que = r#"
                SELECT EXISTS(
                    SELECT 1
                    FROM sqlite_master
                    WHERE type='table'
                    AND name = 'table_name'
                )"#;
            }
        }
        que.replace("table_name", table_name).to_owned()
    }

    /// check a table's schema
    pub fn check_table_schema(&self, table_name: &str) -> String {
        let que: &str;
        match self {
            Sql::Mysql => {
                que = r#"
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = '_table_name_'
                "#;
            }
            Sql::Postgres => {
                que = r#"
                SELECT column_name, udt_name
                FROM information_schema.columns
                WHERE table_name = '_table_name_'
                "#;
            }
            Sql::Sqlite => {
                que = r#"
                SELECT name, type FROM PRAGMA_TABLE_INFO('_table_name_')
                "#;
            }
        }
        que.replace("_table_name_", table_name).to_owned()
    }

    /// given a list of ids, check existed ids (used for `upsert` method)
    pub fn select_exist_ids(&self, table_name: &str, ids: &Vec<Index>, index: &str) -> String {
        let mut statement = Query::select();

        statement
            .column(Alias::new(index))
            .from(Alias::new(table_name))
            .and_where(Expr::col(Alias::new(index)).is_in(ids));

        statement!(self, statement)
    }

    /// given a `Dataframe` columns, generate SQL create_table string
    pub fn create_table(
        &self,
        table_name: &str,
        columns: &Vec<DataframeColumn>,
        index_option: Option<&IndexOption>,
    ) -> String {
        let mut statement = Table::create();
        statement.table(Alias::new(table_name)).if_not_exists();

        if let Some(idx) = index_option {
            match idx.index_type {
                IndexType::Int => statement.col(&mut gen_primary_col(idx.name, false)),
                IndexType::BigInt => statement.col(&mut gen_primary_col(idx.name, true)),
                IndexType::Uuid => statement.col(&mut gen_primary_uuid_col(idx.name)),
            };
        }

        columns.iter().for_each(|c| {
            statement.col(&mut gen_col(c));
        });

        statement!(self, statement)
    }

    /// drop a table by its name
    pub fn delete_table(&self, table_name: &str) -> String {
        let mut statement = Table::drop();
        statement.table(Alias::new(table_name));

        statement!(self, statement)
    }

    /// given a `Dataframe`, insert it into an existing table
    pub fn insert(
        &self,
        table_name: &str,
        df: Dataframe,
        index_option: Option<&IndexOption>,
    ) -> String {
        let mut statement = Query::insert();
        statement.into_table(Alias::new(table_name));
        if let Some(idx) = index_option {
            statement.columns(vec![Alias::new(idx.name)]);
        }
        statement.columns(df.columns().iter().map(|c| Alias::new(c.name.as_str())));

        df.into_iter().for_each(|c| {
            let record: Vec<Value> = c.into_iter().map(|d| d.into()).collect();

            // make sure columns length equals records length
            statement.values_panic(record);
        });

        statement!(self, statement)
    }

    /// given a `Dataframe`, in terms of indices update to an existing table
    pub fn update(
        &self,
        table_name: &str,
        df: Dataframe,
        index_option: &IndexOption,
    ) -> Vec<String> {
        // column alias list
        let cols: Vec<Alias> = df.columns().iter().map(|c| Alias::new(&c.name)).collect();
        let indices = df.indices().clone();
        // result
        let mut res = vec![];

        for (row, idx) in df.into_iter().zip(indices) {
            let mut statement = Query::update();
            statement.table(Alias::new(table_name));

            let updates: Vec<(Alias, Value)> = cols
                .clone()
                .into_iter()
                .zip(row.into_iter())
                .map(|(c, v)| (c, v.into()))
                .collect();

            statement
                .values(updates)
                .and_where(Expr::col(Alias::new(index_option.name)).eq(idx));

            statement!(res; self, statement)
        }

        res
    }

    /// given a `Dataframe`, saves it with `SaveOption` strategy (transaction capability is required on executor)
    pub fn save(&self, table_name: &str, df: Dataframe, save_option: &SaveOption) -> Vec<String> {
        let mut res = Vec::new();
        match save_option.strategy {
            SaveStrategy::Replace => {
                // delete table if exists
                res.push(self.delete_table(table_name));
                // create a new table
                res.push(self.create_table(table_name, df.columns(), save_option.index.as_ref()));
                // insert data to this new table
                res.push(self.insert(table_name, df, save_option.index.as_ref()))
            }
            SaveStrategy::Append => {
                // append, ignore index
                res.push(self.insert(table_name, df, None));
            }
            SaveStrategy::Upsert => {
                // check table existence and return an integer value, 0: false, 1: true.
                res.push(self.check_table(table_name));
                // check IDs
                let id_col_name = save_option.index.as_ref().unwrap().name; // TODO: fix unwrap
                let ids = df.col(id_col_name).unwrap();
                res.push(self.select_exist_ids(table_name, ids, id_col_name));
            }
            SaveStrategy::Fail => {
                // check table existence and return an integer value, 0: false, 1: true.
                res.push(self.check_table(table_name));
                // if table does not exist (the result of the previous sql execution is 0), then create a new one
                res.push(self.create_table(table_name, df.columns(), save_option.index.as_ref()));
                // insert data to this new table
                res.push(self.insert(table_name, df, save_option.index.as_ref()));
            }
        }

        res
    }
}

/// generate a primary column
fn gen_primary_col(name: &str, big_int: bool) -> ColumnDef {
    let mut cd = ColumnDef::new(Alias::new(name));
    if big_int {
        cd.big_integer();
    } else {
        cd.integer();
    }
    cd.not_null().auto_increment().primary_key();

    cd
}

/// generate a primary uuid column
fn gen_primary_uuid_col(name: &str) -> ColumnDef {
    let mut cd = ColumnDef::new(Alias::new(name));
    cd.uuid().not_null().auto_increment().primary_key();

    cd
}

/// generate column by `DataframeColumn`
fn gen_col(col: &DataframeColumn) -> ColumnDef {
    let mut c = ColumnDef::new(Alias::new(&col.name));
    match col.col_type {
        DataType::Id => c.big_integer(),
        DataType::Bool => c.boolean(),
        DataType::Short => c.integer(),
        DataType::Long => c.big_integer(),
        DataType::UShort => c.integer(),
        DataType::ULong => c.big_integer(),
        DataType::Float => c.float(),
        DataType::Double => c.double(),
        DataType::Decimal => c.decimal(),
        DataType::String => c.string(),
        DataType::Date => c.timestamp(),
        DataType::Time => c.time(),
        DataType::DateTime => c.date_time(),
        DataType::Error => c.char(), // no type
        DataType::None => c.char(),  // no type
    };

    c
}

impl Into<Value> for DataframeData {
    fn into(self) -> Value {
        match self {
            DataframeData::Id(v) => Value::BigInt(Some(v as i64)),
            DataframeData::Bool(v) => Value::Bool(Some(v)),
            DataframeData::Short(v) => Value::Int(Some(v)),
            DataframeData::Long(v) => Value::BigInt(Some(v)),
            DataframeData::UShort(v) => Value::Unsigned(Some(v)),
            DataframeData::ULong(v) => Value::BigUnsigned(Some(v)),
            DataframeData::Float(v) => Value::Float(Some(v)),
            DataframeData::Double(v) => Value::Double(Some(v)),
            DataframeData::Decimal(v) => Value::Decimal(Some(Box::new(v))),
            DataframeData::String(v) => Value::String(Some(Box::new(v))),
            DataframeData::Date(v) => {
                let dt = NaiveDateTime::new(v, NaiveTime::from_hms(0, 0, 0));
                Value::DateTime(Some(Box::new(dt)))
            }
            DataframeData::Time(v) => Value::String(Some(Box::new(v.to_string()))),
            DataframeData::DateTime(v) => Value::DateTime(Some(Box::new(v))),
            DataframeData::Error => Value::String(None),
            DataframeData::None => Value::String(None),
        }
    }
}

impl Into<Value> for &DataframeData {
    fn into(self) -> Value {
        self.clone().into()
    }
}

#[test]
fn test_insert() {
    use crate::df;

    let table_name = "dev".to_string();
    let df = df![
        "h";
        ["name", "val",],
        ["Jacob", 100f64,],
        ["Sam", 80f64,]
    ];

    let sql = Sql::Postgres;
    let idx = IndexOption::new("id", "u");
    let query = sql.insert(&table_name, df, Some(&idx));

    println!("{:?}", query);
}

#[test]
fn test_save() {
    use crate::df;

    let table_name = "dev".to_string();
    let df = df![
        "h";
        ["name", "val",],
        ["Jacob", 100f64,],
        ["Sam", 80f64,],
        ["Joe", DataframeData::None,],
    ];

    let sql = Sql::Mysql;
    let idx = IndexOption::new("id", "i");
    let query = sql.save(
        &table_name,
        df,
        &SaveOption::new(Some(idx), SaveStrategy::Replace),
    );

    println!("{:?}", query);
}

#[test]
fn test_gen_create_table_query_sqlite() {
    let sql = Table::create()
        .table(Alias::new("test_table"))
        .if_not_exists()
        .col(
            ColumnDef::new(Alias::new("Id"))
                .integer()
                .not_null()
                .auto_increment()
                .primary_key(),
        )
        .col(ColumnDef::new(Alias::new("Uuid")).uuid())
        .col(ColumnDef::new(Alias::new("FontSize")).integer())
        .col(ColumnDef::new(Alias::new("Character")).string())
        .col(ColumnDef::new(Alias::new("Meta")).json())
        .col(ColumnDef::new(Alias::new("Created")).date_time())
        .build(SqliteQueryBuilder);

    println!("{:?}", sql);
}
