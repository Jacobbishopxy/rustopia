use chrono::{NaiveDateTime, NaiveTime};
use sea_query::{
    Alias, ColumnDef, Expr, MysqlQueryBuilder, PostgresQueryBuilder, Query, SqliteQueryBuilder,
    Table, Value,
};

use crate::prelude::*;

pub struct SaveOption<'a> {
    pub index: Option<&'a str>,
    pub strategy: SaveStrategy,
}

impl<'a> SaveOption<'a> {
    pub fn new(index: Option<&'a str>, strategy: SaveStrategy) -> Self {
        SaveOption { index, strategy }
    }
}

pub enum SaveStrategy {
    Replace,
    Append,
    Fail,
}

#[derive(Debug)]
pub enum Sql {
    Postgres,
    MySql,
    Sqlite,
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
                    WHERE table_schema = 'public'
                    AND table_name = 'table_name'
                )"#;
            }
            Sql::MySql => {
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

    /// given a list of ids, check existed ids (used for `upsert` method)
    pub fn select_exist_ids(&self, table_name: &str, ids: Vec<Index>, index: &str) -> String {
        let mut statement = Query::select();

        statement
            .column(Alias::new(index))
            .from(Alias::new(table_name))
            .and_where(Expr::col(Alias::new(index)).is_in(ids));

        match self {
            Sql::Postgres => statement.to_string(PostgresQueryBuilder),
            Sql::MySql => statement.to_string(MysqlQueryBuilder),
            Sql::Sqlite => statement.to_string(SqliteQueryBuilder),
        }
    }

    /// given a `Dataframe` columns, generate SQL create_table string
    pub fn create_table(
        &self,
        table_name: &str,
        columns: &Vec<DataframeColumn>,
        index: Option<&str>,
    ) -> String {
        let mut statement = Table::create();
        statement.table(Alias::new(table_name));

        if let Some(idx) = index {
            statement.col(&mut gen_primary_col(idx));
        }

        columns.iter().for_each(|c| {
            statement.col(&mut gen_col(c));
        });

        match self {
            Sql::Postgres => statement.to_string(PostgresQueryBuilder),
            Sql::MySql => statement.to_string(MysqlQueryBuilder),
            Sql::Sqlite => statement.to_string(SqliteQueryBuilder),
        }
    }

    /// drop a table by its name
    pub fn delete_table(&self, table_name: &str) -> String {
        let mut statement = Table::drop();
        statement.table(Alias::new(table_name));

        match self {
            Sql::Postgres => statement.to_string(PostgresQueryBuilder),
            Sql::MySql => statement.to_string(MysqlQueryBuilder),
            Sql::Sqlite => statement.to_string(SqliteQueryBuilder),
        }
    }

    /// given a `Dataframe`, insert it into an existing table
    pub fn insert(&self, table_name: &str, df: Dataframe, index: Option<&str>) -> String {
        let mut statement = Query::insert();
        statement.into_table(Alias::new(table_name));
        if let Some(idx) = index {
            statement.columns(vec![Alias::new(idx)]);
        }
        statement.columns(df.columns().iter().map(|c| Alias::new(c.name.as_str())));

        df.into_iter().for_each(|c| {
            let record: Vec<Value> = c.into_iter().map(|d| d.into()).collect();

            // make sure columns length equals records length
            statement.values_panic(record);
        });

        match self {
            Sql::Postgres => statement.to_string(PostgresQueryBuilder),
            Sql::MySql => statement.to_string(MysqlQueryBuilder),
            Sql::Sqlite => statement.to_string(SqliteQueryBuilder),
        }
    }

    /// given a `Dataframe`, in terms of indices update to an existing table
    pub fn update(&self, table_name: &str, df: Dataframe, index: &str) -> Vec<String> {
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
                .and_where(Expr::col(Alias::new(index)).eq(idx));

            match self {
                Sql::Postgres => {
                    res.push(statement.to_string(PostgresQueryBuilder));
                }
                Sql::MySql => {
                    res.push(statement.to_string(MysqlQueryBuilder));
                }
                Sql::Sqlite => {
                    res.push(statement.to_string(SqliteQueryBuilder));
                }
            }
        }

        res
    }

    /// given a `Dataframe`, saves it with `SaveOption` strategy (transaction capability is required on executor)
    pub fn save(&self, table_name: &str, df: Dataframe, option: SaveOption) -> Vec<String> {
        let mut res = Vec::new();
        match option.strategy {
            SaveStrategy::Replace => {
                res.push(self.delete_table(table_name));
                res.push(self.create_table(table_name, df.columns(), option.index));
                res.push(self.insert(table_name, df, option.index))
            }
            SaveStrategy::Append => {
                // append, ignore index
                res.push(self.insert(table_name, df, None));
            }
            SaveStrategy::Fail => {
                res.push(self.check_table(table_name));
                res.push(self.insert(table_name, df, option.index));
            }
        }

        res
    }
}

/// generate a primary column
fn gen_primary_col(name: &str) -> ColumnDef {
    let mut cd = ColumnDef::new(Alias::new(name));
    cd.big_integer().not_null().auto_increment().primary_key();

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
        DataType::Float => c.float(),
        DataType::Double => c.double(),
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
            DataframeData::Id(v) => Value::BigInt(v as i64),
            DataframeData::Bool(v) => Value::Bool(v),
            DataframeData::Short(v) => Value::Int(v),
            DataframeData::Long(v) => Value::BigInt(v),
            DataframeData::Float(v) => Value::Float(v),
            DataframeData::Double(v) => Value::Double(v),
            DataframeData::String(v) => Value::String(Box::new(v)),
            DataframeData::Date(v) => Value::DateTime(Box::new(NaiveDateTime::new(
                v,
                NaiveTime::from_hms(0, 0, 0),
            ))),
            DataframeData::Time(v) => Value::String(Box::new(v.to_string())),
            DataframeData::DateTime(v) => Value::DateTime(Box::new(v)),
            DataframeData::Error => Value::Null,
            DataframeData::None => Value::Null,
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
    let df = df!["h"; ["name", "progress",], ["Jacob", 100f64,], ["Sam", 80f64,]];

    let sql = Sql::Postgres;
    let query = sql.insert(&table_name, df, Some("id"));

    println!("{:?}", query);
}

#[test]
fn test_save() {
    use crate::df;

    let table_name = "dev".to_string();
    let df = df!["h"; ["name", "progress",], ["Jacob", 100f64,], ["Sam", 80f64,],];

    let sql = Sql::MySql;
    let query = sql.save(
        &table_name,
        df,
        SaveOption::new(Some("id"), SaveStrategy::Replace),
    );

    println!("{:?}", query);
}
