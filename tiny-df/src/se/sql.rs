use chrono::{NaiveDateTime, NaiveTime};
use sea_query::*;

use crate::{DataType, Dataframe, DataframeColumn, DataframeData};

pub enum SaveOption {
    Replace,
    Append,
    Upsert,
    Fail,
}

#[derive(Debug)]
pub enum Sql {
    Postgres,
    MySql,
    Sqlite,
}

impl Sql {
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

    pub fn create_table(&self, table_name: &str, columns: &Vec<DataframeColumn>) -> String {
        let mut statement = Table::create();
        statement.table(Alias::new(table_name));

        columns.iter().for_each(|c| {
            statement.col(&mut gen_col(c));
        });

        match self {
            Sql::Postgres => statement.to_string(PostgresQueryBuilder),
            Sql::MySql => statement.to_string(MysqlQueryBuilder),
            Sql::Sqlite => statement.to_string(SqliteQueryBuilder),
        }
    }

    pub fn delete_table(&self, table_name: &str) -> String {
        let mut statement = Table::drop();
        statement.table(Alias::new(table_name));

        match self {
            Sql::Postgres => statement.to_string(PostgresQueryBuilder),
            Sql::MySql => statement.to_string(MysqlQueryBuilder),
            Sql::Sqlite => statement.to_string(SqliteQueryBuilder),
        }
    }

    pub fn insert(&self, table_name: &str, df: Dataframe) -> String {
        let mut statement = Query::insert();
        statement.into_table(Alias::new(table_name));
        statement.columns(df.columns().iter().map(|c| Alias::new(c.name.as_str())));

        df.data().into_iter().for_each(|c| {
            let values: Vec<Value> = c.into_iter().map(|d| d.into()).collect();

            statement.values_panic(values);
        });

        match self {
            Sql::Postgres => statement.to_string(PostgresQueryBuilder),
            Sql::MySql => statement.to_string(MysqlQueryBuilder),
            Sql::Sqlite => statement.to_string(SqliteQueryBuilder),
        }
    }

    pub fn save(&self, table_name: &str, df: Dataframe, option: SaveOption) -> Vec<String> {
        let mut res = Vec::new();
        match option {
            SaveOption::Replace => {
                res.push(self.delete_table(table_name));
                res.push(self.create_table(table_name, df.columns()));
                res.push(self.insert(table_name, df))
            }
            SaveOption::Append => {
                res.push(self.insert(table_name, df));
            }
            SaveOption::Upsert => {
                res.push(self.check_table(table_name));
                res.push(self.upsert(table_name, df));
            }
            SaveOption::Fail => {
                res.push(self.check_table(table_name));
                res.push(self.insert(table_name, df));
            }
        }

        res
    }

    // TODO: dataframe ID column (auto gen primary key) specific, (create & insert by using ID)
    pub fn update(&self, _table_name: &str, _df: Dataframe) -> String {
        unimplemented!()
    }

    pub fn upsert(&self, _table_name: &str, _df: Dataframe) -> String {
        unimplemented!()
    }
}

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
    let data = df![["name", "progress",], ["Jacob", 100f64,], ["Sam", 80f64,],];
    let df = Dataframe::new(data, "h");

    let sql = Sql::Postgres;
    let query = sql.insert(&table_name, df);

    println!("{:?}", query);
}

#[test]
fn test_save() {
    use crate::df;

    let table_name = "dev".to_string();
    let data = df![["name", "progress",], ["Jacob", 100f64,], ["Sam", 80f64,],];
    let df = Dataframe::new(data, "h");

    let sql = Sql::MySql;
    let query = sql.save(&table_name, df, SaveOption::Replace);

    println!("{:?}", query);
}
