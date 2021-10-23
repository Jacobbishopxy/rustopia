//! Sql

use std::fmt::Display;

use polars::prelude::{DataType, Field};
use sea_query::{
    Alias, ColumnDef, Expr, MysqlQueryBuilder, PostgresQueryBuilder, Query, SqliteQueryBuilder,
    Table, Value as SValue,
};

use super::try_from_value_to_svalue;
use crate::{DataFrame, FabrixError, FabrixResult, Series};

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

    pub fn try_from_series(series: &'a Series) -> FabrixResult<Self> {
        let dtype = series.dtype();
        let index_type = match dtype {
            DataType::UInt8 => Ok(IndexType::Int),
            DataType::UInt16 => Ok(IndexType::Int),
            DataType::UInt32 => Ok(IndexType::Int),
            DataType::UInt64 => Ok(IndexType::BigInt),
            DataType::Int8 => Ok(IndexType::Int),
            DataType::Int16 => Ok(IndexType::Int),
            DataType::Int32 => Ok(IndexType::Int),
            DataType::Int64 => Ok(IndexType::BigInt),
            DataType::Utf8 => Ok(IndexType::Uuid), // TODO: saving uuid as String?
            _ => Err(FabrixError::new_common_error(format!(
                "{:?} cannot convert to index type",
                dtype
            ))),
        }?;

        Ok(IndexOption {
            name: series.name(),
            index_type,
        })
    }
}

pub enum SaveStrategy {
    Replace,
    Append,
    Upsert,
    Fail,
}

#[derive(Debug, Clone)]
pub enum SqlBuilder {
    Mysql,
    Postgres,
    Sqlite,
}

impl Display for SqlBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mysql => write!(f, "mysql"),
            Self::Postgres => write!(f, "postgres"),
            Self::Sqlite => write!(f, "sqlite"),
        }
    }
}

impl From<&str> for SqlBuilder {
    fn from(v: &str) -> Self {
        match &v.to_lowercase()[..] {
            "mysql" | "m" => SqlBuilder::Mysql,
            "postgres" | "p" => SqlBuilder::Postgres,
            _ => SqlBuilder::Sqlite,
        }
    }
}

/// table field
pub struct TableField {
    field: Field,
    nullable: bool,
}

impl TableField {
    pub fn new(field: Field, nullable: bool) -> Self {
        TableField { field, nullable }
    }

    pub fn field(&self) -> &Field {
        &self.field
    }

    pub fn name(&self) -> &String {
        &self.field.name()
    }

    pub fn data_type(&self) -> &DataType {
        &self.field.data_type()
    }

    pub fn nullable(&self) -> bool {
        self.nullable
    }
}

/// statement macro
macro_rules! statement {
    ($builder:expr, $statement:expr) => {{
        match $builder {
            SqlBuilder::Mysql => $statement.to_string(MysqlQueryBuilder),
            SqlBuilder::Postgres => $statement.to_string(PostgresQueryBuilder),
            SqlBuilder::Sqlite => $statement.to_string(SqliteQueryBuilder),
        }
    }};
    ($accumulator:expr; $builder:expr, $statement:expr) => {{
        match $builder {
            SqlBuilder::Postgres => {
                $accumulator.push($statement.to_string(PostgresQueryBuilder));
            }
            SqlBuilder::Mysql => {
                $accumulator.push($statement.to_string(MysqlQueryBuilder));
            }
            SqlBuilder::Sqlite => {
                $accumulator.push($statement.to_string(SqliteQueryBuilder));
            }
        }
    }};
}

impl SqlBuilder {
    /// check whether table exists
    pub fn check_table(&self, table_name: &str) -> String {
        let que: &str;
        match self {
            SqlBuilder::Postgres => {
                que = r#"
                SELECT EXISTS(
                    SELECT 1
                    FROM information_schema.tables
                    WHERE TABLE_NAME = '_table_name_'
                )::int"#;
            }
            SqlBuilder::Mysql => {
                que = r#"
                SELECT EXISTS(
                    SELECT 1
                    FROM information_schema.TABLES
                    WHERE TABLE_NAME = '_table_name_'
                )"#;
            }
            SqlBuilder::Sqlite => {
                que = r#"
                SELECT EXISTS(
                    SELECT 1
                    FROM sqlite_master
                    WHERE type='table'
                    AND name = '_table_name_'
                )"#;
            }
        }
        que.replace("_table_name_", table_name).to_owned()
    }

    /// check a table's schema
    pub fn check_table_schema(&self, table_name: &str) -> String {
        let que: &str;
        match self {
            SqlBuilder::Mysql => {
                que = r#"
                SELECT
                    column_name,
                    data_type,
                    CASE WHEN is_nullable = 'YES' THEN 1 else 0 END AS is_nullable
                FROM
                    information_schema.columns
                WHERE
                    table_name = '_table_name_'
                "#;
            }
            SqlBuilder::Postgres => {
                que = r#"
                SELECT
                    column_name,
                    udt_name,
                    CASE WHEN is_nullable = 'YES' THEN 1 else 0 END AS is_nullable
                FROM
                    information_schema.columns
                WHERE
                    table_name = '_table_name_'
                "#;
            }
            SqlBuilder::Sqlite => {
                que = r#"
                SELECT
                    name,
                    type,
                    CASE WHEN `notnull` = 0 THEN 1 else 0 END AS is_nullable
                FROM
                    PRAGMA_TABLE_INFO('_table_name_')
                "#;
            }
        }
        que.replace("_table_name_", table_name).to_owned()
    }

    /// given a list of ids, check existed ids (used for `upsert` method). Make sure index contains only not-null values
    pub fn select_exist_ids(&self, table_name: &str, index: &Series) -> FabrixResult<String> {
        let mut statement = Query::select();
        let (index_name, index_dtype) = (index.name(), index.dtype());
        let ids = index
            .into_iter()
            .map(|i| try_from_value_to_svalue(i, index_dtype, false))
            .collect::<FabrixResult<Vec<_>>>()?;

        statement
            .column(Alias::new(index_name))
            .from(Alias::new(table_name))
            .and_where(Expr::col(Alias::new(index_name)).is_in(ids));

        Ok(statement!(self, statement))
    }

    /// given a `Dataframe` columns, generate SQL create_table string
    pub fn create_table(
        &self,
        table_name: &str,
        columns: &Vec<TableField>,
        index_option: Option<&IndexOption>,
    ) -> String {
        let mut statement = Table::create();
        statement.table(Alias::new(table_name)).if_not_exists();

        if let Some(idx) = index_option {
            statement.col(&mut gen_primary_col(idx));
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
    pub fn insert(&self, table_name: &str, df: DataFrame) -> FabrixResult<String> {
        let mut statement = Query::insert();
        statement.into_table(Alias::new(table_name));
        statement.columns(vec![Alias::new(df.index.name())]);
        statement.columns(df.fields().iter().map(|c| Alias::new(c.name())));

        let column_info = df.column_info();
        for c in df.into_iter() {
            let record = c
                .data
                .into_iter()
                .zip(column_info.clone())
                .map(|(v, inf)| try_from_value_to_svalue(v, inf.0.data_type(), inf.1))
                .collect::<FabrixResult<Vec<_>>>()?;

            // make sure columns length equals records length
            statement.values(record)?;
        }

        Ok(statement!(self, statement))
    }

    /// given a `Dataframe`, in terms of indices update to an existing table
    pub fn update(
        &self,
        table_name: &str,
        df: DataFrame,
        index_option: &IndexOption,
    ) -> FabrixResult<Vec<String>> {
        let column_info = df.column_info();
        let indices = df.index().clone();
        let indices_type = indices.dtype().clone();
        let mut res = vec![];

        for (row, idx) in df.into_iter().zip(indices.into_iter()) {
            let mut statement = Query::update();
            statement.table(Alias::new(table_name));

            let updates = row
                .data
                .clone()
                .into_iter()
                .zip(column_info.clone())
                .map(|(v, inf)| try_from_value_to_svalue(v, inf.0.data_type(), inf.1))
                .collect::<FabrixResult<Vec<_>>>()?;
            let updates = column_info
                .clone()
                .into_iter()
                .zip(updates.into_iter())
                .map(|(inf, v)| (Alias::new(inf.0.name()), v))
                .collect::<Vec<(_, _)>>();

            statement.values(updates).and_where(
                Expr::col(Alias::new(index_option.name)).eq(try_from_value_to_svalue(
                    idx,
                    &indices_type,
                    false,
                )?),
            );

            statement!(res; self, statement)
        }

        Ok(res)
    }

    /// given a `Dataframe`, saves it with `SaveOption` strategy (transaction capability is required on executor)
    pub fn save(
        &self,
        table_name: &str,
        df: DataFrame,
        save_strategy: &SaveStrategy,
    ) -> FabrixResult<Vec<String>> {
        let mut res = Vec::new();
        match save_strategy {
            SaveStrategy::Replace => {
                // delete table if exists
                res.push(self.delete_table(table_name));
                // create a new table
                let index_option = IndexOption::try_from_series(df.index())?;
                res.push(self.create_table(
                    table_name,
                    &conv_fields(df.fields()),
                    Some(&index_option),
                ));
                // insert data to this new table
                res.push(self.insert(table_name, df)?)
            }
            SaveStrategy::Append => {
                // append, ignore index
                res.push(self.insert(table_name, df)?);
            }
            SaveStrategy::Upsert => {
                // check table existence and return an integer value, 0: false, 1: true.
                res.push(self.check_table(table_name));
                // check IDs
                res.push(self.select_exist_ids(table_name, df.index())?);
            }
            SaveStrategy::Fail => {
                // check table existence and return an integer value, 0: false, 1: true.
                res.push(self.check_table(table_name));
                // if table does not exist (the result of the previous sql execution is 0), then create a new one
                let index_option = IndexOption::try_from_series(df.index())?;
                res.push(self.create_table(
                    table_name,
                    &conv_fields(df.fields()),
                    Some(&index_option),
                ));
                // insert data to this new table
                res.push(self.insert(table_name, df)?);
            }
        }

        Ok(res)
    }
}

/// generate a primary column
fn gen_primary_col(index_option: &IndexOption) -> ColumnDef {
    let mut cd = ColumnDef::new(Alias::new(index_option.name));

    match index_option.index_type {
        IndexType::Int => cd.integer(),
        IndexType::BigInt => cd.big_integer(),
        IndexType::Uuid => cd.uuid(),
    };

    cd.not_null().auto_increment().primary_key();

    cd
}

/// generate column by `DataframeColumn`
fn gen_col(field: &TableField) -> ColumnDef {
    let mut c = ColumnDef::new(Alias::new(field.name()));
    match field.data_type() {
        DataType::Boolean => c.boolean(),
        DataType::UInt8 => c.integer(),
        DataType::UInt16 => c.integer(),
        DataType::UInt32 => c.integer(),
        DataType::UInt64 => c.big_integer(),
        DataType::Int8 => c.integer(),
        DataType::Int16 => c.integer(),
        DataType::Int32 => c.integer(),
        DataType::Int64 => c.big_integer(),
        DataType::Float32 => c.double(),
        DataType::Float64 => c.float(),
        DataType::Utf8 => c.string(),
        DataType::Date32 => c.date_time(),
        DataType::Date64 => c.date_time(),
        DataType::Time64(_) => c.time(),
        DataType::List(_) => unimplemented!(),
        DataType::Duration(_) => unimplemented!(),
        DataType::Null => unimplemented!(),
        DataType::Categorical => unimplemented!(),
    };

    if !field.nullable {
        c.not_null();
    }

    c
}

/// dataframe fields conversion. Temporary solution
fn conv_fields(fields: Vec<Field>) -> Vec<TableField> {
    fields
        .into_iter()
        .map(|f| TableField::new(f, true))
        .collect()
}

#[cfg(test)]
mod test_sql {

    use super::*;
    use crate::series;

    #[test]
    fn test_select_exist_ids() {
        let ids = series!("index" => [1, 2, 3, 4, 5]);
        let sql = SqlBuilder::Mysql.select_exist_ids("dev", &ids);

        println!("{:?}", sql);
    }
}
