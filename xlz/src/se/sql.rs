use sea_query::*;

use tiny_df::{DataType, Dataframe, DataframeColDef, DataframeData};

#[derive(Debug)]
pub enum Sql {
    Postgres,
    MySql,
    // Sqlite,
}

impl Sql {
    pub fn create_table(&self, table_name: &str, columns: Vec<DataframeColDef>) -> String {
        let mut statement = Table::create();
        statement.table(Alias::new(table_name));

        columns.iter().for_each(|c| {
            statement.col(&mut gen_col(c));
        });

        match &self {
            Sql::Postgres => statement.to_string(PostgresQueryBuilder),
            Sql::MySql => statement.to_string(MysqlQueryBuilder),
        }
    }

    pub fn insert(&self, table_name: &str, df: Dataframe) -> String {
        let mut statement = Query::insert();
        statement.into_table(Alias::new(table_name));
        statement.columns(df.columns().iter().map(|c| Alias::new(c.name.as_str())));

        df.data.into_iter().for_each(|c| {
            let values: Vec<Value> = c.into_iter().map(|d| DD(d).into()).collect();

            statement.values_panic(values);
        });

        match &self {
            Sql::Postgres => statement.to_string(PostgresQueryBuilder),
            Sql::MySql => statement.to_string(MysqlQueryBuilder),
        }
    }

    pub fn update(&self, _table_name: &str, _df: Dataframe) -> String {
        unimplemented!()
    }
}

fn gen_col_type(c: &mut ColumnDef, col_type: &DataType) {
    match col_type {
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
}

fn gen_col(col: &DataframeColDef) -> ColumnDef {
    let mut c = ColumnDef::new(Alias::new(&col.name));
    gen_col_type(&mut c, &col.col_type);

    c
}

pub struct DD(pub DataframeData);

impl Into<Value> for DD {
    fn into(self) -> Value {
        match self.0 {
            DataframeData::Id(v) => Value::BigInt(v as i64),
            DataframeData::Bool(v) => Value::Bool(v),
            DataframeData::Short(v) => Value::Int(v),
            DataframeData::Long(v) => Value::BigInt(v),
            DataframeData::Float(v) => Value::Float(v),
            DataframeData::Double(v) => Value::Double(v),
            DataframeData::String(v) => Value::String(Box::new(v)),
            DataframeData::Date(v) => Value::String(Box::new(v.to_string())),
            DataframeData::Time(v) => Value::String(Box::new(v.to_string())),
            DataframeData::DateTime(v) => Value::DateTime(Box::new(v)),
            DataframeData::Error => Value::Null,
            DataframeData::None => Value::Null,
        }
    }
}

#[test]
fn test_insert() {
    // let table_name = "dev".to_string();
    // let data = vec![
    //     vec![
    //         DataframeData::String("name".to_owned()),
    //         DataframeData::String("progress".to_owned()),
    //     ],
    //     vec![
    //         DataframeData::String("Jacob".to_owned()),
    //         DataframeData::Double(100f64),
    //     ],
    //     vec![
    //         DataframeData::String("Sam".to_owned()),
    //         DataframeData::Double(80f64),
    //     ],
    // ];
    // let df = Dataframe::new(data);

    // let sql = Sql::Postgres;
    // let query = sql.insert(&table_name, df);

    // println!("{:?}", query);
}
