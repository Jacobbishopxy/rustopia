// use chrono::{NaiveDateTime, NaiveTime};
// use sea_query::*;

// use super::df::{Dataframe, DataframeData};

#[derive(Debug)]
pub enum Sql {
    Postgres,
    MySql,
    Sqlite,
}

#[derive(Debug)]
pub enum ColType {
    Bool,
    Float,
    Double,
    Date,
    Time,
    DateTime,
    VarChar,
    Text,
}

#[derive(Debug)]
pub struct Col {
    pub name: String,
    pub col_type: ColType,
}

// impl Sql {
//     pub fn create_table(&self, table_name: &str, columns: Vec<Col>) -> String {
//         let mut statement = Table::create();
//         statement.table(Alias::new(table_name));

//         columns.iter().for_each(|c| {
//             statement.col(gen_col(c));
//         });

//         match &self {
//             Sql::Postgres => statement.to_string(PostgresQueryBuilder),
//             Sql::MySql => statement.to_string(MysqlQueryBuilder),
//         }
//     }

//     pub fn insert(&self, table_name: &str, df: Dataframe) -> String {
//         let mut statement = Query::insert();
//         statement.into_table(Alias::new(table_name));
//         statement.columns(df.column.iter().map(|c| Alias::new(c)));

//         df.data.into_iter().for_each(|c| {
//             let values: Vec<Value> = c.into_iter().map(|d| d.into()).collect();

//             statement.values_panic(values);
//         });

//         match &self {
//             Sql::Postgres => statement.to_string(PostgresQueryBuilder),
//             Sql::MySql => statement.to_string(MysqlQueryBuilder),
//         }
//     }

//     pub fn update(&self, _table_name: &str, _df: Dataframe) -> String {
//         unimplemented!()
//     }
// }

// fn gen_col_type(c: ColumnDef, col_type: &ColType) -> ColumnDef {
//     match col_type {
//         ColType::Bool => c.boolean(),
//         ColType::Float => c.float(),
//         ColType::Double => c.double(),
//         ColType::Date => c.date(),
//         ColType::Time => c.time(),
//         ColType::DateTime => c.date_time(),
//         ColType::VarChar => c.string(),
//         ColType::Text => c.text(),
//     }
// }

// fn gen_col(col: &Col) -> ColumnDef {
//     let c = ColumnDef::new(Alias::new(&col.name));
//     gen_col_type(c, &col.col_type)
// }

// impl Into<Value> for DataframeData {
//     fn into(self) -> Value {
//         match self {
//             DataframeData::Id(v) => Value::BigUnsigned(v),
//             DataframeData::Bool(v) => Value::Bool(v),
//             DataframeData::Short(v) => Value::Int(v),
//             DataframeData::Long(v) => Value::BigInt(v),
//             DataframeData::Date(v) => Value::DateTime(Box::new(NaiveDateTime::new(
//                 v,
//                 NaiveTime::from_hms(0, 0, 0),
//             ))),
//             DataframeData::Time(v) => Value::String(Box::new(v.to_string())),
//             DataframeData::DateTime(v) => Value::DateTime(Box::new(v)),
//             DataframeData::Float(v) => Value::Float(v),
//             DataframeData::Double(v) => Value::Double(v),
//             DataframeData::String(v) => Value::String(Box::new(v)),
//             DataframeData::Error => Value::Null,
//             DataframeData::None => Value::Null,
//         }
//     }
// }

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