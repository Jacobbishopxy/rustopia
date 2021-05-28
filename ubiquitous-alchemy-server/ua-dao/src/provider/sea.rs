use sea_query::{Alias, ColumnDef, Index, IndexOrder, PostgresQueryBuilder, Table};
use ua_model;

fn grant_column_type(c: ColumnDef, col_type: &ua_model::ColumnType) -> ColumnDef {
    match col_type {
        ua_model::ColumnType::Binary => c.binary(),
        ua_model::ColumnType::Bool => c.boolean(),
        ua_model::ColumnType::Int => c.integer(),
        ua_model::ColumnType::Float => c.float(),
        ua_model::ColumnType::Double => c.double(),
        ua_model::ColumnType::Date => c.date(),
        ua_model::ColumnType::Time => c.time(),
        ua_model::ColumnType::DateTime => c.date_time(),
        ua_model::ColumnType::Timestamp => c.timestamp(),
        ua_model::ColumnType::Char => c.char(),
        ua_model::ColumnType::VarChar => c.string(),
        ua_model::ColumnType::Text => c.text(),
        ua_model::ColumnType::Json => c.json(),
    }
}

fn create_column(col: &ua_model::Column) -> ColumnDef {
    let c = ColumnDef::new(Alias::new(&col.name));
    let c = grant_column_type(c, &col.col_type);
    let c = if col.null.unwrap_or(true) == true {
        c
    } else {
        c.not_null()
    };
    let c = if let Some(ck) = &col.key {
        match ck {
            ua_model::ColumnKey::NotKey => c,
            ua_model::ColumnKey::Primary => c.primary_key(),
            ua_model::ColumnKey::Unique => c.unique_key(),
            ua_model::ColumnKey::Multiple => c,
        }
    } else {
        c
    };

    c
}

// todo: 1. return type; 2. generic Builder
pub fn create_table(table: &ua_model::TableCreate, create_if_not_exists: bool) -> String {
    let mut s = Table::create();
    s.table(Alias::new(&table.name));

    if create_if_not_exists {
        s.if_not_exists();
    }

    for c in &table.columns {
        s.col(create_column(c));
    }

    s.to_string(PostgresQueryBuilder)
}

pub fn alter_table(table: &ua_model::TableAlter) -> Vec<String> {
    let s = Table::alter().table(Alias::new(&table.name));
    let mut alter_series = vec![];

    for a in &table.alter {
        match a {
            ua_model::ColumnAlterCase::Add(c) => {
                alter_series.push(s.clone().add_column(create_column(c)));
            }
            ua_model::ColumnAlterCase::Modify(c) => {
                alter_series.push(s.clone().modify_column(create_column(c)));
            }
            ua_model::ColumnAlterCase::Rename(c) => {
                let from_name = Alias::new(&c.from_name);
                let to_name = Alias::new(&c.to_name);
                alter_series.push(s.clone().rename_column(from_name, to_name));
            }
            ua_model::ColumnAlterCase::Drop(c) => {
                alter_series.push(s.clone().drop_column(Alias::new(&c.name)));
            }
        }
    }

    alter_series
        .iter()
        .map(|a| a.to_string(PostgresQueryBuilder))
        .collect()
}

pub fn drop_table(table: &ua_model::TableDrop) -> String {
    let s = Table::drop().table(Alias::new(&table.name));

    s.to_string(PostgresQueryBuilder)
}

pub fn rename_table(table: &ua_model::TableRename) -> String {
    let from = Alias::new(&table.from);
    let to = Alias::new(&table.to);
    let s = Table::rename().table(from, to);

    s.to_string(PostgresQueryBuilder)
}

pub fn truncate_table(table: &ua_model::TableTruncate) -> String {
    let s = Table::truncate().table(Alias::new(&table.name));

    s.to_string(PostgresQueryBuilder)
}

fn convert_index_order(index_order: &ua_model::IndexOrder) -> IndexOrder {
    match index_order {
        ua_model::IndexOrder::Asc => IndexOrder::Asc,
        ua_model::IndexOrder::Desc => IndexOrder::Desc,
    }
}

pub fn create_index(index: &ua_model::IndexCreate) -> String {
    let mut s = Index::create();
    s = s.name(&index.name).table(Alias::new(&index.table));

    for i in &index.columns {
        match &i.order {
            Some(o) => {
                s = s.col((Alias::new(&i.name), convert_index_order(o)));
            }
            None => {
                s = s.col(Alias::new(&i.name));
            }
        }
    }

    s.to_string(PostgresQueryBuilder)
}

pub fn drop_index(index: &ua_model::IndexDrop) -> String {
    let s = Index::drop()
        .name(&index.name)
        .table(Alias::new(&index.table));

    s.to_string(PostgresQueryBuilder)
}

#[cfg(test)]
mod tests_sea {
    use super::*;

    #[test]
    fn test_table_create() {
        let table = ua_model::TableCreate {
            name: "test".to_string(),
            columns: vec![
                ua_model::Column {
                    name: "id".to_string(),
                    key: Some(ua_model::ColumnKey::Primary),
                    ..Default::default()
                },
                ua_model::Column {
                    name: "name".to_string(),
                    ..Default::default()
                },
            ],
        };

        println!("{:?}", create_table(&table, true));
    }

    #[test]
    fn test_table_alter() {
        let alter = ua_model::TableAlter {
            name: "test".to_string(),
            alter: vec![ua_model::ColumnAlterCase::Add(ua_model::Column {
                name: "name".to_string(),
                ..Default::default()
            })],
        };

        println!("{:?}", alter_table(&alter));
    }

    #[test]
    fn test_index_create() {
        let foo = Index::create()
            .name("233")
            .table(Alias::new("n"))
            .col(Alias::new("1"))
            .col(Alias::new("2"))
            .col(Alias::new("2"))
            .to_string(PostgresQueryBuilder);

        println!("{:?}", foo);
    }
}
