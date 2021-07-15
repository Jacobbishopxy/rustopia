//!

use sea_query::*;

use crate::util::DataEnum;

pub const PG_BUILDER: Builder = Builder::PG;
pub const MY_BUILDER: Builder = Builder::MY;

impl From<sqlz::DataEnum> for DataEnum {
    fn from(d: sqlz::DataEnum) -> Self {
        match d {
            sqlz::DataEnum::Integer(d) => DataEnum::Integer(d),
            sqlz::DataEnum::Float(d) => DataEnum::Float(d),
            sqlz::DataEnum::String(d) => DataEnum::String(d),
            sqlz::DataEnum::Bool(d) => DataEnum::Bool(d),
            sqlz::DataEnum::Null => DataEnum::Null,
        }
    }
}

impl Into<Value> for DataEnum {
    fn into(self) -> Value {
        match self {
            DataEnum::Integer(v) => Value::BigInt(v),
            DataEnum::Float(v) => Value::Double(v),
            DataEnum::String(v) => Value::String(Box::new(v)),
            DataEnum::Bool(v) => Value::Bool(v),
            DataEnum::Null => Value::Null,
        }
    }
}

fn gen_column_type(c: ColumnDef, col_type: &sqlz::ColumnType) -> ColumnDef {
    match col_type {
        sqlz::ColumnType::Binary => c.binary(),
        sqlz::ColumnType::Bool => c.boolean(),
        sqlz::ColumnType::Int => c.integer(),
        sqlz::ColumnType::Float => c.float(),
        sqlz::ColumnType::Double => c.double(),
        sqlz::ColumnType::Date => c.date(),
        sqlz::ColumnType::Time => c.time(),
        sqlz::ColumnType::DateTime => c.date_time(),
        sqlz::ColumnType::Timestamp => c.timestamp(),
        sqlz::ColumnType::Char => c.char(),
        sqlz::ColumnType::VarChar => c.string(),
        sqlz::ColumnType::Text => c.text(),
        sqlz::ColumnType::Json => c.json(),
    }
}

fn gen_column(col: &sqlz::Column) -> ColumnDef {
    let c = ColumnDef::new(Alias::new(&col.name));
    let c = gen_column_type(c, &col.col_type);
    let c = if col.null.unwrap_or(true) == true {
        c
    } else {
        c.not_null()
    };
    let c = if let Some(ck) = &col.key {
        match ck {
            sqlz::ColumnKey::NotKey => c,
            sqlz::ColumnKey::Primary => c.primary_key(),
            sqlz::ColumnKey::Unique => c.unique_key(),
            sqlz::ColumnKey::Multiple => c,
        }
    } else {
        c
    };

    c
}

fn convert_foreign_key_action(foreign_key_action: &sqlz::ForeignKeyAction) -> ForeignKeyAction {
    match foreign_key_action {
        sqlz::ForeignKeyAction::Restrict => ForeignKeyAction::Restrict,
        sqlz::ForeignKeyAction::Cascade => ForeignKeyAction::Cascade,
        sqlz::ForeignKeyAction::SetNull => ForeignKeyAction::SetNull,
        sqlz::ForeignKeyAction::NoAction => ForeignKeyAction::NoAction,
        sqlz::ForeignKeyAction::SetDefault => ForeignKeyAction::SetDefault,
    }
}

fn convert_index_order(index_order: &sqlz::OrderType) -> IndexOrder {
    match index_order {
        sqlz::OrderType::Asc => IndexOrder::Asc,
        sqlz::OrderType::Desc => IndexOrder::Desc,
    }
}

fn gen_foreign_key(key: &sqlz::ForeignKeyCreate) -> ForeignKeyCreateStatement {
    ForeignKey::create()
        .name(&key.name)
        .from(Alias::new(&key.from.table), Alias::new(&key.from.column))
        .to(Alias::new(&key.to.table), Alias::new(&key.to.column))
        .on_delete(convert_foreign_key_action(&key.on_delete))
        .on_update(convert_foreign_key_action(&key.on_update))
}

fn filter_builder(qs: &mut SelectStatement, flt: &Vec<sqlz::Expression>) {
    let mut vec_cond: Vec<Condition> = vec![Cond::all()];

    flt.iter().for_each(|e| match e {
        sqlz::Expression::Conjunction(c) => {
            match c {
                sqlz::Conjunction::AND => vec_cond.push(Cond::all()),
                sqlz::Conjunction::OR => vec_cond.push(Cond::any()),
            };
        }
        sqlz::Expression::Simple(c) => {
            let tmp_expr = Expr::col(Alias::new(&c.column));
            let tmp_expr = match &c.equation {
                sqlz::Equation::Equal(d) => tmp_expr.eq(DataEnum::from(d.clone())),
                sqlz::Equation::NotEqual(d) => tmp_expr.ne(DataEnum::from(d.clone())),
                sqlz::Equation::Greater(d) => tmp_expr.gt(DataEnum::from(d.clone())),
                sqlz::Equation::GreaterEqual(d) => tmp_expr.gte(DataEnum::from(d.clone())),
                sqlz::Equation::Less(d) => tmp_expr.lt(DataEnum::from(d.clone())),
                sqlz::Equation::LessEqual(d) => tmp_expr.lte(DataEnum::from(d.clone())),
                sqlz::Equation::In(d) => {
                    tmp_expr.is_in(d.iter().map(|e| DataEnum::from(e.clone())))
                }
                sqlz::Equation::Between(d) => {
                    tmp_expr.between(DataEnum::from(d.0.clone()), DataEnum::from(d.1.clone()))
                }
                sqlz::Equation::Like(d) => tmp_expr.like(&d),
            };
            let last = vec_cond.last().unwrap().clone();
            let mut_last = vec_cond.last_mut().unwrap();
            *mut_last = last.add(tmp_expr);
        }
        sqlz::Expression::Nest(n) => filter_builder(qs, n),
    });

    vec_cond.iter().for_each(|c| {
        qs.cond_where(c.clone());
    });
}

/// Sql string builder
pub enum Builder {
    PG,
    MY,
}

impl Builder {
    /// list all columns
    pub fn list_column(&self, table: &str) -> String {
        let query_str = r##"
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = 'table'
        "##;
        match &self {
            Builder::PG => query_str.replace("table", table),
            Builder::MY => query_str.replace("table", table),
        }
    }

    /// List all table's name
    pub fn list_table(&self) -> String {
        match &self {
            Builder::MY => "SHOW TABLES;".to_owned(),
            Builder::PG => r##"
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='public'
            AND table_type='BASE TABLE';
            "##
            .to_owned(),
        }
    }

    /// create a table
    pub fn create_table(&self, table: &sqlz::TableCreate, create_if_not_exists: bool) -> String {
        let mut s = Table::create();
        s.table(Alias::new(&table.name));

        if create_if_not_exists {
            s.if_not_exists();
        }

        for c in &table.columns {
            s.col(gen_column(c));
        }

        if let Some(f) = &table.foreign_key {
            s.foreign_key(gen_foreign_key(f));
        }

        match &self {
            Builder::MY => s.to_string(MysqlQueryBuilder),
            Builder::PG => s.to_string(PostgresQueryBuilder),
        }
    }

    /// alter a table
    pub fn alter_table(&self, table: &sqlz::TableAlter) -> Vec<String> {
        let s = Table::alter().table(Alias::new(&table.name));
        let mut alter_series = vec![];

        for a in &table.alter {
            match a {
                sqlz::ColumnAlterCase::Add(c) => {
                    alter_series.push(s.clone().add_column(gen_column(c)));
                }
                sqlz::ColumnAlterCase::Modify(c) => {
                    alter_series.push(s.clone().modify_column(gen_column(c)));
                }
                sqlz::ColumnAlterCase::Rename(c) => {
                    let from_name = Alias::new(&c.from_name);
                    let to_name = Alias::new(&c.to_name);
                    alter_series.push(s.clone().rename_column(from_name, to_name));
                }
                sqlz::ColumnAlterCase::Drop(c) => {
                    alter_series.push(s.clone().drop_column(Alias::new(&c.name)));
                }
            }
        }

        alter_series
            .iter()
            .map(|_| match &self {
                Builder::MY => s.to_string(MysqlQueryBuilder),
                Builder::PG => s.to_string(PostgresQueryBuilder),
            })
            .collect()
    }

    /// drop a table
    pub fn drop_table(&self, table: &sqlz::TableDrop) -> String {
        let s = Table::drop().table(Alias::new(&table.name));

        match &self {
            Builder::MY => s.to_string(MysqlQueryBuilder),
            Builder::PG => s.to_string(PostgresQueryBuilder),
        }
    }

    /// rename a table
    pub fn rename_table(&self, table: &sqlz::TableRename) -> String {
        let from = Alias::new(&table.from);
        let to = Alias::new(&table.to);
        let s = Table::rename().table(from, to);

        match &self {
            Builder::MY => s.to_string(MysqlQueryBuilder),
            Builder::PG => s.to_string(PostgresQueryBuilder),
        }
    }

    /// truncate a table
    pub fn truncate_table(&self, table: &sqlz::TableTruncate) -> String {
        let s = Table::truncate().table(Alias::new(&table.name));

        match &self {
            Builder::MY => s.to_string(MysqlQueryBuilder),
            Builder::PG => s.to_string(PostgresQueryBuilder),
        }
    }

    /// create an index
    pub fn create_index(&self, index: &sqlz::IndexCreate) -> String {
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

        match &self {
            Builder::MY => s.to_string(MysqlQueryBuilder),
            Builder::PG => s.to_string(PostgresQueryBuilder),
        }
    }

    /// drop an index
    pub fn drop_index(&self, index: &sqlz::IndexDrop) -> String {
        let s = Index::drop()
            .name(&index.name)
            .table(Alias::new(&index.table));

        match &self {
            Builder::MY => s.to_string(MysqlQueryBuilder),
            Builder::PG => s.to_string(PostgresQueryBuilder),
        }
    }

    /// create a foreign key
    pub fn create_foreign_key(&self, key: &sqlz::ForeignKeyCreate) -> String {
        let s = gen_foreign_key(key);

        match &self {
            Builder::MY => s.to_string(MysqlQueryBuilder),
            Builder::PG => s.to_string(PostgresQueryBuilder),
        }
    }

    /// drop a foreign key
    pub fn drop_foreign_key(&self, key: &sqlz::ForeignKeyDrop) -> String {
        let s = ForeignKey::drop()
            .name(&key.name)
            .table(Alias::new(&key.table));

        match &self {
            Builder::MY => s.to_string(MysqlQueryBuilder),
            Builder::PG => s.to_string(PostgresQueryBuilder),
        }
    }

    /// select <columns> from <table> where <conditions> ...
    pub fn select(&self, select: &sqlz::Select) -> String {
        let mut s = Query::select();

        for c in &select.columns {
            s.column(Alias::new(&c.name()));
        }

        s.from(Alias::new(&select.table));

        if let Some(flt) = &select.filter {
            filter_builder(&mut s, flt);
        }

        if let Some(ord) = &select.order {
            ord.iter().for_each(|o| match &o.order {
                Some(ot) => match ot {
                    sqlz::OrderType::Asc => {
                        s.order_by(Alias::new(&o.name), Order::Asc);
                    }
                    sqlz::OrderType::Desc => {
                        s.order_by(Alias::new(&o.name), Order::Desc);
                    }
                },
                None => {
                    s.order_by(Alias::new(&o.name), Order::Asc);
                }
            })
        }

        if let Some(l) = &select.limit {
            s.limit(l.clone());
        }

        if let Some(o) = &select.offset {
            s.offset(o.clone());
        }

        match &self {
            Builder::MY => s.to_string(MysqlQueryBuilder),
            Builder::PG => s.to_string(PostgresQueryBuilder),
        }
    }
}

#[cfg(test)]
mod tests_sea {
    use super::*;

    #[test]
    fn test_table_create() {
        let table = sqlz::TableCreate {
            name: "test".to_string(),
            columns: vec![
                sqlz::Column {
                    name: "id".to_string(),
                    key: Some(sqlz::ColumnKey::Primary),
                    ..Default::default()
                },
                sqlz::Column {
                    name: "name".to_string(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        println!("{:?}", Builder::PG.create_table(&table, true));
    }

    #[test]
    fn test_table_alter() {
        let alter = sqlz::TableAlter {
            name: "test".to_string(),
            alter: vec![sqlz::ColumnAlterCase::Add(sqlz::Column {
                name: "name".to_string(),
                ..Default::default()
            })],
        };

        println!("{:?}", Builder::PG.alter_table(&alter));
    }

    #[test]
    fn test_index_create() {
        let index = sqlz::IndexCreate {
            name: "dev".to_owned(),
            table: "test".to_owned(),
            columns: vec![sqlz::Order {
                name: "i".to_owned(),
                ..Default::default()
            }],
        };

        println!("{:?}", Builder::PG.create_index(&index));
    }

    #[test]
    fn test_select() {
        let conditions = vec![
            sqlz::Expression::Simple(sqlz::Condition {
                column: "c1".to_owned(),
                equation: sqlz::Equation::Between((
                    sqlz::DataEnum::Integer(23),
                    sqlz::DataEnum::Integer(25),
                )),
            }),
            sqlz::Expression::Conjunction(sqlz::Conjunction::OR),
            sqlz::Expression::Simple(sqlz::Condition {
                column: "c2".to_owned(),
                equation: sqlz::Equation::Equal(sqlz::DataEnum::Integer(1)),
            }),
            sqlz::Expression::Conjunction(sqlz::Conjunction::AND),
            sqlz::Expression::Nest(vec![
                sqlz::Expression::Simple(sqlz::Condition {
                    column: "c3".to_owned(),
                    equation: sqlz::Equation::Greater(sqlz::DataEnum::Integer(23)),
                }),
                sqlz::Expression::Conjunction(sqlz::Conjunction::AND),
                sqlz::Expression::Simple(sqlz::Condition {
                    column: "c4".to_owned(),
                    equation: sqlz::Equation::In(vec![
                        sqlz::DataEnum::from("T1"),
                        sqlz::DataEnum::from("T2"),
                    ]),
                }),
            ]),
        ];
        let selection = sqlz::Select {
            table: "sqlz".to_owned(),
            columns: vec![
                sqlz::ColumnAlias::Simple("c1".to_owned()),
                sqlz::ColumnAlias::Alias(("c2".to_owned(), "c2_t".to_owned())),
            ],
            filter: Some(conditions),
            order: None,
            limit: Some(10),
            offset: Some(20),
        };

        let sql_str = Builder::PG.select(&selection);

        println!("{:?}", sql_str);
    }

    #[test]
    fn test_list_column() {
        let q = Builder::PG.list_column("ss1s");

        println!("{}", q);
    }
}
