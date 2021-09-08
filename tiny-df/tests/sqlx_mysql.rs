use chrono::NaiveDate;
use rust_decimal::Decimal;

use sea_query::{ColumnDef, Expr, Func, Iden, MysqlQueryBuilder, Order, Query, Table};
use sqlx::{types::chrono::NaiveDateTime, MySqlPool};

use serde_json::{json, Value as Json};
use uuid::Uuid;

#[tokio::test]
async fn main() {
    let connection = MySqlPool::connect("mysql://root:secret@localhost:3306/dev")
        .await
        .unwrap();
    let mut pool = connection.try_acquire().unwrap();

    // Schema
    let sql = Table::create()
        .table(Character::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(Character::Id)
                .integer()
                .not_null()
                .auto_increment()
                .primary_key(),
        )
        .col(ColumnDef::new(Character::Uuid).uuid())
        .col(ColumnDef::new(Character::FontSize).integer())
        .col(ColumnDef::new(Character::Character).string())
        .col(ColumnDef::new(Character::Meta).json())
        .col(ColumnDef::new(Character::Decimal).decimal())
        .col(ColumnDef::new(Character::Created).date_time())
        .build(MysqlQueryBuilder);

    let result = sqlx::query(&sql).execute(&mut pool).await;
    println!("Create table character: {:?}\n", result);

    let id = Uuid::new_v4();

    // Create
    let (sql, values) = Query::insert()
        .into_table(Character::Table)
        .columns(vec![
            Character::Uuid,
            Character::FontSize,
            Character::Character,
            Character::Meta,
            Character::Decimal,
            Character::Created,
        ])
        .values_panic(vec![
            id.into(),
            12.into(),
            "A".into(),
            json!({
                "notes": "some notes here",
            })
            .into(),
            Decimal::from_i128_with_scale(3141i128, 3).into(),
            NaiveDate::from_ymd(2020, 8, 20).and_hms(0, 0, 0).into(),
        ])
        .build(MysqlQueryBuilder);

    // Read
    let (sql, values) = Query::select()
        .columns(vec![
            Character::Id,
            Character::Uuid,
            Character::Character,
            Character::FontSize,
            Character::Meta,
            Character::Decimal,
            Character::Created,
        ])
        .from(Character::Table)
        .order_by(Character::Id, Order::Desc)
        .limit(1)
        .build(MysqlQueryBuilder);

    // Update
    let (sql, values) = Query::update()
        .table(Character::Table)
        .values(vec![(Character::FontSize, 24.into())])
        .and_where(Expr::col(Character::Id).eq(id))
        .build(MysqlQueryBuilder);

    // Read

    let (sql, values) = Query::select()
        .columns(vec![
            Character::Id,
            Character::Uuid,
            Character::Character,
            Character::FontSize,
            Character::Meta,
            Character::Decimal,
            Character::Created,
        ])
        .from(Character::Table)
        .order_by(Character::Id, Order::Desc)
        .limit(1)
        .build(MysqlQueryBuilder);

    // Count
    let (sql, values) = Query::select()
        .from(Character::Table)
        .expr(Func::count(Expr::col(Character::Id)))
        .build(MysqlQueryBuilder);

    // Delete
    let (sql, values) = Query::delete()
        .from_table(Character::Table)
        .and_where(Expr::col(Character::Id).eq(id))
        .build(MysqlQueryBuilder);
}

#[derive(Iden)]
enum Character {
    Table,
    Id,
    Uuid,
    Character,
    FontSize,
    Meta,
    Decimal,
    Created,
}

#[derive(sqlx::FromRow, Debug)]
struct CharacterStruct {
    id: i32,
    uuid: Uuid,
    character: String,
    font_size: i32,
    meta: Json,
    decimal: Decimal,
    created: NaiveDateTime,
}
