# Ubiquitous Alchemy Server

## Dependencies

1. [actix-web](https://github.com/actix/actix-web): http service
1. [serde](https://github.com/serde-rs/serde): Json (de)serialize
1. [sea-query](https://github.com/SeaQL/sea-query): Sql string generator
1. [sqlx](https://github.com/launchbadge/sqlx): Sql connector & executor
1. [rbatis](https://github.com/rbatis/rbatis): Sql ORM

## Project Structure

1. ua-service:

   - dao: database access object
   - error: ua-service error handling
   - interface: business logic's interface
   - provider: Sql string generator
   - repository: business logic's implement
   - util: utilities

1. ua-persistence:

   - model: data persistence

1. ua-application

   - controller: Http routes
   - service: integration of business logic's implement
   - error: ua-applications error handling
   - constant: env file variables
   - model: business integration & data persistence

## API Description

1. configuration:

   - [POST] check_connection
   - [GET] conn
   - [POST] conn
   - [PUT] conn
   - [DELETE] conn

1. schema:

   - [GET] table_list
   - [GET] column_list
   - [POST] table_create
   - [POST] table_alter
   - [POST] table_drop
   - [POST] table_rename
   - [POST] table_truncate
   - [POST] index_create
   - [POST] index_drop
   - [POST] foreign_key_create
   - [POST] foreign_key_drop

1. query:

   - [POST] select
