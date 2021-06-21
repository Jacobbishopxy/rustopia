# Ubiquitous Alchemy Server

## Dependencies

1. [actix-web](https://github.com/actix/actix-web): http service
1. [serde](https://github.com/serde-rs/serde): Json (de)serialize
1. [sea-query](https://github.com/SeaQL/sea-query): Sql string generator
1. [sqlx](https://github.com/launchbadge/sqlx): Sql connector & executor

## Project Structure

1. ua-domain-model:

   - query: database DML domain model
   - schema: database DDL domain model

1. ua-service:

   - dao: database access object
   - error: ua-service error handling
   - interface: business logic's interface
   - provider: Sql string generator
   - repository: business logic's implement
   - util: utilities

1. ua-application

   - controller: Http routes
   - error: ua-applications error handling
   - persistence: app data persistence
   - service: integration of business logic's implement
