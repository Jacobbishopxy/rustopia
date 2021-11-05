# Fabrix

Fabrix is a lib crate, who uses [Polars](https://github.com/pola-rs/polars) Series and DataFrame as fundamental data structures, and is capable to communicate among different data sources, such as Database (MySql/Postgres/Sqlite), File, BSON/JSON and etc. Furthermore, ETL process among different sources are provided as well, and additionally, manipulation or operation on data itself is enhanced.

## Structure

```txt
├── core
│   ├── value.rs                        // the smallest data unit
│   ├── series.rs                       // series of value
│   ├── dataframe.rs                    // collection of series, with index series
│   ├── row.rs                          // row-wise data structure
│   └── util.rs                         // utility functions
│
├── sources
│   ├── db
│   │   ├── sql_builder                 // SQL builder
│   │   │   ├── ddl                     // DDL
│   │   │   │   ├── query.rs
│   │   │   │   └── mutation.rs
│   │   │   ├── dml                     // DML
│   │   │   │   ├── query.rs
│   │   │   │   └── mutation.rs
│   │   │   ├── adt.rs                  // algebraic data type
│   │   │   ├── builder.rs              // SQL builder & ddl/dml logic implement
│   │   │   ├── interface.rs            // SQL builder & ddl/dml logic interface
│   │   │   └── macros.rs
│   │   │
│   │   └── sql_executor
│   │       ├── types.rs                // Conversion between Sql data type and Fabrix `Value`
│   │       ├── processor.rs            // Sql row process, turn raw sql row into `Vec<Value>` or `Row`
│   │       ├── loader.rs               // Database loader, CRUD logic implementation
│   │       └── executor.rs             // Sql executor, business logic implementation
│   │
│   ├── file
│   │
│   └── json
│
├── errors.rs                           // error handling
│
├── macros.rs                           // helpful macros
│
├── prelude.rs                          // prelude of this crate
│
└── lib.rs
```

## Note

Developing process of `dataframe/core`: `value` -> `series` -> `dataframe` -> `row`
