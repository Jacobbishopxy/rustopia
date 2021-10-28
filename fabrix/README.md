# Fabrix

Fabrix, a lib crate, which is built on `polars` Series and DataFrame as base data structures, communicates among different data sources, such as Database, File, and BSON/JSON. Furthermore, ETL process among different sources are provided as well, and additionally, manipulation or operation on data itself is enhanced.

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
│   │       ├── provider.rs
│   │       ├── types.rs
│   │       └── executor.rs
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
