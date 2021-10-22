# Fabrix

This crate is a lib crate, which based on `polars` Series and DataFrame as data storage, communicates among different data sources: Database, File, and BSON/JSON. ETL process among different sources are provided, and additionally, manipulation or operation on data itself is enhanced.

## Note

- `core`

  Developing process: `value` -> `series` -> `dataframe` -> `row`

- `source`

  - `db`

  - `file`

  - `json`
