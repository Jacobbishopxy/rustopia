# Tiny DF

A tiny row based dataframe.

- Core

  - meta: data type declaration, based on Rust primitive data type
  - series: a collection of heterogenous 1d vector
  - dataframe: a collection of heterogenous 2d vector
  - macros: `d1!`, `d2!`, `series!` and `df!`

- De

- Se

  - json: JSON serialize
  - sql: Sql string serialize

- Db

## TODO

- considering strict data type mode for a `Dataframe`, with fixed cols (vec with cap) and fixed data orientation: `[series#1, series#2, ...]` and each series is guaranteed by the same length. In other hands, a `Dataframe` is a fixed len vec of `series`. Whereas strict `series` is required as well.
