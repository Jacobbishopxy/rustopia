#[cfg(any(feature = "json", feature = "sql"))]
pub mod df;
#[cfg(feature = "json")]
pub mod json;
#[cfg(feature = "sql")]
pub mod sql;

#[cfg(any(feature = "json", feature = "sql"))]
pub use df::{Dataframe, DataframeData, DataframeRow};
