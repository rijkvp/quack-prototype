mod connection;
mod duckdb;
mod error;
mod frame;
mod lazy;
mod query;

pub mod prelude {
    pub use crate::duckdb::DuckConnection;
    pub use crate::frame::Dataframe;
    pub use crate::lazy::LazyFrame;
    pub use crate::query::*;
}
