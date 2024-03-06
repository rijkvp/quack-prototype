use crate::{error::Error, query::Query};
use std::path::PathBuf;

pub trait Connection {
    type TableRef: Sized + Clone;

    fn parquet_ref(&self, path: PathBuf) -> Self::TableRef;
    fn in_memory_ref(&self) -> Self::TableRef;

    fn clone_table(&self, table_ref: &Self::TableRef) -> Self::TableRef;
    fn drop_table(&self, table_ref: &Self::TableRef);

    fn exec_query(&self, query: Query, table_ref: &Self::TableRef) -> Result<Self::TableRef, Error>;

    fn get_metadata(&self, table_ref: &Self::TableRef) -> Result<Vec<(String, String)>, Error>;
}
