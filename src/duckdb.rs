use crate::{connection::Connection, error::Error, query::Query};
use duckdb::Connection as DuckDBConnection;
use log::info;
use std::{path::PathBuf, sync::atomic::AtomicU64};

pub struct DuckConnection {
    next_df_number: AtomicU64,
    conn: DuckDBConnection,
}

#[derive(Clone)]
pub enum DuckTableRef {
    InMemory(String),
    Parquet(PathBuf),
}

impl DuckTableRef {
    fn to_sql(&self) -> String {
        match self {
            DuckTableRef::InMemory(name) => "\"".to_string() + &name + "\"",
            DuckTableRef::Parquet(path) => "'".to_string() + &path.to_string_lossy() + "'",
        }
    }
}

impl DuckConnection {
    pub fn open_in_memory() -> Self {
        let conn = DuckDBConnection::open_in_memory().unwrap();
        Self {
            next_df_number: AtomicU64::new(0),
            conn,
        }
    }

    fn next_df_name(&self) -> String {
        format!(
            "df_{}",
            self.next_df_number
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        )
    }
}

impl Connection for DuckConnection {
    type TableRef = DuckTableRef;

    fn parquet_ref(&self, path: PathBuf) -> Self::TableRef {
        DuckTableRef::Parquet(path)
    }

    fn in_memory_ref(&self) -> Self::TableRef {
        DuckTableRef::InMemory(self.next_df_name())
    }

    fn clone_table(&self, table_ref: &Self::TableRef) -> Self::TableRef {
        match table_ref {
            DuckTableRef::InMemory(name) => {
                let new_name = self.next_df_name();
                info!("Cloning in-memory table {} to {}", name, new_name);
                self.conn
                    .execute(
                        &format!("CREATE TABLE {} AS SELECT * FROM {}", new_name, name),
                        [],
                    )
                    .unwrap();
                DuckTableRef::InMemory(new_name)
            }
            DuckTableRef::Parquet(path) => DuckTableRef::Parquet(path.clone()),
        }
    }

    fn drop_table(&self, table_ref: &Self::TableRef) {
        match table_ref {
            DuckTableRef::InMemory(name) => {
                info!("Dropping in-memory table {}", name);
                self.conn
                    .execute(&format!("DROP TABLE {}", name), [])
                    .unwrap();
            }
            DuckTableRef::Parquet(_) => {}
        }
    }

    fn exec_query(
        &self,
        query: Query,
        table_ref: &Self::TableRef,
    ) -> Result<Self::TableRef, Error> {
        let new_table_ref = self.next_df_name();
        let sql = query.to_duckdb_sql(&table_ref);
        log::info!("Executing query: {}", sql);
        self.conn
            .execute(&format!("CREATE TABLE {new_table_ref} AS {sql}"), [])?;
        Ok(DuckTableRef::InMemory(new_table_ref))
    }

    fn get_metadata(&self, table_ref: &Self::TableRef) -> Result<Vec<(String, String)>, Error> {
        let mut stmt = self
            .conn
            .prepare(&format!("DESCRIBE TABLE {}", table_ref.to_sql()))?;
        let mut rows = stmt.query([]).unwrap();
        let mut meta = Vec::new();
        while let Some(row) = rows.next().unwrap() {
            let column_name: String = row.get("column_name").unwrap();
            let column_type: String = row.get("column_type").unwrap();
            meta.push((column_name, column_type));
        }
        Ok(meta)
    }
}

impl Query {
    fn to_duckdb_sql(&self, table_ref: &DuckTableRef) -> String {
        self.to_sql(&table_ref.to_sql())
    }
}
