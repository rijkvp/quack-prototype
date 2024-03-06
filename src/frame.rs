use crate::{connection::Connection, error::Error, lazy::LazyFrame, query::col, query::Query};
use std::path::Path;

pub struct Dataframe<'a, C: Connection> {
    table_ref: C::TableRef,
    connection: &'a C,
}

impl<'a, C: Connection> Dataframe<'a, C> {
    pub fn open_parquet(path: impl AsRef<Path>, connection: &'a C) -> Result<Dataframe<C>, Error> {
        let path = path.as_ref().canonicalize()?;
        if !path.exists() {
            return Err(Error::FileNotFound(path));
        }
        Ok(Self {
            table_ref: connection.parquet_ref(path),
            connection,
        })
    }

    pub fn meta(&self) -> Result<Vec<(String, String)>, Error> {
        Ok(self.connection.get_metadata(&self.table_ref)?)
    }

    pub fn query(&self, query: Query) -> Result<Dataframe<C>, Error> {
        let new_ref = self.connection.exec_query(query, &self.table_ref)?;
        Ok(Dataframe {
            table_ref: new_ref,
            connection: self.connection,
        })
    }

    pub fn select<I, S>(&self, selection: I) -> Result<Dataframe<C>, Error>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.query(
            Query::default().select(selection.into_iter().map(|s| col(s.as_ref())).collect()),
        )
    }

    pub fn lazy(&self) -> LazyFrame<C> {
        LazyFrame::from_frame(self)
    }
}

impl<C: Connection> Clone for Dataframe<'_, C> {
    fn clone(&self) -> Self {
        Self {
            table_ref: C::clone_table(&self.connection, &self.table_ref),
            connection: self.connection,
        }
    }
}

impl<C: Connection> Drop for Dataframe<'_, C> {
    fn drop(&mut self) {
        C::drop_table(&self.connection, &self.table_ref);
    }
}
