use crate::{connection::Connection, error::Error, frame::Dataframe, query::Expr, query::Query};

pub struct LazyFrame<'a, C: Connection> {
    frame: &'a Dataframe<'a, C>,
    query: Query,
}

impl<'a, C: Connection> LazyFrame<'a, C> {
    pub fn from_frame(frame: &'a Dataframe<'a, C>) -> Self {
        Self {
            frame,
            query: Query::default(),
        }
    }

    pub fn select(self, exprs: impl AsRef<[Expr]>) -> LazyFrame<'a, C> {
        Self {
            query: self.query.select(exprs.as_ref().to_vec()),
            ..self
        }
    }

    pub fn collect(self) -> Result<Dataframe<'a, C>, Error> {
        self.frame.query(self.query)
    }
}
