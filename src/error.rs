#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    DuckDB(duckdb::Error),
    FileNotFound(std::path::PathBuf),
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<duckdb::Error> for Error {
    fn from(e: duckdb::Error) -> Self {
        Error::DuckDB(e)
    }
}
