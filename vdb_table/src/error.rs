#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("[vdb_table] Sqlite Error")]
    SqliteError(#[from] rusqlite::Error),

    #[error("[vdb_table] Value Error")]
    ValueError(#[from] vdb_value::Error),

    #[error("[vdb_table] Key Error")]
    KeyError(#[from] vdb_key::Error),

    #[error("[vdb_table] Index missing {0}")]
    IndexMissing(String),
}
