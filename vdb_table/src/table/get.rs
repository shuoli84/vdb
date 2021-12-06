use crate::{Error, Table};

impl Table {
    /// Get latest value
    pub fn get(
        &self,
        conn: &rusqlite::Connection,
        key: &[u8],
    ) -> Result<Option<(Vec<u8>, i64)>, Error> {
        let mut stmt = conn.prepare_cached(
            format!(
                r#"select value, rowid from {table_name} where key = :key and is_latest = 1 and is_deleted <> 1"#,
                table_name = self.data_table()
            ).as_str(),
        )?;

        no_row_to_none!(stmt.query_row(
            rusqlite::named_params! {
                ":key": key
            },
            |r| Ok((r.get(0)?, r.get(1)?)),
        ))
        .map_err(Into::into)
    }

    /// Get value at specific version
    pub fn get_by_version(
        &self,
        conn: &rusqlite::Connection,
        key: &[u8],
        version: i64,
    ) -> Result<Option<Vec<u8>>, Error> {
        let mut stmt = conn.prepare_cached(
            format!(
                r#"select value from {table_name} where key = :key and rowid = :version and is_deleted <> 1"#,
                table_name = self.data_table()
            ).as_str(),
        )?;

        no_row_to_none!(stmt.query_row(
            rusqlite::named_params! {
                ":key": key,
                ":version": version,
            },
            |r| r.get(0),
        ))
        .map_err(Into::into)
    }
}
