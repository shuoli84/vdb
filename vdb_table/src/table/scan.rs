use crate::{Error, Table};
use rusqlite::Connection;

impl Table {
    /// scan to end, useful for index catch up
    pub fn scan_to_end(
        &self,
        conn: &Connection,
        from_version: i64,
        mut f: impl FnMut(Vec<u8>, Option<Vec<u8>>, i64) -> Result<(), Error>,
    ) -> Result<(), Error> {
        let mut stmt = conn.prepare_cached(&format!(
            r#"SELECT key, value, rowid FROM {table_name} WHERE rowid > :from_version AND is_latest = 1 ORDER BY rowid"#,
            table_name = self.data_table()
        ))?;

        let rows = no_row_to_none!(stmt.query(rusqlite::named_params! {
            ":from_version": from_version,
        }))?;

        let mut rows = match rows {
            None => return Ok(()),
            Some(rows) => rows,
        };

        while let Some(row) = rows.next()? {
            let key: Vec<u8> = row.get(0)?;
            let value: Option<Vec<u8>> = row.get(1)?;
            let v = row.get(2)?;
            f(key, value, v)?;
        }

        Ok(())
    }
}
