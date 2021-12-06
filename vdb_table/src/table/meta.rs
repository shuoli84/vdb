use crate::{Error, Table, TableEvent, TableUpdate};
use rusqlite::Connection;

impl Table {
    pub fn create_table(&self, conn: &rusqlite::Connection) -> Result<(), Error> {
        // create primary tables
        conn.execute_batch(
            format!(r#"
                CREATE TABLE IF NOT EXISTS {table_name} (
                  rowid INTEGER PRIMARY KEY AUTOINCREMENT,
                  key BLOB,
                  is_deleted BOOL,
                  is_latest BOOL,
                  value BLOB
                );

                CREATE TABLE IF NOT EXISTS {conf_table} (
                  key INTEGER PRIMARY KEY,
                  value BLOB
                );

                CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_name}_key_latest ON {table_name}(key, is_latest) WHERE is_latest = 1;
            "#,
                    table_name = self.data_table(),
                    conf_table = self.conf_table(),
            ).as_str()
        )?;

        // create index associated tables
        let mut tables_active = Vec::new();
        for index in self.indexes.iter() {
            tables_active.extend(index.create_table(conn)?.into_iter());
        }

        {
            // refresh index
            // todo: performance batch update
            for index in self.indexes.iter() {
                let synced = index.get_data_version(conn)?;
                self.scan_to_end(conn, synced.unwrap_or_default(), |key, value, version| {
                    match value {
                        None => index.table_update(
                            conn,
                            &[TableUpdate::Delete(vec![(key.as_slice(), version)])],
                        )?,
                        Some(value) => index.table_update(
                            conn,
                            &[TableUpdate::Upsert(vec![(
                                key.as_slice(),
                                value.as_slice(),
                                version,
                            )])],
                        )?,
                    }
                    Ok(())
                })?;
            }
        }

        {
            // manage associated tables
            let prev_tables = dbg!(self.load_associated_tables(conn)?);

            let tables_to_delete = prev_tables
                .iter()
                .filter(|t| !tables_active.contains(t))
                .collect::<Vec<_>>();

            for t in tables_to_delete.iter() {
                let _ = conn.execute_batch(&format!(
                    r#"DROP TABLE IF EXISTS {table_name};"#,
                    table_name = t
                ));
            }

            self.save_associated_tables(conn, tables_active)?;
        }

        {
            self.observers
                .iter()
                .for_each(|ob| ob(TableEvent::TableCreated));
        }

        Ok(())
    }

    pub(super) fn data_table(&self) -> String {
        format!("{}_$_data", self.table_name)
    }

    pub(super) fn conf_table(&self) -> String {
        format!("{}_$_conf", self.table_name)
    }

    fn save_associated_tables(
        &self,
        conn: &rusqlite::Connection,
        tables: Vec<String>,
    ) -> Result<(), Error> {
        let mut stmt = conn.prepare_cached(&format!(
            r#"INSERT OR REPLACE INTO {config_table} (key, value) VALUES ( 2, :tables ) "#,
            config_table = self.conf_table(),
        ))?;

        stmt.execute(rusqlite::named_params! {
            ":tables": tables.join(","),
        })?;

        Ok(())
    }

    fn load_associated_tables(&self, conn: &Connection) -> Result<Vec<String>, Error> {
        let mut stmt = conn.prepare_cached(&format!(
            r#"SELECT value FROM {config_table} WHERE key = 2"#,
            config_table = self.conf_table(),
        ))?;
        let version: Option<String> = no_row_to_none!(stmt.query_row([], |row| row.get(0)))?;

        Ok(version
            .unwrap_or_default()
            .split(",")
            .map(|s| s.to_string())
            .collect())
    }
}
