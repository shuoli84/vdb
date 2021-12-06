use crate::{Error, Table, TableEvent, TableItemEvent};

impl Table {
    pub fn delete(&self, conn: &mut rusqlite::Connection, key: &[u8]) -> Result<i64, Error> {
        let trans = conn.transaction()?;

        if !self.update_last_to_not_latest(&trans, key)? {
            return Ok(0);
        }

        trans.execute(
            format!(
                r#"insert into {table_name} (key, is_latest, is_deleted, value) values (:key, 1, 1, '')"#,
                table_name = self.data_table()
            )
                .as_str(),
            rusqlite::named_params! {
                ":key": key
            },
        )?;

        let last_version = trans.last_insert_rowid();

        trans.commit()?;

        Ok(last_version)
    }

    pub fn delete_with_version(
        &self,
        conn: &mut rusqlite::Connection,
        key: Vec<u8>,
        version: i64,
    ) -> Result<i64, Error> {
        let trans = conn.transaction()?;

        let last_value = self.get_by_version(&trans, &key, version)?;
        let modified = self.update_last_to_not_latest_with_version(&trans, &key, version)?;

        if !modified {
            return Ok(0);
        }

        trans.execute(
            format!(
                r#"insert into {table_name} (key, is_latest, is_deleted, value) values (:key, 1, 1, '')"#,
                table_name = self.data_table()
            )
                .as_str(),
            rusqlite::named_params! {
                ":key": key
            },
        )?;

        let last_version = trans.last_insert_rowid();

        trans.commit()?;

        let data_events = vec![TableItemEvent {
            key,
            from: last_value.map(|x| (x, version)),
            to: None,
        }];
        self.observers
            .iter()
            .for_each(|o| o(TableEvent::DataUpdates(data_events.as_slice())));

        Ok(last_version)
    }
}
