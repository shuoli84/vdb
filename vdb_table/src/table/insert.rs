use crate::{Error, Table, TableEvent, TableItemEvent, TableUpdate};

impl Table {
    pub fn insert(
        &self,
        conn: &mut rusqlite::Connection,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<i64, Error> {
        let trans = conn.transaction()?;

        let mut table_events = Vec::<TableItemEvent>::new();
        let (v, event) = self.inner_insert(&trans, key, value)?;
        table_events.push(event);

        trans.commit()?;

        self.observers
            .iter()
            .for_each(|ob| ob(TableEvent::DataUpdates(&table_events[..])));

        Ok(v)
    }

    pub fn insert_batch(
        &self,
        conn: &mut rusqlite::Connection,
        key_values: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), Error> {
        if key_values.is_empty() {
            return Ok(());
        }

        let trans = conn.transaction()?;

        let mut table_events = Vec::<TableItemEvent>::new();
        for (key, value) in key_values.into_iter() {
            let (_v, event) = self.inner_insert(&trans, key, value)?;
            table_events.push(event);
        }

        trans.commit()?;

        self.observers
            .iter()
            .for_each(|ob| ob(TableEvent::DataUpdates(&table_events[..])));

        Ok(())
    }

    /// insert key value into table, also updates attached indexes
    pub fn inner_insert(
        &self,
        trans: &rusqlite::Connection,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(i64, TableItemEvent), Error> {
        let last_value_and_v = if let Some(last_value) = self.get(&trans, &key)? {
            self.update_last_to_not_latest(&trans, &key)?;
            Some(last_value)
        } else {
            None
        };
        let mut stmt = trans.prepare_cached(
            format!(
                r#"INSERT INTO {table_name} (key, is_latest, is_deleted, value) VALUES (:key, 1, 0, :value)"#,
                table_name = self.data_table(),
            )
                .as_str(),
        )?;

        stmt.execute(rusqlite::named_params! {
            ":key": key,
            ":value": value,
        })?;
        drop(stmt);

        let v = trans.last_insert_rowid();

        let updates = [TableUpdate::Upsert(vec![(
            key.as_slice(),
            value.as_slice(),
            v,
        )])];

        // also update all indexes
        for index in self.indexes.iter() {
            index.table_update(&trans, &updates)?;
        }

        Ok((
            v,
            TableItemEvent {
                key,
                from: last_value_and_v.map(|(value, version)| (value, version.clone())),
                to: Some((value, v)),
            },
        ))
    }
}
