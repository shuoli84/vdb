use crate::{Error, TableUpdate};
use rusqlite::ToSql;

pub type Extractor = Box<dyn Fn(&[u8], &[u8]) -> Result<Vec<Vec<u8>>, Error> + Send + Sync>;

pub struct IndexOption {
    pub without_rowid: bool,
}

pub struct Index {
    pub name: String,
    data_table_name: String,
    config_table_name: String,
    option: IndexOption,
    extractor: Extractor,
}

impl Index {
    pub fn new(
        index_name: &str,
        table_name: &str,
        option: IndexOption,
        extractor: Extractor,
    ) -> Self {
        let data_table_name = format!("{}_idx_{}_data", table_name, index_name);
        let config_table_name = format!("{}_idx_{}_config", table_name, index_name);

        Self {
            name: index_name.to_string(),
            data_table_name,
            config_table_name,
            option,
            extractor,
        }
    }

    /// Create all required tables, then table will track
    /// each index's table and delete unused tables
    /// returns all tables created
    pub fn create_table(&self, conn: &rusqlite::Connection) -> Result<Vec<String>, Error> {
        let without_rowid = if self.option.without_rowid {
            "WITHOUT ROWID"
        } else {
            ""
        };

        conn.execute_batch(
            format!(
                r#"
                create table if not exists {data_table} (
                  ik blob,
                  pk blob,
                  primary key (ik, pk)
                ) {without_rowid};
                
                create index if not exists idx_{data_table}_pk on {data_table} (pk);
                
                create table if not exists {config_table} (
                  key integer primary key,
                  value integer
                )
                "#,
                data_table = self.data_table_name,
                config_table = self.config_table_name,
                without_rowid = without_rowid,
            )
            .as_str(),
        )?;

        Ok(vec![
            self.data_table_name.clone(),
            self.config_table_name.clone(),
        ])
    }

    pub fn table_update(
        &self,
        conn: &rusqlite::Connection,
        updates: &[TableUpdate],
    ) -> Result<(), Error> {
        for update in updates.iter() {
            match update {
                TableUpdate::Upsert(items) => {
                    for (key, value, version) in items.iter() {
                        self.update(conn, key, value, *version)?;
                    }
                }
                TableUpdate::Delete(keys) => {
                    for key in keys {
                        self.delete_by_pk(conn, key.0)?;
                    }
                }
            }
        }
        Ok(())
    }

    pub fn update(
        &self,
        conn: &rusqlite::Connection,
        pk: &[u8],
        value: &[u8],
        version: i64,
    ) -> Result<(), Error> {
        let prev_index_keys = self.inner_get_prev_keys(conn, pk)?;
        let new_index_keys = (self.extractor)(pk, value)?;

        let mut keys_to_delete = Vec::<&[u8]>::new();
        let mut keys_to_insert = Vec::<&[u8]>::new();

        for index_key in new_index_keys.iter() {
            if prev_index_keys.contains(index_key) {
                continue;
            }
            keys_to_insert.push(index_key);
        }

        for index_key in prev_index_keys.iter() {
            if new_index_keys.contains(index_key) {
                continue;
            }
            keys_to_delete.push(index_key);
        }

        self.inner_delete_iks(conn, keys_to_delete.as_slice())?;
        self.inner_insert_iks(conn, keys_to_insert.as_slice(), pk)?;

        // store version to config table
        self.inner_save_version(conn, version)?;
        Ok(())
    }

    fn delete_by_pk(&self, conn: &rusqlite::Connection, pk: &[u8]) -> Result<(), Error> {
        let mut stmt = conn.prepare_cached(&format!(
            r#"delete from {data_table} where pk = :pk"#,
            data_table = self.data_table_name
        ))?;

        stmt.execute(rusqlite::named_params! {
            ":pk": pk,
        })?;

        Ok(())
    }
}

/// Scan related methods
pub struct ScanKey<'a> {
    pub ik: &'a [u8],
    pub pk: &'a [u8],
    pub inclusive: bool,
}

pub enum ScanOrder {
    Asc,
    Desc,
}

pub struct ScanOptions<'a> {
    pub lower_key: Option<ScanKey<'a>>,
    pub higher_key: Option<ScanKey<'a>>,
    pub count: u32,
    pub order: ScanOrder,
}

impl ScanOptions<'_> {
    /// Convert the scan option to sql string and named params
    /// e.g:
    /// (ik, pk) >= (:l_ik, :l_pk)
    /// (ik, pk) > (:l_ik, :l_pk) AND (ik, pk) <= (:h_ik, :h_pk)
    fn where_clause(&self) -> (String, Vec<(&'static str, &[u8])>) {
        let mut clauses = vec![];
        let mut params = vec![];

        match self.lower_key.as_ref() {
            None => {}
            Some(scan_key) => {
                clauses.push("(ik, pk) >= (:lower_ik, :lower_pk)");
                params.push((":lower_ik", scan_key.ik));
                params.push((":lower_pk", scan_key.pk));
            }
        };
        match self.higher_key.as_ref() {
            None => {}
            Some(scan_key) => {
                clauses.push("(ik, pk) <= (:higher_ik, :higher_pk)");
                params.push((":higher_ik", scan_key.ik));
                params.push((":higher_pk", scan_key.pk));
            }
        };

        (clauses.join(" AND "), params)
    }

    fn order_by(&self) -> &str {
        match self.order {
            ScanOrder::Asc => "ORDER BY ik ASC, pk ASC",
            ScanOrder::Desc => "ORDER BY ik DESC, pk DESC",
        }
    }
}

pub struct ScanResult {
    pub keys: Vec<(Vec<u8>, Vec<u8>)>,
    pub has_more: bool,
}

impl Index {
    pub fn scan(
        &self,
        conn: &rusqlite::Connection,
        options: ScanOptions,
    ) -> Result<ScanResult, Error> {
        let (where_clause, where_params) = options.where_clause();

        let sql = format!(
            r#"SELECT ik, pk FROM {data_table} WHERE {where_clause} {order_clause} LIMIT :count"#,
            data_table = self.data_table_name,
            where_clause = where_clause,
            order_clause = options.order_by(),
        );
        let mut stmt = conn.prepare_cached(dbg!(&sql))?;

        let mut params = Vec::<(&'static str, &dyn ToSql)>::new();
        for (k, v) in where_params.iter() {
            params.push((k, v));
        }

        // +1 to detect has_more
        let query_count = options.count + 1;
        params.push((":count", &query_count));

        let mut rows = stmt.query(params.as_slice())?;

        let mut keys = Vec::new();
        while let Some(row) = rows.next()? {
            let ik: Vec<u8> = row.get(0)?;
            let pk: Vec<u8> = row.get(1)?;
            keys.push((ik, pk));
        }

        let has_more = keys.len() > options.count as usize;
        if has_more {
            keys.pop();
        }

        Ok(ScanResult { keys, has_more })
    }
}

impl Index {
    fn inner_get_prev_keys(
        &self,
        conn: &rusqlite::Connection,
        pk: &[u8],
    ) -> Result<Vec<Vec<u8>>, Error> {
        let mut stmt = conn.prepare_cached(&format!(
            r#"SELECT ik FROM {data_table} WHERE pk = :pk"#,
            data_table = self.data_table_name
        ))?;

        let mut rows = stmt.query(rusqlite::named_params! {
            ":pk": pk,
        })?;

        let mut keys = Vec::new();
        while let Some(row) = rows.next()? {
            let key: Vec<u8> = row.get(0)?;
            keys.push(key);
        }

        Ok(keys)
    }

    fn inner_insert_iks(
        &self,
        conn: &rusqlite::Connection,
        iks: &[&[u8]],
        pk: &[u8],
    ) -> Result<(), Error> {
        let (iks, pk) = dbg!((iks, pk));

        let mut stmt = conn.prepare_cached(&format!(
            r#"INSERT INTO {data_table} (ik, pk) VALUES (:ik, :pk)"#,
            data_table = self.data_table_name
        ))?;

        for ik in iks.iter() {
            stmt.execute(rusqlite::named_params! {
                ":ik": ik,
                ":pk": pk,
            })?;
        }

        Ok(())
    }

    fn inner_delete_iks(&self, conn: &rusqlite::Connection, iks: &[&[u8]]) -> Result<(), Error> {
        let mut stmt = conn.prepare_cached(&format!(
            r#"DELETE FROM {data_table} WHERE ik = :ik"#,
            data_table = self.data_table_name
        ))?;

        for ik in iks.iter() {
            stmt.execute(rusqlite::named_params! {
                ":ik": ik,
            })?;
        }

        Ok(())
    }

    fn inner_save_version(&self, conn: &rusqlite::Connection, version: i64) -> Result<(), Error> {
        let mut stmt = conn.prepare_cached(&format!(
            r#"INSERT OR REPLACE INTO {config_table} (key, value) VALUES ( 1, :version ) "#,
            config_table = self.config_table_name,
        ))?;

        stmt.execute(rusqlite::named_params! {
            ":version": version,
        })?;

        Ok(())
    }

    /// get index synced data version
    pub fn get_data_version(&self, conn: &rusqlite::Connection) -> Result<Option<i64>, Error> {
        let mut stmt = conn.prepare_cached(&format!(
            r#"SELECT value FROM {config_table} WHERE key = 1"#,
            config_table = self.config_table_name,
        ))?;
        let version: Option<i64> = no_row_to_none!(stmt.query_row([], |row| row.get(0)))?;
        Ok(version)
    }
}
