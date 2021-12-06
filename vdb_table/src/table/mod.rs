use crate::index::{Extractor, Index, IndexOption};
use crate::Error;

pub enum TableUpdate<'a> {
    Upsert(Vec<(&'a [u8], &'a [u8], i64)>),
    Delete(Vec<(&'a [u8], i64)>),
}

#[derive(Debug)]
pub struct TableItemEvent {
    key: Vec<u8>,
    from: Option<(Vec<u8>, i64)>,
    to: Option<(Vec<u8>, i64)>,
}

#[derive(Debug)]
pub enum TableEvent<'a> {
    TableCreated,
    DataUpdates(&'a [TableItemEvent]),
}

pub type TableObserver = Box<dyn Fn(TableEvent<'_>)>;

/// Table just provides bytes key value interface
pub struct Table {
    table_name: String,
    indexes: Vec<Index>,
    observers: Vec<TableObserver>,
}

impl Table {
    /// Create a new table named `name`
    pub fn new(name: String) -> Self {
        Self {
            table_name: name,
            indexes: vec![],
            observers: vec![],
        }
    }

    /// Append index defined by Extractor
    pub fn append_index(&mut self, name: &str, extractor: Extractor) {
        let index = Index::new(
            name,
            self.table_name.as_str(),
            IndexOption {
                without_rowid: true,
            },
            extractor,
        );
        self.indexes.push(index);
    }

    /// Append an update observer
    pub fn append_observer(&mut self, observer: TableObserver) {
        self.observers.push(observer);
    }
}

impl Table {
    /// update last item for same key as not latest
    fn update_last_to_not_latest_with_version(
        &self,
        trans: &rusqlite::Connection,
        key: &[u8],
        version: i64,
    ) -> Result<bool, Error> {
        let modified = trans.execute(
            format!(
                r#"update {table_name} set is_latest = 0 where rowid = :version and key = :key and is_latest = 1"#,
                table_name = self.data_table()
            )
                .as_str(),
            rusqlite::named_params! {
                ":version": version,
                ":key": key,
            },
        )?;

        Ok(modified != 0)
    }

    fn update_last_to_not_latest(
        &self,
        trans: &rusqlite::Connection,
        key: &[u8],
    ) -> Result<bool, Error> {
        let modified = trans.execute(
            format!(
                r#"update {table_name} set is_latest = 0 where key = :key and is_latest = 1"#,
                table_name = self.data_table()
            )
            .as_str(),
            rusqlite::named_params! {
                ":key": key
            },
        )?;

        Ok(modified != 0)
    }
}

mod meta;
pub use meta::*;

mod update;
pub use update::*;

mod insert;
pub use insert::*;

mod get;
pub use get::*;

mod delete;
pub use delete::*;

mod index;
pub use index::*;

mod scan;
pub use scan::*;
