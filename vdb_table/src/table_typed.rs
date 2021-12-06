use crate::{Error, Table};
use std::marker::PhantomData;
use vdb_key::Key;

/// Typed table, wraps underlying table with type, provide strong typed interface
/// instead of deal with bytes, now user can deal with type directly

/// TableItem
pub trait TableItem: vdb_value::Value {
    type PrimaryKey: Into<vdb_key::Key> + TryFrom<vdb_key::Key, Error = vdb_key::Error>;

    fn primary_key(&self) -> Self::PrimaryKey;
}

pub struct TypedTable<Item: TableItem> {
    table: Table,
    _ph: PhantomData<Item>,
}

impl<Item: TableItem> TypedTable<Item> {
    pub fn new(name: &str) -> Self {
        Self {
            table: Table::new(name.to_string()),
            _ph: Default::default(),
        }
    }

    /// create typed table with existing table.
    pub fn new_with_table(table: Table) -> Self {
        Self {
            table,
            _ph: Default::default(),
        }
    }

    pub fn create_table(&mut self, conn: &rusqlite::Connection) -> Result<(), Error> {
        self.table.create_table(conn)
    }

    /// append a code defined index, which accepts pk and item,
    /// and convert to IK
    pub fn append_index<F, IK>(&mut self, name: &str, f: F)
    where
        F: Fn(&Item::PrimaryKey, &Item) -> Vec<IK> + Send + Sync + 'static,
        IK: Into<Key>,
    {
        self.table.append_index(
            name,
            Box::new(move |pk, item| {
                let item = Item::from_slice(item)?;
                let pk = Item::PrimaryKey::try_from(Key::load_from_bytes_unchecked(pk.to_vec()))?;

                let index_keys = f(&pk, &item);

                Ok(index_keys
                    .into_iter()
                    .map(|ik| ik.into().into_bytes())
                    .collect::<Vec<_>>())
            }),
        )
    }

    pub fn insert(&self, conn: &mut rusqlite::Connection, item: &Item) -> Result<i64, Error> {
        self.table
            .insert(conn, item.primary_key().into().into_bytes(), item.to_vec())
    }

    pub fn delete(
        &self,
        conn: &mut rusqlite::Connection,
        pk: Item::PrimaryKey,
    ) -> Result<i64, Error> {
        self.table.delete(conn, pk.into().into_bytes().as_slice())
    }

    pub fn get(
        &self,
        conn: &rusqlite::Connection,
        pk: Item::PrimaryKey,
    ) -> Result<Option<(Item, i64)>, Error> {
        let result = self.table.get(conn, pk.into().into_bytes().as_slice())?;

        match result {
            None => Ok(None),
            Some((bytes, v)) => {
                let item = Item::from_slice(bytes.as_slice())?;
                Ok(Some((item, v)))
            }
        }
    }

    pub fn batch_insert(
        &self,
        conn: &mut rusqlite::Connection,
        items: &[Item],
    ) -> Result<i64, Error> {
        let mut version = 0;
        for item in items {
            version =
                self.table
                    .insert(conn, item.primary_key().into().into_bytes(), item.to_vec())?;
        }

        Ok(version)
    }
}
