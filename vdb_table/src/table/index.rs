use crate::index::{Index, ScanKey, ScanOptions, ScanOrder};
use crate::{Error, Table};
use rusqlite::Connection;

impl Table {
    /// Get all index key and relative primary key pairs
    pub fn get_by_index(
        &self,
        conn: &Connection,
        index_name: &str,
        key: &[u8],
        count: u32,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Error> {
        let index = match self.get_index_by_name(index_name) {
            None => return Err(Error::IndexMissing(index_name.to_string())),
            Some(index) => index,
        };
        let scan_result = index.scan(
            conn,
            ScanOptions {
                lower_key: Some(ScanKey {
                    ik: key,
                    pk: b"",
                    inclusive: true,
                }),
                higher_key: None,
                count,
                order: ScanOrder::Asc,
            },
        )?;
        Ok(scan_result.keys)
    }

    fn get_index_by_name(&self, name: &str) -> Option<&Index> {
        for index in self.indexes.iter() {
            if index.name.eq(name) {
                return Some(index);
            }
        }
        None
    }
}
