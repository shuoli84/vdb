use crate::{Error, Table};

/// UpdateResult, returned from update_f, to indicate update result
pub enum UpdateResult {
    /// Nothing need change, just return
    NotChange,
    /// A new value generated, update the db
    Update(Vec<u8>),
    /// The record should be deleted
    Delete,
}

impl Table {
    /// Update for key
    pub fn update<'a>(
        &'a self,
        conn: &'a mut rusqlite::Connection,
        key: Vec<u8>,
        mut update_f: Box<dyn FnMut(Option<(Vec<u8>, i64)>) -> Result<UpdateResult, Error> + 'a>,
    ) -> Result<Option<i64>, Error> {
        let prev = self.get(conn, &key)?;

        let prev_v = prev.as_ref().map(|x| x.1).unwrap_or_default();

        let new_v = match update_f(prev)? {
            UpdateResult::NotChange => {
                return Ok(None);
            }
            UpdateResult::Delete => self.delete_with_version(conn, key, prev_v)?,
            UpdateResult::Update(new) => self.insert(conn, key, new)?,
        };

        Ok(Some(new_v))
    }
}
