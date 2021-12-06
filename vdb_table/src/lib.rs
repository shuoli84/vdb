/// turns NoRows error to option none
macro_rules! no_row_to_none {
    ($exp: expr) => {
        match $exp {
            Ok(x) => Ok(Some(x)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e),
        }
    };
}

mod table;
pub use table::*;
pub mod index;

mod table_typed;
pub use table_typed::*;

mod error;
pub use error::*;

#[cfg(test)]
mod tests;
