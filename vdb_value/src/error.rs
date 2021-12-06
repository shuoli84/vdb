#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("[vdb] Decode error, premature end")]
    DecodePrematureEnd,

    #[error("[vdb] Decode error, invalid type")]
    DecodeInvalidType,

    #[error("[vdb] String encoding error")]
    String,
}
