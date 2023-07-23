use crate::FieldType;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Field is not Option type, can't be deserialize of None")]
    Null,

    #[error("Fury on Rust not support Ref type")]
    Ref,

    #[error("Fury on Rust not support RefValue type")]
    RefValue,

    #[error("BadRefFlag")]
    BadRefFlag,

    #[error("Bad FieldType; expected: {expected:?}, actual: {actial:?}")]
    FieldType { expected: FieldType, actial: i16 },

    #[error("Bad timestamp; out-of-range number of milliseconds")]
    NaiveDateTime,

    #[error("Bad date; out-of-range")]
    NaiveDate,

    #[error("Schema is not consistent; expected: {expected:?}, actual: {actial:?}")]
    StructHash { expected: u32, actial: u32 },

    #[error("Bad Tag Type: {0}")]
    TagType(u8),
}
