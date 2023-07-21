use std::collections::{HashMap, HashSet};

use chrono::{NaiveDate, NaiveDateTime};

pub trait FuryMeta {
    fn ty() -> FieldType;

    fn vec_ty() -> FieldType {
        FieldType::ARRAY
    }

    fn hash() -> u32 {
        0
    }

    fn tag() -> &'static str {
        ""
    }
}

macro_rules! impl_number_meta {
    ($expr: expr, $tt: tt) => {
        impl FuryMeta for $tt {
            fn ty() -> FieldType {
                $expr
            }
        }
    };
}

macro_rules! impl_primitive_array_meta {
    ($ty: expr, $vec_ty: expr, $tt: tt) => {
        impl FuryMeta for $tt {
            fn ty() -> FieldType {
                $ty
            }

            fn vec_ty() -> FieldType {
                $vec_ty
            }
        }
    };
}

impl<T1, T2> FuryMeta for HashMap<T1, T2> {
    fn ty() -> FieldType {
        FieldType::MAP
    }
}

impl<T> FuryMeta for HashSet<T> {
    fn ty() -> FieldType {
        FieldType::FurySet
    }
}

impl FuryMeta for u8 {
    fn ty() -> FieldType {
        FieldType::UINT8
    }
    fn vec_ty() -> FieldType {
        FieldType::BINARY
    }
}

impl FuryMeta for NaiveDateTime {
    fn ty() -> FieldType {
        FieldType::TIMESTAMP
    }
}

impl FuryMeta for NaiveDate {
    fn ty() -> FieldType {
        FieldType::DATE
    }
}

impl_number_meta!(FieldType::UINT16, u16);
impl_number_meta!(FieldType::UINT32, u32);
impl_number_meta!(FieldType::UINT64, u64);
impl_number_meta!(FieldType::INT8, i8);

// special type array
impl_primitive_array_meta!(FieldType::BOOL, FieldType::FuryPrimitiveBoolArray, bool);
impl_primitive_array_meta!(FieldType::INT16, FieldType::FuryPrimitiveShortArray, i16);
impl_primitive_array_meta!(FieldType::INT32, FieldType::FuryPrimitiveIntArray, i32);
impl_primitive_array_meta!(FieldType::INT64, FieldType::FuryPrimitiveLongArray, i64);
impl_primitive_array_meta!(FieldType::FLOAT, FieldType::FuryPrimitiveFloatArray, f32);
impl_primitive_array_meta!(FieldType::DOUBLE, FieldType::FuryPrimitiveDoubleArray, f64);
impl_primitive_array_meta!(FieldType::STRING, FieldType::FuryStringArray, String);

impl<T: FuryMeta> FuryMeta for Vec<T> {
    fn ty() -> FieldType {
        FieldType::ARRAY
    }
}

impl<T: FuryMeta> FuryMeta for Option<T> {
    fn vec_ty() -> FieldType {
        T::vec_ty()
    }

    fn ty() -> FieldType {
        T::ty()
    }
}

#[allow(dead_code)]
pub enum StringFlag {
    LATIN1 = 0,
    UTF8 = 1,
}

pub enum RefFlag {
    NullFlag = -3,
    // RefFlag indicates that object is a not-null value.
    // We don't use another byte to indicate REF, so that we can save one byte.
    RefFlag = -2,
    // NotNullValueFlag indicates that the object is a non-null value.
    NotNullValueFlag = -1,
    // RefValueFlag indicates that the object is a referencable and first read.
    RefValueFlag = 0,
}

#[derive(Clone, Copy, Debug)]
pub enum FieldType {
    STRING = 13,
    ARRAY = 25,
    MAP = 30,
    BOOL = 1,
    UINT8 = 2,
    INT8 = 3,
    UINT16 = 4,
    INT16 = 5,
    UINT32 = 6,
    INT32 = 7,
    UINT64 = 8,
    INT64 = 9,
    FLOAT = 11,
    DOUBLE = 12,
    BINARY = 14,
    DATE = 16,
    TIMESTAMP = 18,
    FuryTypeTag = 256,
    FurySet = 257,
    FuryPrimitiveBoolArray = 258,
    FuryPrimitiveShortArray = 259,
    FuryPrimitiveIntArray = 260,
    FuryPrimitiveLongArray = 261,
    FuryPrimitiveFloatArray = 262,
    FuryPrimitiveDoubleArray = 263,
    FuryStringArray = 264,
}

const MAX_UNT32: u64 = (1 << 31) - 1;

pub fn compute_string_hash(s: &str) -> u32 {
    let mut hash: u64 = 17;
    s.as_bytes()
        .iter()
        .for_each(|b| hash = (hash * 31) + (*b as u64));
    hash /= 7;
    while hash >= MAX_UNT32 {
        hash /= 7;
    }
    hash as u32
}

pub fn compute_field_hash(hash: u32, prop: &(FieldType, &str)) -> u32 {
    let id = match prop {
        (FieldType::FuryTypeTag, tag) => compute_string_hash(tag),
        (t, _) => *t as u32,
    };
    let mut new_hash: u64 = (hash as u64) * 31 + (id as u64);
    new_hash /= 7;
    while new_hash >= MAX_UNT32 {
        new_hash /= 7;
    }
    new_hash as u32
}

pub fn compute_tag_hash(props: Vec<(FieldType, &str)>) -> u32 {
    let mut hash = 17;
    props
        .iter()
        .for_each(|prop| hash = compute_field_hash(hash, prop));
    hash
}
