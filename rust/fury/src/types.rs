// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{
    collections::{HashMap, HashSet},
    mem,
};

use chrono::{NaiveDate, NaiveDateTime};

use crate::Error;

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

    fn is_vec() -> bool {
        false
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

    fn is_vec() -> bool {
        true
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
    Null = -3,
    // Ref indicates that object is a not-null value.
    // We don't use another byte to indicate REF, so that we can save one byte.
    Ref = -2,
    // NotNullValueFlag indicates that the object is a non-null value.
    NotNullValue = -1,
    // RefValueFlag indicates that the object is a referencable and first read.
    RefValue = 0,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum FieldType {
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
    STRING = 13,
    BINARY = 14,
    DATE = 16,
    TIMESTAMP = 18,
    ARRAY = 25,
    MAP = 30,
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

// todo: struct hash
#[allow(dead_code)]
pub fn compute_string_hash(s: &str) -> u32 {
    let mut hash: u64 = 17;
    s.as_bytes().iter().for_each(|b| {
        hash = (hash * 31) + (*b as u64);
        while hash >= MAX_UNT32 {
            hash /= 7;
        }
    });
    hash as u32
}

const BASIC_TYPES: [FieldType; 11] = [
    FieldType::BOOL,
    FieldType::INT8,
    FieldType::INT16,
    FieldType::INT32,
    FieldType::INT64,
    FieldType::FLOAT,
    FieldType::DOUBLE,
    FieldType::STRING,
    FieldType::BINARY,
    FieldType::DATE,
    FieldType::TIMESTAMP,
];

pub fn compute_field_hash(hash: u32, id: i16) -> u32 {
    let mut new_hash: u64 = (hash as u64) * 31 + (id as u64);
    while new_hash >= MAX_UNT32 {
        new_hash /= 7;
    }
    new_hash as u32
}

pub fn compute_struct_hash(props: Vec<(&str, FieldType, &str)>) -> u32 {
    let mut hash = 17;
    props.iter().for_each(|prop| {
        let (_name, ty, _tag) = prop;
        hash = match ty {
            FieldType::ARRAY | FieldType::MAP => compute_field_hash(hash, *ty as i16),
            _ => hash,
        };
        let is_basic_type = BASIC_TYPES.iter().any(|x| *x == *ty);
        if is_basic_type {
            hash = compute_field_hash(hash, *ty as i16);
        }
    });
    hash
}

// todo: flag check
#[allow(dead_code)]
pub mod config_flags {
    pub const IS_NULL_FLAG: u8 = 1 << 0;
    pub const IS_LITTLE_ENDIAN_FLAG: u8 = 2;
    pub const IS_CROSS_LANGUAGE_FLAG: u8 = 4;
    pub const IS_OUT_OF_BAND_FLAG: u8 = 8;
}

#[derive(Debug, PartialEq)]
pub enum Language {
    Xlang = 0,
    Java = 1,
    Python = 2,
    Cpp = 3,
    Go = 4,
    Javascript = 5,
    Rust = 6,
}

impl TryFrom<u8> for Language {
    type Error = Error;

    fn try_from(num: u8) -> Result<Self, Error> {
        match num {
            0 => Ok(Language::Xlang),
            1 => Ok(Language::Java),
            2 => Ok(Language::Python),
            3 => Ok(Language::Cpp),
            4 => Ok(Language::Go),
            5 => Ok(Language::Javascript),
            6 => Ok(Language::Rust),
            _ => Err(Error::UnsupportLanguageCode { code: num }),
        }
    }
}

// every object start with i8 i16 reference flag and type flag
pub const SIZE_OF_REF_AND_TYPE: usize = mem::size_of::<i8>() + mem::size_of::<i16>();
