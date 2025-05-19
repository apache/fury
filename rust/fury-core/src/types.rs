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

use crate::error::Error;
use anyhow::anyhow;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use std::mem;

#[allow(dead_code)]
pub enum StringFlag {
    LATIN1 = 0,
    UTF8 = 1,
}

#[derive(TryFromPrimitive)]
#[repr(i8)]
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

#[derive(Clone, Copy, Debug, PartialEq, TryFromPrimitive, IntoPrimitive)]
#[repr(i16)]
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

pub trait FuryGeneralList {}

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

pub fn compute_struct_hash(props: Vec<(&str, FieldType)>) -> u32 {
    let mut hash = 17;
    props.iter().for_each(|prop| {
        let (_name, ty) = prop;
        hash = match ty {
            FieldType::ARRAY | FieldType::MAP => compute_field_hash(hash, *ty as i16),
            _ => hash,
        };
        let is_basic_type = BASIC_TYPES.contains(ty);
        if is_basic_type {
            hash = compute_field_hash(hash, *ty as i16);
        }
    });
    hash
}

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
    Dart = 7,
}

#[derive(PartialEq)]
pub enum Mode {
    // Type declaration must be consistent between serialization peer and deserialization peer.
    SchemaConsistent,
    // Type declaration can be different between serialization peer and deserialization peer.
    // They can add/delete fields independently.
    Compatible,
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
            _ => Err(anyhow!("Unsupported language code, value:{num}"))?,
        }
    }
}

// every object start with i8 i16 reference flag and type flag
pub const SIZE_OF_REF_AND_TYPE: usize = mem::size_of::<i8>() + mem::size_of::<i16>();
