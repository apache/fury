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

use super::types::Language;

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

    #[error("Bad FieldType; expected: {expected:?}, actual: {actual:?}")]
    FieldType { expected: i16, actual: i16 },

    #[error("Bad timestamp; out-of-range number of milliseconds")]
    NaiveDateTime,

    #[error("Bad date; out-of-range")]
    NaiveDate,

    #[error("Schema is not consistent; expected: {expected:?}, actual: {actual:?}")]
    StructHash { expected: u32, actual: u32 },

    #[error("Bad Tag Type: {0}")]
    TagType(u8),

    #[error("Only Xlang supported; receive: {language:?}")]
    UnsupportedLanguage { language: Language },

    #[error("Unsupported Language Code; receive: {code:?}")]
    UnsupportedLanguageCode { code: u8 },

    #[error("Unsupported encoding of field name in type meta; receive: {code:?}")]
    UnsupportedTypeMetaFieldNameEncoding { code: u8 },

    #[error("encoded_data cannot be empty")]
    EncodedDataEmpty,

    #[error("Long meta string than 32767 is not allowed")]
    LengthExceed,

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Works like anyhow's [ensure](https://docs.rs/anyhow/latest/anyhow/macro.ensure.html)
/// But return `Return<T, ErrorFromAnyhow>`
#[macro_export]
macro_rules! ensure {
    ($cond:expr, $msg:literal) => {
        if !$cond {
            return Err(anyhow::anyhow!($msg).into());
        }
    };
    ($cond:expr, $err:expr) => {
        if !$cond {
            return Err($err.into());
        }
    };
    ($cond:expr, $fmt:expr, $($arg:tt)*) => {
        if !$cond {
            return Err(anyhow::anyhow!($fmt, $($arg)*).into());
        }
    };
}
