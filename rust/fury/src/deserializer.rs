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

use super::buffer::Reader;
use super::types::Language;
use crate::{
    error::Error,
    types::{FuryMeta, RefFlag},
};
use chrono::{DateTime, Days, NaiveDate, NaiveDateTime, TimeZone, Utc};
use std::{
    collections::{HashMap, HashSet},
    mem,
};

fn from_u8_slice<T: Clone>(slice: &[u8]) -> Vec<T> {
    let byte_len = slice.len() / mem::size_of::<T>();
    unsafe { std::slice::from_raw_parts(slice.as_ptr().cast::<T>(), byte_len) }.to_vec()
}

pub trait Deserialize
where
    Self: Sized + FuryMeta,
{
    fn read(deserializer: &mut DeserializerState) -> Result<Self, Error>;

    fn read_vec(deserializer: &mut DeserializerState) -> Result<Vec<Self>, Error> {
        // length
        let len = deserializer.reader.var_int32();
        // value
        let mut result = Vec::new();
        for _ in 0..len {
            result.push(Self::deserialize(deserializer)?);
        }
        Ok(result)
    }

    fn deserialize(deserializer: &mut DeserializerState) -> Result<Self, Error> {
        // ref flag
        let ref_flag = deserializer.reader.i8();

        if ref_flag == (RefFlag::NotNullValue as i8) || ref_flag == (RefFlag::RefValue as i8) {
            // type_id
            let type_id = deserializer.reader.i16();
            let ty = if Self::is_vec() {
                Self::vec_ty()
            } else {
                Self::ty()
            };
            if type_id != ty as i16 {
                Err(Error::FieldType {
                    expected: ty,
                    actial: type_id,
                })
            } else {
                Ok(Self::read(deserializer)?)
            }
        } else if ref_flag == (RefFlag::Null as i8) {
            Err(Error::Null)
        } else if ref_flag == (RefFlag::Ref as i8) {
            Err(Error::Ref)
        } else {
            Err(Error::BadRefFlag)
        }
    }
}

macro_rules! impl_num_deserialize {
    ($name: ident, $ty:tt) => {
        impl Deserialize for $ty {
            fn read(deserializer: &mut DeserializerState) -> Result<Self, Error> {
                Ok(deserializer.reader.$name())
            }
        }
    };
}

macro_rules! impl_num_deserialize_and_pritimive_vec {
    ($name: ident, $ty:tt) => {
        impl Deserialize for $ty {
            fn read(deserializer: &mut DeserializerState) -> Result<Self, Error> {
                Ok(deserializer.reader.$name())
            }

            fn read_vec(deserializer: &mut DeserializerState) -> Result<Vec<Self>, Error> {
                // length
                let len = (deserializer.reader.var_int32() as usize) * mem::size_of::<$ty>();
                Ok(from_u8_slice::<$ty>(
                    deserializer.reader.bytes(len as usize),
                ))
            }
        }
    };
}

impl_num_deserialize!(u16, u16);
impl_num_deserialize!(u32, u32);
impl_num_deserialize!(u64, u64);
impl_num_deserialize!(i8, i8);

impl_num_deserialize_and_pritimive_vec!(u8, u8);
impl_num_deserialize_and_pritimive_vec!(i16, i16);
impl_num_deserialize_and_pritimive_vec!(i32, i32);
impl_num_deserialize_and_pritimive_vec!(i64, i64);
impl_num_deserialize_and_pritimive_vec!(f32, f32);
impl_num_deserialize_and_pritimive_vec!(f64, f64);

impl Deserialize for String {
    fn read(deserializer: &mut DeserializerState) -> Result<Self, Error> {
        let len = deserializer.reader.var_int32();
        Ok(deserializer.reader.string(len as usize))
    }
}

impl Deserialize for bool {
    fn read(deserializer: &mut DeserializerState) -> Result<Self, Error> {
        Ok(deserializer.reader.u8() == 1)
    }
}

impl<T1: Deserialize + Eq + std::hash::Hash, T2: Deserialize> Deserialize for HashMap<T1, T2> {
    fn read(deserializer: &mut DeserializerState) -> Result<Self, Error> {
        // length
        let len = deserializer.reader.var_int32();
        let mut result = HashMap::new();
        // key-value
        for _ in 0..len {
            result.insert(
                <T1 as Deserialize>::deserialize(deserializer)?,
                <T2 as Deserialize>::deserialize(deserializer)?,
            );
        }
        Ok(result)
    }
}

impl<T: Deserialize + Eq + std::hash::Hash> Deserialize for HashSet<T> {
    fn read(deserializer: &mut DeserializerState) -> Result<Self, Error> {
        // length
        let len = deserializer.reader.var_int32();
        let mut result = HashSet::new();
        // key-value
        for _ in 0..len {
            result.insert(<T as Deserialize>::deserialize(deserializer)?);
        }
        Ok(result)
    }
}

impl Deserialize for NaiveDateTime {
    fn read(deserializer: &mut DeserializerState) -> Result<Self, Error> {
        let timestamp = deserializer.reader.u64();
        let ret = DateTime::from_timestamp_millis(timestamp as i64).map(|dt| dt.naive_utc());
        match ret {
            Some(r) => Ok(r),
            None => Err(Error::NaiveDateTime),
        }
    }
}

impl<T: Deserialize> Deserialize for Vec<T> {
    fn read(deserializer: &mut DeserializerState) -> Result<Self, Error> {
        T::read_vec(deserializer)
    }
}

impl<T: Deserialize> Deserialize for Option<T> {
    fn read(deserializer: &mut DeserializerState) -> Result<Self, Error> {
        Ok(Some(T::read(deserializer)?))
    }

    fn deserialize(deserializer: &mut DeserializerState) -> Result<Self, Error> {
        // ref flag
        let ref_flag = deserializer.reader.i8();

        if ref_flag == (RefFlag::NotNullValue as i8) || ref_flag == (RefFlag::RefValue as i8) {
            // type_id
            let type_id = deserializer.reader.i16();

            if type_id != <Self as FuryMeta>::ty() as i16 {
                Err(Error::FieldType {
                    expected: <Self as FuryMeta>::ty(),
                    actial: type_id,
                })
            } else {
                Ok(Self::read(deserializer)?)
            }
        } else if ref_flag == (RefFlag::Null as i8) {
            Ok(None)
        } else if ref_flag == (RefFlag::Ref as i8) {
            Err(Error::Ref)
        } else {
            Err(Error::BadRefFlag)
        }
    }
}

lazy_static::lazy_static!(
    static ref EPOCH: DateTime<Utc> = Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap();
);

impl Deserialize for NaiveDate {
    fn read(serializer: &mut DeserializerState) -> Result<Self, Error> {
        let days = serializer.reader.u64();
        match EPOCH.checked_add_days(Days::new(days)) {
            Some(value) => Ok(value.date_naive()),
            None => Err(Error::NaiveDate),
        }
    }
}
pub struct DeserializerState<'de, 'bf: 'de> {
    pub reader: Reader<'bf>,
    pub tags: Vec<&'de str>,
}

impl<'de, 'bf: 'de> DeserializerState<'de, 'bf> {
    fn new(reader: Reader<'bf>) -> DeserializerState<'de, 'bf> {
        DeserializerState {
            reader,
            tags: Vec::new(),
        }
    }

    fn head(&mut self) -> Result<(), Error> {
        let _bitmap = self.reader.u8();
        let _language: Language = self.reader.u8().try_into()?;
        self.reader.skip(8); // native offset and size
        Ok(())
    }

    pub fn read_tag(&mut self) -> Result<&str, Error> {
        const USESTRINGVALUE: u8 = 0;
        const USESTRINGID: u8 = 1;
        let tag_type = self.reader.u8();
        if tag_type == USESTRINGID {
            Ok(self.tags[self.reader.i16() as usize])
        } else if tag_type == USESTRINGVALUE {
            self.reader.skip(8); // todo tag hash
            let len = self.reader.i16();
            let tag: &str =
                unsafe { std::str::from_utf8_unchecked(self.reader.bytes(len as usize)) };
            self.tags.push(tag);
            Ok(tag)
        } else {
            Err(Error::TagType(tag_type))
        }
    }
}

pub fn from_buffer<T: Deserialize>(bf: &[u8]) -> Result<T, Error> {
    let reader = Reader::new(bf);
    let mut deserializer = DeserializerState::new(reader);
    deserializer.head()?;
    <T as Deserialize>::deserialize(&mut deserializer)
}
