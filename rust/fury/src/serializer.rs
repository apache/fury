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

//! Implement the serializer of internal types in fury protocol
//!
//! Serializeable type should implements the Serialize trait.
//! This file include all the internal type implements which describe in fury protocol,
//! But custom type which serializable is not here, custom type implement the Serialize trait via fury-derive.
//! # Examples:
//! ```
//!
//! use fury::Fury;
//! #[derive(Fury)]
//! #[tag("example.foo2")]
//! struct Animal {
//! category: String,
//! }
//!
//! ```
//! fury::Fury would expand the code and automatic implements the Serialize trait

use super::buffer::Writer;
use super::types::{config_flags, FuryMeta, Language, RefFlag, SIZE_OF_REF_AND_TYPE};
use chrono::{NaiveDate, NaiveDateTime};
use std::collections::{HashMap, HashSet};
use std::mem;

/// Convert a typed slice to a u8 slice.
/// Usually used to convert a typed array like Vec to &[u8], which can be easily written to a buffer.
fn to_u8_slice<T>(slice: &[T]) -> &[u8] {
    let byte_len = std::mem::size_of_val(slice);
    unsafe { std::slice::from_raw_parts(slice.as_ptr().cast::<u8>(), byte_len) }
}

/// Types that implement the Serialize trait can be serialized to Fury.
///
/// 1. Normal situation:
/// The order of function calls is reserved_space -> serialize -> write.
///     a. reserved_space is used to allocate the fixed memory space, which can avoid the cost of the memory check.
///         However, dynamic types like strings should allocate the size separately before being written to the buffer.
///     b. serialize is used to serialize the data into the buffer. The first step is to write the object head,
///         which includes one byte reference flag and two byte type flag.
///         The second step is to call the write function, which is used to write the Rust object.
///     c. write is used to write the Rust object into the buffer.
/// 2. Vec situation:
/// If the object is in a Vec, the call order is reserved_space -> serialize -> write -> write_vec.
/// The write_vec function is used to write the elements of the Vec. But why can't we just loop through the elements and write each element one by one?
/// This is because Fury includes some primitive types like FuryPrimitiveBoolArray which do not include the head of the elements,
/// but other Vecs do. So the write_vec function is necessary to handle the differences. Primitive arrays can overwrite the function.
pub trait Serialize
where
    Self: Sized + FuryMeta,
{
    /// This function is invoked when the data is in a Vec.
    ///
    /// The default implementation invokes the serialize function, which includes the type head. Some types like primitive arrays can overwrite it.
    /// Step 1: write the length of the Vec into the buffer.
    /// Step 2: reserve the fixed size of all the elements.
    /// Step 3: loop through the Vec and invoke the serialize function of each item.
    fn write_vec(value: &[Self], serializer: &mut SerializerState) {
        serializer.writer.var_int32(value.len() as i32);
        serializer
            .writer
            .reserve((<Self as Serialize>::reserved_space() + SIZE_OF_REF_AND_TYPE) * value.len());
        for item in value.iter() {
            item.serialize(serializer);
        }
    }

    /// The fixed memory size of the Type.
    /// Avoid the memory check, which would hurt performance.
    fn reserved_space() -> usize;

    /// Write the data into the buffer.
    fn write(&self, serializer: &mut SerializerState);

    /// Entry point of the serialization.
    ///
    /// Step 1: write the type flag and type flag into the buffer.
    /// Step 2: invoke the write function to write the Rust object.
    fn serialize(&self, serializer: &mut SerializerState) {
        // ref flag
        serializer.writer.i8(RefFlag::NotNullValue as i8);
        // type
        serializer.writer.i16(if Self::is_vec() {
            Self::vec_ty()
        } else {
            Self::ty()
        } as i16);
        self.write(serializer);
    }
}

/// Implement Serialize for all the number types.
///
/// Serializing number types is similar, the only difference is the fixed size.
macro_rules! impl_num_serialize {
    ($name: ident, $ty:tt) => {
        impl Serialize for $ty {
            fn write(&self, serializer: &mut SerializerState) {
                serializer.writer.$name(*self);
            }

            fn reserved_space() -> usize {
                mem::size_of::<$ty>()
            }
        }
    };
}

/// Implement Serialize for all the number types, but overwrite the primitive type Vec, which does not include the object head.
macro_rules! impl_num_serialize_and_pritimive_vec {
    ($name: ident, $ty:tt) => {
        impl Serialize for $ty {
            fn write(&self, serializer: &mut SerializerState) {
                serializer.writer.$name(*self);
            }

            fn write_vec(value: &[Self], serializer: &mut SerializerState) {
                serializer.writer.var_int32(value.len() as i32);
                serializer.writer.bytes(to_u8_slice(value));
            }

            fn reserved_space() -> usize {
                mem::size_of::<$ty>()
            }
        }
    };
}

impl_num_serialize!(u16, u16);
impl_num_serialize!(u32, u32);
impl_num_serialize!(u64, u64);
impl_num_serialize!(i8, i8);

impl_num_serialize_and_pritimive_vec!(u8, u8);
impl_num_serialize_and_pritimive_vec!(i16, i16);
impl_num_serialize_and_pritimive_vec!(i32, i32);
impl_num_serialize_and_pritimive_vec!(i64, i64);
impl_num_serialize_and_pritimive_vec!(f32, f32);
impl_num_serialize_and_pritimive_vec!(f64, f64);

/// The implement of String Type
impl Serialize for String {
    fn write(&self, serializer: &mut SerializerState) {
        serializer.writer.var_int32(self.len() as i32);
        serializer.writer.bytes(self.as_bytes());
    }

    fn write_vec(value: &[Self], serializer: &mut SerializerState) {
        serializer.writer.var_int32(value.len() as i32);
        serializer
            .writer
            .reserve((<Self as Serialize>::reserved_space()) * value.len());

        for x in value.iter() {
            x.write(serializer);
        }
    }

    fn reserved_space() -> usize {
        mem::size_of::<i32>()
    }
}

impl Serialize for bool {
    fn write(&self, serializer: &mut SerializerState) {
        serializer.writer.u8(if *self { 1 } else { 0 });
    }

    fn write_vec(value: &[Self], serializer: &mut SerializerState) {
        serializer.writer.var_int32(value.len() as i32);
        serializer.writer.bytes(to_u8_slice(value));
    }

    fn reserved_space() -> usize {
        mem::size_of::<u8>()
    }
}

impl<T1: Serialize, T2: Serialize> Serialize for HashMap<T1, T2> {
    fn write(&self, serializer: &mut SerializerState) {
        // length
        serializer.writer.var_int32(self.len() as i32);

        let reserved_space = (<T1 as Serialize>::reserved_space() + SIZE_OF_REF_AND_TYPE)
            * self.len()
            + (<T2 as Serialize>::reserved_space() + SIZE_OF_REF_AND_TYPE) * self.len();
        serializer.writer.reserve(reserved_space);

        // key-value
        for i in self.iter() {
            i.0.serialize(serializer);
            i.1.serialize(serializer);
        }
    }

    fn reserved_space() -> usize {
        mem::size_of::<i32>()
    }
}

impl<T: Serialize> Serialize for HashSet<T> {
    fn write(&self, serializer: &mut SerializerState) {
        // length
        serializer.writer.i32(self.len() as i32);

        let reserved_space =
            (<T as Serialize>::reserved_space() + SIZE_OF_REF_AND_TYPE) * self.len();
        serializer.writer.reserve(reserved_space);

        // key-value
        for i in self.iter() {
            i.serialize(serializer);
        }
    }

    fn reserved_space() -> usize {
        mem::size_of::<i32>()
    }
}

impl Serialize for NaiveDateTime {
    fn write(&self, serializer: &mut SerializerState) {
        serializer
            .writer
            .u64(self.and_utc().timestamp_millis() as u64);
    }

    fn reserved_space() -> usize {
        mem::size_of::<u64>()
    }
}

lazy_static::lazy_static!(
    static ref EPOCH: NaiveDate = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
);

impl Serialize for NaiveDate {
    fn write(&self, serializer: &mut SerializerState) {
        let days_since_epoch = self.signed_duration_since(*EPOCH).num_days();
        serializer.writer.u64(days_since_epoch as u64);
    }

    fn reserved_space() -> usize {
        mem::size_of::<u64>()
    }
}

impl<T> Serialize for Vec<T>
where
    T: Serialize,
{
    fn write(&self, serializer: &mut SerializerState) {
        T::write_vec(self, serializer);
    }

    fn reserved_space() -> usize {
        // size of the vec
        mem::size_of::<u32>()
    }
}

impl<T> Serialize for Option<T>
where
    T: Serialize,
{
    fn write(&self, serializer: &mut SerializerState) {
        if let Some(v) = self {
            T::write(v, serializer)
        } else {
            unreachable!("write should be call by serialize")
        }
    }

    fn serialize(&self, serializer: &mut SerializerState) {
        match self {
            Some(v) => {
                // ref flag
                serializer.writer.i8(RefFlag::NotNullValue as i8);
                // type
                serializer.writer.i16(<Self as FuryMeta>::ty() as i16);

                v.write(serializer);
            }
            None => {
                serializer.writer.i8(RefFlag::Null as i8);
            }
        }
    }

    fn reserved_space() -> usize {
        mem::size_of::<T>()
    }
}

pub struct SerializerState<'se> {
    pub writer: &'se mut Writer,
    pub tags: Vec<&'static str>,
}

impl<'de> SerializerState<'de> {
    fn new(writer: &mut Writer) -> SerializerState {
        SerializerState {
            writer,
            tags: Vec::new(),
        }
    }

    pub fn write_tag(&mut self, tag: &'static str) {
        const USESTRINGVALUE: u8 = 0;
        const USESTRINGID: u8 = 1;

        let mayby_idx = self.tags.iter().position(|x| *x == tag);
        match mayby_idx {
            Some(idx) => {
                self.writer.u8(USESTRINGID);
                self.writer.i16(idx as i16);
            }
            None => {
                self.writer.u8(USESTRINGVALUE);
                self.writer.skip(8); // todo tag hash
                self.writer.i16(tag.len() as i16);
                self.writer.bytes(tag.as_bytes());
            }
        };
    }

    fn head<T: Serialize>(&mut self) -> &Self {
        const HEAD_SIZE: usize = 10;
        self.writer
            .reserve(<T as Serialize>::reserved_space() + SIZE_OF_REF_AND_TYPE + HEAD_SIZE);

        let mut bitmap = 0;
        bitmap |= config_flags::IS_LITTLE_ENDIAN_FLAG;
        bitmap |= config_flags::IS_CROSS_LANGUAGE_FLAG;
        self.writer.u8(bitmap);
        self.writer.u8(Language::Rust as u8);
        self.writer.skip(4); // native offset
        self.writer.skip(4); // native size
        self
    }
}

pub fn to_buffer<T: Serialize>(record: &T) -> Vec<u8> {
    let mut writer = Writer::default();
    let mut serializer = SerializerState::new(&mut writer);
    serializer.head::<T>();
    <T as Serialize>::serialize(record, &mut serializer);
    writer.dump()
}
