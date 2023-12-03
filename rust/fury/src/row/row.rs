// Copyright 2023 The Fury Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::marker::PhantomData;

use crate::Error;
use arrow::datatypes::ToByteSlice;
use byteorder::{ByteOrder, LittleEndian};
use chrono::{Days, NaiveDate, NaiveDateTime};

use super::{
    reader::{ArrayData, RowData, StructData},
    writer::RowWriter,
};

#[derive(Clone, Copy)]
pub struct Schema {
    num_fields: usize,
    is_container: bool,
}

impl Schema {
    pub fn num_fields(&self) -> usize {
        self.num_fields
    }

    pub fn is_container(&self) -> bool {
        self.is_container
    }

    pub fn new(num_fields: usize, is_container: bool) -> Schema {
        Schema {
            num_fields,
            is_container,
        }
    }
}

pub trait Row<'a> {
    type ReadResult;

    fn write(v: &Self, row_writer: &mut RowWriter) -> usize;

    fn cast(bytes: &'a [u8]) -> Self::ReadResult;

    fn schema() -> Schema {
        Schema {
            num_fields: 0,
            is_container: false,
        }
    }
}

fn read_i8_from_bytes(bytes: &[u8]) -> i8 {
    bytes[0] as i8
}

macro_rules! impl_row_for_number {
    ($tt: tt, $visitor: expr) => {
        impl<'a> Row<'a> for $tt {
            type ReadResult = Self;

            fn write(v: &Self, row_writer: &mut RowWriter) -> usize {
                row_writer.write(&v.to_le_bytes())
            }

            fn cast(bytes: &[u8]) -> Self::ReadResult {
                $visitor(bytes)
            }
        }
    };
}
impl_row_for_number!(i8, read_i8_from_bytes);
impl_row_for_number!(i16, LittleEndian::read_i16);
impl_row_for_number!(i32, LittleEndian::read_i32);
impl_row_for_number!(i64, LittleEndian::read_i64);
impl_row_for_number!(f32, LittleEndian::read_f32);
impl_row_for_number!(f64, LittleEndian::read_f64);

impl<'a> Row<'a> for String {
    type ReadResult = &'a str;

    fn write(v: &Self, row_writer: &mut RowWriter) -> usize {
        row_writer.write(v.as_bytes())
    }

    fn cast(bytes: &'a [u8]) -> Self::ReadResult {
        unsafe { std::str::from_utf8_unchecked(bytes) }
    }
}

impl<'a> Row<'a> for bool {
    type ReadResult = Self;

    fn write(v: &Self, row_writer: &mut RowWriter) -> usize {
        row_writer.write(&if *v { [1] } else { [0] })
    }

    fn cast(bytes: &[u8]) -> Self::ReadResult {
        bytes[0] == 1
    }
}

lazy_static::lazy_static!(
    static ref EPOCH: NaiveDate = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
);

impl<'a> Row<'a> for NaiveDate {
    type ReadResult = Result<NaiveDate, Error>;

    fn write(v: &Self, row_writer: &mut RowWriter) -> usize {
        let days_since_epoch = v.signed_duration_since(*EPOCH).num_days();
        row_writer.write(&(days_since_epoch as u32).to_le_bytes())
    }

    fn cast(bytes: &[u8]) -> Self::ReadResult {
        let days = LittleEndian::read_u32(bytes);
        match EPOCH.checked_add_days(Days::new(days.into())) {
            Some(value) => Ok(value),
            None => Err(Error::NaiveDate),
        }
    }
}

impl<'a> Row<'a> for NaiveDateTime {
    type ReadResult = Result<NaiveDateTime, Error>;

    fn write(v: &Self, row_writer: &mut RowWriter) -> usize {
        row_writer.write(&v.timestamp_millis().to_le_bytes())
    }

    fn cast(bytes: &[u8]) -> Self::ReadResult {
        let timestamp = LittleEndian::read_u64(bytes);
        let ret = NaiveDateTime::from_timestamp_millis(timestamp as i64);
        match ret {
            Some(r) => Ok(r),
            None => Err(Error::NaiveDateTime),
        }
    }
}

impl<'a> Row<'a> for Vec<u8> {
    type ReadResult = &'a [u8];

    fn write(v: &Self, row_writer: &mut RowWriter) -> usize {
        row_writer.write(v.to_byte_slice())
    }

    fn cast(bytes: &'a [u8]) -> Self::ReadResult {
        bytes
    }
}

pub struct ArrayGetter<'a, T> {
    array_data: ArrayData<'a>,
    _marker: PhantomData<T>,
}

impl<'a, T: Row<'a>> ArrayGetter<'a, T> {
    pub fn size(&self) -> usize {
        self.array_data.num_elements()
    }

    pub fn get(&self, idx: usize) -> T::ReadResult {
        if idx >= self.array_data.num_elements() {
            panic!("out of bound");
        }
        let bytes = self.array_data.get_field_bytes(idx);
        <T as Row>::cast(bytes)
    }
}

impl<'a, T: Row<'a>> Row<'a> for Vec<T> {
    type ReadResult = ArrayGetter<'a, T>;

    fn write(v: &Self, row_writer: &mut RowWriter) -> usize {
        let start = row_writer.writer.len();

        let schema = <T as Row>::schema();
        if schema.is_container() {
            v.iter().enumerate().for_each(|(index, item)| {
                let mut callback = row_writer.write_offset_size_callback(index);
                row_writer.point_to(schema.num_fields());
                <T as Row>::write(item, row_writer);
                callback(row_writer);
            });
        } else {
            v.iter().enumerate().for_each(|(index, item)| {
                let size = <T as Row>::write(item, row_writer);
                row_writer.write_offset_size(index, size);
            });
        }
        let end = row_writer.writer.len();
        end - start
    }

    fn cast(row: &'a [u8]) -> Self::ReadResult {
        ArrayGetter {
            array_data: ArrayData::new(row),
            _marker: PhantomData::<T>,
        }
    }

    fn schema() -> Schema {
        Schema::new(30, true)
    }
}
