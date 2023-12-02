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

use super::{writer::RowWriter, reader::RowReader};

pub struct Schema {
    num_fields: usize,
    is_container: bool,
}

impl Schema {
    pub fn num_fields(&self) -> usize {
        self.num_fields
    }

    pub fn is_container(&self) -> bool{
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

    fn read(idx: usize, row_reader: RowReader<'a>) -> Self::ReadResult;

    fn schema() -> Schema {
        Schema { num_fields: 0, is_container: false }
    }
}

impl<'a> Row<'a> for i8 {
    type ReadResult = Self;

    fn write(v: &Self, row_writer: &mut RowWriter) -> usize {
        row_writer.write(&[*v as u8])
    }


    fn read(idx: usize, row_reader: RowReader) -> Self::ReadResult {
        let bytes = row_reader.get_field_bytes(idx);
        bytes[0] as i8
    }
}

impl<'a> Row<'a> for String {
    type ReadResult = &'a str;

    fn write(v: &Self, row_writer: &mut RowWriter) -> usize {
        row_writer.write(v.as_bytes())
    }

    fn read(idx: usize, row_reader: RowReader<'a>) -> Self::ReadResult {
        let bytes = row_reader.get_field_bytes(idx);
        unsafe { std::str::from_utf8_unchecked(bytes) }
    }
}

