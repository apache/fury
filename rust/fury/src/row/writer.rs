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

use crate::buffer::Writer;

use super::{bit_util::calculate_bitmap_width_in_bytes, row::Row};

pub trait RowWriter {
    fn get_field_offset(&self, idx: usize) -> usize;

    fn base_offset(&self) -> usize;

    fn write_start(&mut self, idx: usize) -> WriteCallbackInfo {
        let base_offset = self.base_offset();
        let field_offset = self.get_field_offset(idx);
        let writer: &mut Writer = self.borrow_writer();
        let offset = writer.len() - base_offset;
        writer.set_bytes(field_offset, &(offset as u32).to_le_bytes());
        let data_start: usize = writer.len();
        WriteCallbackInfo {
            field_offset,
            data_start,
        }
    }

    fn write_end(&mut self, callback_info: WriteCallbackInfo) {
        let writer: &mut Writer = self.borrow_writer();
        let size: usize = writer.len() - callback_info.data_start;
        writer.set_bytes(callback_info.field_offset + 4, &(size as u32).to_le_bytes());
    }

    fn borrow_writer(&mut self) -> &mut Writer;
}

pub struct StructWriter<'a> {
    bit_map_width_in_bytes: usize,
    base_offset: usize,
    num_fields: usize,
    writer: &'a mut Writer,
}

impl<'a> StructWriter<'a> {
    fn get_fixed_size(&self) -> usize {
        self.bit_map_width_in_bytes + self.num_fields * 8
    }

    pub fn new(num_fields: usize, writer: &mut Writer) -> StructWriter {
        let base_offset = writer.len();
        let mut struct_writer = StructWriter {
            writer,
            bit_map_width_in_bytes: 0,
            base_offset,
            num_fields,
        };
        struct_writer.bit_map_width_in_bytes = calculate_bitmap_width_in_bytes(num_fields);
        let fixed_size = struct_writer.get_fixed_size();
        struct_writer.writer.reserve(fixed_size);
        struct_writer.writer.skip(fixed_size);
        struct_writer
    }
}

impl<'a> RowWriter for StructWriter<'a> {
    fn borrow_writer(&mut self) -> &mut Writer {
        self.writer
    }

    fn get_field_offset(&self, idx: usize) -> usize {
        self.base_offset + self.bit_map_width_in_bytes + idx * 8
    }

    fn base_offset(&self) -> usize {
        self.base_offset
    }
}

pub struct ArrayWriter<'a> {
    bit_map_width_in_bytes: usize,
    base_offset: usize,
    num_fields: usize,
    writer: &'a mut Writer,
}

impl<'a> ArrayWriter<'a> {
    fn get_fixed_size(&self) -> usize {
        8 + self.bit_map_width_in_bytes + self.num_fields * 8
    }

    pub fn new(num_fields: usize, writer: &mut Writer) -> ArrayWriter {
        let base_offset = writer.len();
        let mut array_writer = ArrayWriter {
            writer,
            bit_map_width_in_bytes: 0,
            base_offset,
            num_fields,
        };
        array_writer.bit_map_width_in_bytes = calculate_bitmap_width_in_bytes(num_fields);
        let fixed_size = array_writer.get_fixed_size();
        array_writer.writer.reserve(fixed_size);
        array_writer.writer.u64(num_fields as u64);
        array_writer.writer.skip(fixed_size - 8);
        array_writer
    }
}

pub struct WriteCallbackInfo {
    field_offset: usize,
    data_start: usize,
}

impl<'a> RowWriter for ArrayWriter<'a> {
    fn borrow_writer(&mut self) -> &mut Writer {
        self.writer
    }

    fn get_field_offset(&self, idx: usize) -> usize {
        8 + self.base_offset + self.bit_map_width_in_bytes + idx * 8
    }

    fn base_offset(&self) -> usize {
        self.base_offset
    }
}

pub fn to_row<'a, T: Row<'a>>(v: &T) -> Vec<u8> {
    let mut writer = Writer::default();
    T::write(v, &mut writer);
    writer.dump()
}
