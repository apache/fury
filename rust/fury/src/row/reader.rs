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

use byteorder::{ByteOrder, LittleEndian};

pub use super::row::Row;
use super::Schema;

pub trait RowData<'r> {
    fn get_offset_size(&self, idx: usize) -> (u32, u32);

    fn get_field_bytes(&self, idx: usize) -> &'r [u8];
}

#[derive(Clone, Copy)]
pub struct StructData<'r> {
    bit_map_width_in_bytes: usize,
    row: &'r [u8],
}

const WORD_SIZE: usize = 8;

impl<'r> StructData<'r> {
    fn calculate_bitmap_width_in_bytes(num_fields: usize) -> usize {
        return ((num_fields + 63) / 64) * WORD_SIZE;
    }

    pub fn new(row: &'r [u8], schema: Schema) -> StructData<'r> {
        let bit_map_width_in_bytes = Self::calculate_bitmap_width_in_bytes(schema.num_fields());
        StructData {
            row,
            bit_map_width_in_bytes,
        }
    }

    fn get_field_offset(&self, idx: usize) -> usize {
        self.bit_map_width_in_bytes + idx * 8
    }
}

impl<'r> RowData<'r> for StructData<'r> {
    fn get_offset_size(&self, idx: usize) -> (u32, u32) {
        let field_offset = self.get_field_offset(idx);
        let offset = LittleEndian::read_u32(&self.row[field_offset..field_offset + 4]);
        let size = LittleEndian::read_u32(&self.row[field_offset + 4..field_offset + 8]);
        (offset, size)
    }

    fn get_field_bytes(&self, idx: usize) -> &'r [u8] {
        let (offset, size) = self.get_offset_size(idx);
        &self.row[(offset as usize)..(offset + size) as usize]
    }
}

#[derive(Clone, Copy)]
pub struct ArrayData<'r> {
    bit_map_width_in_bytes: usize,
    row: &'r [u8],
    num_elements: usize,
}

impl<'r> ArrayData<'r> {
    fn calculate_bitmap_width_in_bytes(num_fields: usize) -> usize {
        return 8 + ((num_fields + 63) / 64) * WORD_SIZE;
    }

    fn get_field_offset(&self, idx: usize) -> usize {
        8 + self.bit_map_width_in_bytes + idx * 8
    }

    pub fn new(row: &'r [u8]) -> ArrayData<'r> {
        let num_elements = LittleEndian::read_u64(&row[0..8]) as usize;
        let bit_map_width_in_bytes = Self::calculate_bitmap_width_in_bytes(num_elements);
        ArrayData {
            row,
            bit_map_width_in_bytes,
            num_elements,
        }
    }

    pub fn num_elements(&self) -> usize {
        self.num_elements
    }
}

impl<'r> RowData<'r> for ArrayData<'r> {
    fn get_offset_size(&self, idx: usize) -> (u32, u32) {
        let offset = LittleEndian::read_u32(
            &self.row[self.get_field_offset(idx)..self.get_field_offset(idx) + 4],
        );
        let size = LittleEndian::read_u32(
            &self.row[self.get_field_offset(idx) + 4..self.get_field_offset(idx) + 8],
        );
        (offset, size)
    }

    fn get_field_bytes(&self, idx: usize) -> &'r [u8] {
        let (offset, size) = self.get_offset_size(idx);
        &self.row[(offset as usize)..(offset + size) as usize]
    }
}

pub fn from_row<'a, T: Row<'a>>(row: &'a [u8]) -> T::ReadResult {
    T::cast(row)
}
