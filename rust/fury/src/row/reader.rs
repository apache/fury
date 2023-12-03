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

use super::{bit_util::calculate_bitmap_width_in_bytes, row::Row};
use byteorder::{ByteOrder, LittleEndian};

pub trait RowViewer<'r> {
    fn row(&self) -> &'r [u8];

    fn get_field_offset(&self, idx: usize) -> usize;

    fn get_offset_size(&self, idx: usize) -> (u32, u32) {
        let row = self.row();
        let field_offset = self.get_field_offset(idx);
        let offset = LittleEndian::read_u32(&row[field_offset..field_offset + 4]);
        let size = LittleEndian::read_u32(&row[field_offset + 4..field_offset + 8]);
        (offset, size)
    }

    fn get_field_bytes(&self, idx: usize) -> &'r [u8] {
        let row = self.row();
        let (offset, size) = self.get_offset_size(idx);
        &row[(offset as usize)..(offset + size) as usize]
    }
}

#[derive(Clone, Copy)]
pub struct StructViewer<'r> {
    bit_map_width_in_bytes: usize,
    row: &'r [u8],
}

impl<'r> StructViewer<'r> {
    pub fn new(row: &'r [u8], num_fields: usize) -> StructViewer<'r> {
        let bit_map_width_in_bytes = calculate_bitmap_width_in_bytes(num_fields);
        StructViewer {
            row,
            bit_map_width_in_bytes,
        }
    }
}

impl<'r> RowViewer<'r> for StructViewer<'r> {
    fn get_field_offset(&self, idx: usize) -> usize {
        self.bit_map_width_in_bytes + idx * 8
    }

    fn row(&self) -> &'r [u8] {
        self.row
    }
}

#[derive(Clone, Copy)]
pub struct ArrayViewer<'r> {
    bit_map_width_in_bytes: usize,
    row: &'r [u8],
    num_elements: usize,
}

impl<'r> ArrayViewer<'r> {
    pub fn new(row: &'r [u8]) -> ArrayViewer<'r> {
        let num_elements = LittleEndian::read_u64(&row[0..8]) as usize;
        let bit_map_width_in_bytes = calculate_bitmap_width_in_bytes(num_elements);
        ArrayViewer {
            row,
            bit_map_width_in_bytes,
            num_elements,
        }
    }

    pub fn num_elements(&self) -> usize {
        self.num_elements
    }
}

impl<'r> RowViewer<'r> for ArrayViewer<'r> {
    fn get_field_offset(&self, idx: usize) -> usize {
        8 + self.bit_map_width_in_bytes + idx * 8
    }

    fn row(&self) -> &'r [u8] {
        self.row
    }
}

pub fn from_row<'a, T: Row<'a>>(row: &'a [u8]) -> T::ReadResult {
    T::cast(row)
}
