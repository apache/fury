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

#[derive(Clone, Copy)]
pub struct RowReader<'r> {
    bit_map_width_in_bytes: usize,
    base_offset: usize,
    row: &'r [u8],
}

const WORD_SIZE: usize = 8;

impl<'r> RowReader<'r> {
    fn calculate_bitmap_width_in_bytes(num_fields: usize) -> usize {
        return ((num_fields + 63) / 64) * WORD_SIZE;
    }

    fn get_offset_absolute(&self, idx: usize) -> usize {
        self.base_offset + self.bit_map_width_in_bytes + idx * 8
    }

    pub fn get_offset_size_absolute(&self, idx: usize) -> (u32, u32) {
        let offset = LittleEndian::read_u32(
            &self.row[self.get_offset_absolute(idx)..self.get_offset_absolute(idx) + 4],
        );
        let size = LittleEndian::read_u32(
            &self.row[self.get_offset_absolute(idx) + 4..self.get_offset_absolute(idx) + 8],
        );
        (self.base_offset as u32 + offset, size)
    }

    pub fn new<'a>(row: &'a [u8]) -> RowReader<'a> {
        RowReader {
            row,
            bit_map_width_in_bytes: 0,
            base_offset: 0,
        }
    }

    pub fn point_to(&self, num_fields: usize, base_offset: usize) -> RowReader<'r> {
        let bit_map_width_in_bytes = Self::calculate_bitmap_width_in_bytes(num_fields);
        RowReader {
            row: self.row,
            bit_map_width_in_bytes,
            base_offset,
        }
    }

    pub fn get_field_bytes(&self, idx: usize) -> &'r [u8] {
        let (offset, size) = self.get_offset_size_absolute(idx);
        &self.row[(offset as usize)..(offset + size) as usize]
    }
}

pub fn from_row<'a, T: Row<'a>>(row: &'a [u8]) -> T::ReadResult {
    if T::schema().is_container() {
        let state = RowReader::new(&row);
        T::read(0, state.point_to(T::schema().num_fields(), 0))
    } else {
        let state = RowReader::new(&row);
        T::read(0, state)
    }
}
