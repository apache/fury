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

use super::row::Row;

pub struct RowWriter {
    bit_map_width_in_bytes: usize,
    base_offset: usize,
    num_fields: usize,
    pub writer: Writer,
}

const WORD_SIZE: usize = 8;

impl RowWriter {
    fn calculate_bitmap_width_in_bytes(&self) -> usize {
        return ((self.num_fields + 63) / 64) * WORD_SIZE;
    }

    fn get_fixed_size(&self) -> usize {
        self.bit_map_width_in_bytes + self.num_fields * 8
    }

    pub fn get_field_offset(&self, idx: usize) -> usize {
        self.base_offset + self.bit_map_width_in_bytes + idx * 8
    }

    pub fn write_offset_size_callback(&mut self, idx: usize) -> impl FnMut(&mut Self) -> usize {
        let offset = self.writer.len() - self.base_offset;
        let field_offset = self.get_field_offset(idx);
        self.writer
            .set_bytes(field_offset, &(offset as u32).to_le_bytes());
        let start: usize = self.writer.len();
        move |this: &mut Self| {
            let size: usize = this.writer.len() - start;
            this.writer
                .set_bytes(field_offset + 4, &(size as u32).to_le_bytes());
            size
        }
    }

    pub fn write_offset_size(&mut self, idx: usize, size: usize) {
        let offset = self.writer.len() - self.base_offset - size;
        let field_offset = self.get_field_offset(idx);
        self.writer
            .set_bytes(field_offset, &(offset as u32).to_le_bytes());
        self.writer
            .set_bytes(field_offset + 4, &(size as u32).to_le_bytes());
    }

    pub fn new() -> RowWriter {
        RowWriter {
            writer: Writer::default(),
            bit_map_width_in_bytes: 0,
            base_offset: 0,
            num_fields: 0,
        }
    }

    pub fn point_to(&mut self, num_fields: usize) {
        self.num_fields = num_fields;
        self.base_offset = self.writer.len();
        self.bit_map_width_in_bytes = self.calculate_bitmap_width_in_bytes();
        let fixed_size = self.get_fixed_size();
        self.writer.reserve(fixed_size);
        self.writer.skip(fixed_size);
    }

    pub fn write(&mut self, bytes: &[u8]) -> usize {
        self.writer.bytes(bytes);
        bytes.len()
    }

    pub fn dump(&self) -> Vec<u8> {
        self.writer.dump()
    }
}

pub fn to_row<'a, T: Row<'a>>(v: &T) -> Vec<u8> {
    let mut state = RowWriter::new();
    if T::schema().is_container() {
        state.point_to(T::schema().num_fields());
    }
    T::write(v, &mut state);
    state.dump()
}
