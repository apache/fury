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


pub struct RowWriter<'w> {
    bit_map_width_in_bytes: usize,
    base_offset: usize,
    num_fields: usize,
    field_idx: usize,
    pub writer: &'w mut Writer,
}

const  WORD_SIZE: usize = 8;

impl<'w> RowWriter<'w> {
    fn calculate_bitmap_width_in_bytes(&self) -> usize {
        return ((self.num_fields + 63) / 64) * WORD_SIZE;
    }

    fn get_fixed_size(&self) -> usize {
        self.bit_map_width_in_bytes + self.num_fields * 8
    }

    pub fn get_field_offset_absolute(&self, idx: usize) -> usize {
        self.base_offset + self.bit_map_width_in_bytes + idx * 8
    }
    
    pub fn write_offset_size(&mut self, size: usize) {
        let offset = self.writer.len() - self.base_offset - size;
        let field_offset = self.get_field_offset_absolute(self.field_idx);
        self.writer.set_bytes(field_offset, &(offset as u32).to_le_bytes());
        self.writer.set_bytes(field_offset + 4, &(size as u32).to_le_bytes());
        self.field_idx += 1;
    }

    pub fn new(writer:  &'w mut Writer) -> RowWriter<'w> {
        RowWriter{ writer, bit_map_width_in_bytes: 0, base_offset: 0, num_fields: 0, field_idx: 0 }
    }

    pub fn point_to(
        &mut self,
        num_fields: usize,
    ) {
        self.num_fields = num_fields;
        self.base_offset = self.writer.len();
        self.field_idx = 0;
        self.bit_map_width_in_bytes = self.calculate_bitmap_width_in_bytes();
        let fixed_size = self.get_fixed_size();
        self.writer.reserve(fixed_size);
        self.writer.skip(fixed_size);
    }

    pub fn write(&mut self, bytes: &[u8]) -> usize {
        self.writer.bytes(bytes);
        bytes.len()
    }
}

pub fn to_row<'a, T: Row<'a>>(v: &T) -> Vec<u8> {
    let mut writer = Writer::default();
    let mut state = RowWriter::new(&mut writer);
    if T::schema().is_container() {
        state.point_to(T::schema().num_fields());
    }
    T::write(v, &mut state);
    writer.dump()
}