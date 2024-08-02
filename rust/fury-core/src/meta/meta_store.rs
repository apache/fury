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

use crate::buffer::{Reader, Writer};
use crate::error::Error;
use crate::meta::TypeMeta;
use std::collections::HashMap;

#[allow(dead_code)]
pub struct MetaReaderStore {
    reading_type_defs: Vec<TypeMeta>,
}

#[allow(dead_code)]
impl MetaReaderStore {
    fn new() -> MetaReaderStore {
        MetaReaderStore {
            reading_type_defs: Vec::new(),
        }
    }

    fn get(&self, index: usize) -> &TypeMeta {
        unsafe { self.reading_type_defs.get_unchecked(index) }
    }

    fn from_bytes(reader: &mut Reader) -> MetaReaderStore {
        let meta_size = reader.var_int32();
        let mut reading_type_defs = Vec::<TypeMeta>::with_capacity(meta_size as usize);
        for _ in 0..meta_size {
            reading_type_defs.push(TypeMeta::from_bytes(reader));
        }
        MetaReaderStore { reading_type_defs }
    }
}

#[derive(Default)]
pub struct MetaWriterStore<'a> {
    writing_type_defs: Vec<&'a [u8]>,
    index_map: HashMap<u32, usize>,
}

#[allow(dead_code)]
impl<'a> MetaWriterStore<'a> {
    pub fn push<'b: 'a>(&mut self, type_id: u32, type_meta_bytes: &'b [u8]) -> usize {
        match self.index_map.get(&type_id) {
            None => {
                let index = self.writing_type_defs.len();
                self.writing_type_defs.push(type_meta_bytes);
                self.index_map.insert(type_id, index);
                index
            }
            Some(index) => *index,
        }
    }

    fn to_bytes(&self, writer: &mut Writer) -> Result<(), Error> {
        for item in self.writing_type_defs.iter() {
            writer.bytes(item)
        }
        Ok(())
    }

    fn reset(&mut self) {
        self.writing_type_defs.clear();
    }
}
