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

use super::buffer::Writer;
use crate::fury::Fury;
use crate::meta::MetaWriterStore;

pub struct WriteState<'se> {
    pub writer: &'se mut Writer,
    pub tags: Vec<&'static str>,
    fury: &'se Fury,
    meta_store: MetaWriterStore<'se>,
}

impl<'se> WriteState<'se> {
    pub fn new(fury: &'se Fury, writer: &'se mut Writer) -> WriteState<'se> {
        WriteState {
            writer,
            tags: Vec::new(),
            fury,
            meta_store: MetaWriterStore::default(),
        }
    }

    pub fn push_meta(&mut self, type_id: u32, type_def: &'static [u8]) {
        self.meta_store.push(type_id, type_def);
    }

    pub fn get_fury(&self) -> &Fury {
        self.fury
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
}
