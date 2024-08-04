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
use crate::fury::Fury;

use crate::meta::TypeMeta;
use crate::resolvers::meta_resolver::{MetaReaderResolver, MetaWriterResolver};
use std::any::TypeId;
use std::rc::Rc;

pub struct WriteContext<'se> {
    pub writer: &'se mut Writer,
    pub tags: Vec<&'static str>,
    fury: &'se Fury,
    meta_resolver: MetaWriterResolver<'se>,
}

impl<'se> WriteContext<'se> {
    pub fn new(fury: &'se Fury, writer: &'se mut Writer) -> WriteContext<'se> {
        WriteContext {
            writer,
            tags: Vec::new(),
            fury,
            meta_resolver: MetaWriterResolver::default(),
        }
    }

    pub fn push_meta(&mut self, type_id: TypeId, type_def: &'static [u8]) -> usize {
        self.meta_resolver.push(type_id, type_def)
    }

    pub fn write_meta(&mut self, offset: usize) {
        self.writer
            .set_bytes(offset, &(self.writer.len() as u32).to_le_bytes());
        self.meta_resolver.to_bytes(self.writer).unwrap()
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

pub struct ReadContext<'de, 'bf: 'de> {
    pub reader: Reader<'bf>,
    pub tags: Vec<&'de str>,
    pub fury: &'de Fury,
    pub meta_resolver: MetaReaderResolver,
}

impl<'de, 'bf: 'de> ReadContext<'de, 'bf> {
    pub fn new(fury: &'de Fury, reader: Reader<'bf>) -> ReadContext<'de, 'bf> {
        ReadContext {
            reader,
            tags: Vec::new(),
            fury,
            meta_resolver: MetaReaderResolver::default(),
        }
    }

    pub fn get_fury(&self) -> &Fury {
        self.fury
    }

    pub fn get_meta(&self, type_index: usize) -> &Rc<TypeMeta> {
        self.meta_resolver.get(type_index)
    }

    pub fn load_meta(&mut self, offset: usize) {
        self.meta_resolver
            .load(&mut Reader::new(&self.reader.slice()[offset..]))
    }

    pub fn read_tag(&mut self) -> Result<&str, Error> {
        const USESTRINGVALUE: u8 = 0;
        const USESTRINGID: u8 = 1;
        let tag_type = self.reader.u8();
        if tag_type == USESTRINGID {
            Ok(self.tags[self.reader.i16() as usize])
        } else if tag_type == USESTRINGVALUE {
            self.reader.skip(8); // todo tag hash
            let len = self.reader.i16();
            let tag: &str =
                unsafe { std::str::from_utf8_unchecked(self.reader.bytes(len as usize)) };
            self.tags.push(tag);
            Ok(tag)
        } else {
            Err(Error::TagType(tag_type))
        }
    }
}
