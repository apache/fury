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

use super::buffer::Reader;
use super::types::Language;
use crate::error::Error;
use crate::fury::Fury;

pub struct ReadState<'de, 'bf: 'de> {
    pub reader: Reader<'bf>,
    pub tags: Vec<&'de str>,
    pub fury: &'de Fury,
}

impl<'de, 'bf: 'de> ReadState<'de, 'bf> {
    pub fn new(fury: &'de Fury, reader: Reader<'bf>) -> ReadState<'de, 'bf> {
        ReadState {
            reader,
            tags: Vec::new(),
            fury,
        }
    }

    pub fn get_fury(&self) -> &Fury {
        self.fury
    }

    pub fn head(&mut self) -> Result<(), Error> {
        let _bitmap = self.reader.u8();
        let _language: Language = self.reader.u8().try_into()?;
        self.reader.skip(8); // native offset and size
        Ok(())
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
