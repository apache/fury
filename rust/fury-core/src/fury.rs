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

use chrono::{NaiveDate, NaiveDateTime};

use crate::buffer::{Reader, Writer};
use crate::error::Error;
use crate::resolver::class_resolver::{ClassInfo, ClassResolver};
use crate::resolver::context::ReadContext;
use crate::resolver::context::WriteContext;
use crate::serializer::Serializer;
use crate::types::{config_flags, Language, Mode, SIZE_OF_REF_AND_TYPE};

pub struct Fury {
    mode: Mode,
    class_resolver: ClassResolver,
}

impl Default for Fury {
    fn default() -> Self {
        let mut fury = Fury {
            mode: Mode::SchemaConsistent,
            class_resolver: ClassResolver::default(),
        };
        fury.register_internal_type::<u8>();
        fury.register_internal_type::<i8>();
        fury.register_internal_type::<u16>();
        fury.register_internal_type::<i16>();
        fury.register_internal_type::<u32>();
        fury.register_internal_type::<i32>();
        fury.register_internal_type::<u64>();
        fury.register_internal_type::<i64>();
        fury.register_internal_type::<f32>();
        fury.register_internal_type::<f64>();
        fury.register_internal_type::<String>();
        fury.register_internal_type::<bool>();
        fury.register_internal_type::<NaiveDate>();
        fury.register_internal_type::<NaiveDateTime>();
        // todo generic type
        fury
    }
}

impl Fury {
    fn register_internal_type<T: Serializer>(&mut self) {
        self.register::<i32>(T::get_type_id(self) as u32);
    }

    pub fn mode(mut self, mode: Mode) -> Self {
        self.mode = mode;
        self
    }

    pub fn get_mode(&self) -> &Mode {
        &self.mode
    }

    pub fn write_head<T: Serializer>(&self, writer: &mut Writer) -> usize {
        const HEAD_SIZE: usize = 10;
        writer.reserve(<T as Serializer>::reserved_space() + SIZE_OF_REF_AND_TYPE + HEAD_SIZE);
        let mut bitmap = 0;
        bitmap |= config_flags::IS_LITTLE_ENDIAN_FLAG;
        bitmap |= config_flags::IS_CROSS_LANGUAGE_FLAG;
        writer.u8(bitmap);
        writer.u8(Language::Rust as u8);
        writer.skip(4); // meta offset
        writer.len() - 4
    }

    fn read_head(&self, reader: &mut Reader) -> Result<u32, Error> {
        let _bitmap = reader.u8();
        let _language: Language = reader.u8().try_into()?;
        Ok(reader.u32())
    }

    pub fn deserialize<T: Serializer>(&self, bf: &[u8]) -> Result<T, Error> {
        let mut reader = Reader::new(bf);
        let meta_offset = self.read_head(&mut reader)?;
        let mut context = ReadContext::new(self, reader);
        if meta_offset > 0 {
            context.load_meta(meta_offset as usize);
        }
        <T as Serializer>::deserialize(&mut context)
    }

    pub fn serialize<T: Serializer>(&self, record: &T) -> Vec<u8> {
        let mut writer = Writer::default();
        let meta_offset = self.write_head::<T>(&mut writer);
        let mut context = WriteContext::new(self, &mut writer);
        <T as Serializer>::serialize(record, &mut context);
        if Mode::Compatible == self.mode {
            context.write_meta(meta_offset);
        }
        writer.dump()
    }

    pub fn get_class_resolver(&self) -> &ClassResolver {
        &self.class_resolver
    }

    pub fn register<T: 'static + Serializer>(&mut self, id: u32) -> &ClassInfo {
        self.class_resolver
            .register::<T>(ClassInfo::new::<T>(self, id), id)
    }
}
