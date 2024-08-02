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

use super::meta_string::MetaStringEncoder;
use crate::buffer::{Reader, Writer};
use crate::error::Error;
use crate::types::FieldType;

//todo backward/forward compatibility
#[allow(dead_code)]
pub struct FieldInfo {
    tag_id: u32,
    field_name: String,
    field_type: FieldType,
}

impl FieldInfo {
    pub fn new(field_name: &str, field_type: FieldType) -> FieldInfo {
        FieldInfo {
            field_name: field_name.to_string(),
            field_type,
            tag_id: 0,
        }
    }

    fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        let mut writer = Writer::default();
        let meta_string = MetaStringEncoder::new().encode(&self.field_name)?;
        let mut header = 1 << 2;
        let encoded = meta_string.bytes.as_slice();
        let size = (encoded.len() - 1) as u32;
        header |= (meta_string.encoding as u8) << 3;
        let big_size = size >= 7;
        if big_size {
            header |= 0b11100000;
            writer.u8(header);
            writer.var_int32((size - 7) as i32);
        } else {
            header |= (size << 5) as u8;
            writer.u8(header);
        }
        writer.bytes(encoded);
        Ok(writer.dump())
    }
}

struct TypeMetaLayer {
    type_id: u32,
    field_info: Vec<FieldInfo>,
}

impl TypeMetaLayer {
    fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        let mut writer = Writer::default();
        writer.var_int32(self.field_info.len() as i32);
        writer.var_int32(self.type_id as i32);
        for field in self.field_info.iter() {
            writer.bytes(field.to_bytes()?.as_slice());
        }
        Ok(writer.dump())
    }
}

pub struct TypeMeta {
    hash: u64,
    layers: Vec<TypeMetaLayer>,
}

impl TypeMeta {
    pub fn from_fields(type_id: u32, field_info: Vec<FieldInfo>) -> TypeMeta {
        TypeMeta {
            hash: 0,
            layers: vec![TypeMetaLayer {
                type_id,
                field_info,
            }],
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        let mut writer = Writer::default();
        writer.u64((self.hash << 4) | (self.layers.len() as u64 & 0b1111));
        for layer in self.layers.iter() {
            writer.bytes(layer.to_bytes()?.as_slice());
        }
        Ok(writer.dump())
    }

    pub fn read_hash_from_bytes(reader: &mut Reader) -> u64 {
        reader.u64() >> 7
    }

    pub fn from_bytes(_reader: &mut Reader) -> TypeMeta {
        TypeMeta {
            hash: 0,
            layers: Vec::new(),
        }
    }
}
