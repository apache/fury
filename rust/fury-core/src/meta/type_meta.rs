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
use crate::meta::{Encoding, MetaStringDecoder};
use anyhow::anyhow;

pub struct FieldInfo {
    field_name: String,
    field_id: i16,
}

impl FieldInfo {
    pub fn new(field_name: &str, field_type: i16) -> FieldInfo {
        FieldInfo {
            field_name: field_name.to_string(),
            field_id: field_type,
        }
    }

    fn u8_to_encoding(value: u8) -> Result<Encoding, Error> {
        match value {
            0x00 => Ok(Encoding::Utf8),
            0x01 => Ok(Encoding::AllToLowerSpecial),
            0x02 => Ok(Encoding::LowerUpperDigitSpecial),
            _ => Err(anyhow!(
                "Unsupported encoding of field name in type meta, value:{value}"
            ))?,
        }
    }

    fn from_bytes(reader: &mut Reader) -> FieldInfo {
        let header = reader.u8();
        let encoding = Self::u8_to_encoding((header & 0b11000) >> 3).unwrap();
        let mut size = (header & 0b11100000) as i32 >> 5;
        size = if size == 0b111 {
            reader.var_int32() + 7
        } else {
            size
        };
        let type_id = reader.i16();
        let field_name = MetaStringDecoder::new()
            .decode(reader.bytes(size as usize), encoding)
            .unwrap();
        FieldInfo {
            field_name,
            field_id: type_id,
        }
    }

    fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        let mut writer = Writer::default();
        let meta_string = MetaStringEncoder::new().encode(&self.field_name)?;
        let mut header = 1 << 2;
        let encoded = meta_string.bytes.as_slice();
        let size = encoded.len() as u32;
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
        writer.i16(self.field_id);
        writer.bytes(encoded);
        Ok(writer.dump())
    }
}

pub struct TypeMetaLayer {
    type_id: u32,
    field_info: Vec<FieldInfo>,
}

impl TypeMetaLayer {
    pub fn new(type_id: u32, field_info: Vec<FieldInfo>) -> TypeMetaLayer {
        TypeMetaLayer {
            type_id,
            field_info,
        }
    }

    pub fn get_type_id(&self) -> u32 {
        self.type_id
    }

    pub fn get_field_info(&self) -> &Vec<FieldInfo> {
        &self.field_info
    }

    fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        let mut writer = Writer::default();
        writer.var_int32(self.field_info.len() as i32);
        writer.var_int32(self.type_id as i32);
        for field in self.field_info.iter() {
            writer.bytes(field.to_bytes()?.as_slice());
        }
        Ok(writer.dump())
    }

    fn from_bytes(reader: &mut Reader) -> TypeMetaLayer {
        let field_num = reader.var_int32();
        let type_id = reader.var_int32() as u32;
        let field_info = (0..field_num)
            .map(|_| FieldInfo::from_bytes(reader))
            .collect();
        TypeMetaLayer::new(type_id, field_info)
    }
}

pub struct TypeMeta {
    hash: u64,
    layers: Vec<TypeMetaLayer>,
}

impl TypeMeta {
    pub fn get_field_info(&self) -> &Vec<FieldInfo> {
        self.layers.first().unwrap().get_field_info()
    }

    pub fn get_type_id(&self) -> u32 {
        self.layers.first().unwrap().get_type_id()
    }

    pub fn from_fields(type_id: u32, field_info: Vec<FieldInfo>) -> TypeMeta {
        TypeMeta {
            hash: 0,
            layers: vec![TypeMetaLayer::new(type_id, field_info)],
        }
    }

    pub fn from_bytes(reader: &mut Reader) -> TypeMeta {
        let header = reader.u64();
        let hash = header >> 8; // high 56bits indicate hash
        let layer_count = header & 0b1111; // class count
        let layers: Vec<TypeMetaLayer> = (0..layer_count)
            .map(|_| TypeMetaLayer::from_bytes(reader))
            .collect();
        TypeMeta { hash, layers }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        let mut writer = Writer::default();
        writer.u64((self.hash << 8) | (self.layers.len() as u64 & 0b1111));
        for layer in self.layers.iter() {
            writer.bytes(layer.to_bytes()?.as_slice());
        }
        Ok(writer.dump())
    }
}
