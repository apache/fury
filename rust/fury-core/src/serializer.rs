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

use crate::error::Error;
use crate::read_state::ReadState;
use crate::types::{FieldType, RefFlag};
use crate::write_state::WriteState;

pub trait Serializer
where
    Self: Sized,
{
    /// The fixed memory size of the Type.
    /// Avoid the memory check, which would hurt performance.
    fn reserved_space() -> usize;

    /// Write the data into the buffer.
    fn write(&self, serializer: &mut WriteState);

    /// Entry point of the serialization.
    ///
    /// Step 1: write the type flag and type flag into the buffer.
    /// Step 2: invoke the write function to write the Rust object.
    fn serialize(&self, serializer: &mut WriteState) {
        // ref flag
        serializer.writer.i8(RefFlag::NotNullValue as i8);
        // type
        serializer.writer.i16(Self::ty() as i16);
        self.write(serializer);
    }

    fn read(deserializer: &mut ReadState) -> Result<Self, Error>;

    fn deserialize(deserializer: &mut ReadState) -> Result<Self, Error> {
        // ref flag
        let ref_flag = deserializer.reader.i8();

        if ref_flag == (RefFlag::NotNullValue as i8) || ref_flag == (RefFlag::RefValue as i8) {
            // type_id
            let type_id = deserializer.reader.i16();
            let ty = Self::ty();
            if type_id != ty as i16 {
                Err(Error::FieldType {
                    expected: ty,
                    actial: type_id,
                })
            } else {
                Ok(Self::read(deserializer)?)
            }
        } else if ref_flag == (RefFlag::Null as i8) {
            Err(Error::Null)
        } else if ref_flag == (RefFlag::Ref as i8) {
            Err(Error::Ref)
        } else {
            Err(Error::BadRefFlag)
        }
    }

    fn ty() -> FieldType;

    fn hash() -> u32 {
        0
    }

    fn tag() -> &'static str {
        ""
    }

    fn type_def() -> &'static [u8] {
        &[]
    }
}
