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
use crate::resolvers::context::ReadContext;
use crate::resolvers::context::WriteContext;
use crate::types::{FieldType, RefFlag};

pub fn serialize<T: Serializer>(this: &T, context: &mut WriteContext) {
    // ref flag
    context.writer.i8(RefFlag::NotNullValue as i8);
    // type
    context.writer.i16(T::ty() as i16);
    this.write(context);
}

pub fn deserialize<T: Serializer>(context: &mut ReadContext) -> Result<T, Error> {
    // ref flag
    let ref_flag = context.reader.i8();

    if ref_flag == (RefFlag::NotNullValue as i8) || ref_flag == (RefFlag::RefValue as i8) {
        // type_id
        let type_id = context.reader.i16();
        let ty = T::ty();
        if type_id != ty as i16 {
            Err(Error::FieldType {
                expected: ty,
                actial: type_id,
            })
        } else {
            Ok(T::read(context)?)
        }
    } else if ref_flag == (RefFlag::Null as i8) {
        Err(Error::Null)
    } else if ref_flag == (RefFlag::Ref as i8) {
        Err(Error::Ref)
    } else {
        Err(Error::BadRefFlag)
    }
}

pub trait Serializer
where
    Self: Default,
{
    /// The fixed memory size of the Type.
    /// Avoid the memory check, which would hurt performance.
    fn reserved_space() -> usize;

    /// Write the data into the buffer.
    fn write(&self, context: &mut WriteContext);

    /// Entry point of the serialization.
    ///
    /// Step 1: write the type flag and type flag into the buffer.
    /// Step 2: invoke the write function to write the Rust object.
    fn serialize(&self, context: &mut WriteContext) {
        serialize(self, context);
    }

    fn read(context: &mut ReadContext) -> Result<Self, Error>;

    fn deserialize(context: &mut ReadContext) -> Result<Self, Error> {
        deserialize(context)
    }

    fn ty() -> FieldType;
}
