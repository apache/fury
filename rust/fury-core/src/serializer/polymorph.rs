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
use crate::raw::maybe_trait_object::MaybeTraitObject;
use crate::resolver::context::{ReadContext, WriteContext};
use crate::types::{Mode, RefFlag};
use std::any::{Any, TypeId};

use super::Serializer;


pub fn as_any_trait_object<T: Serializer + Any + 'static>(context: &mut ReadContext) -> Result<MaybeTraitObject, Error> {
    match T::deserialize(context) {
        Ok(v) => {
            Ok(MaybeTraitObject::new(
                Box::new(v) as Box<dyn Any>,
            ))
        },
        Err(e) => Err(e),
    }

}

pub fn serialize(value: &dyn Any, type_id: TypeId, context: &mut WriteContext) {
    context
        .get_fury()
        .get_class_resolver()
        .get_class_info_by_rust_type(type_id)
        .get_serializer()
        (value, context);
}

pub fn deserialize<T: 'static>(context: &mut ReadContext) -> Result<T, Error> {
    let reset_cursor = context.reader.reset_cursor_to_here();
    // ref flag
    let ref_flag = context.reader.i8();

    if ref_flag == (RefFlag::NotNullValue as i8) || ref_flag == (RefFlag::RefValue as i8) {
        let type_id = if context.get_fury().get_mode().eq(&Mode::Compatible) {
            context.meta_resolver.get(context.reader.i16() as usize).get_type_id()
        } else {
            context.reader.i16() as u32
        };
        reset_cursor(&mut context.reader);
        let v = context
            .get_fury()
            .get_class_resolver()
            .get_class_info_by_fury_type(type_id)
            .get_trait_object_deserializer::<T>()
            .unwrap()(context)?;
        Ok(v.to_trait_object::<T>()?)
    } else if ref_flag == (RefFlag::Null as i8) {
        Err(Error::Null)
    } else if ref_flag == (RefFlag::Ref as i8) {
        reset_cursor(&mut context.reader);
        Err(Error::Ref)
    } else {
        reset_cursor(&mut context.reader);
        Err(Error::BadRefFlag)
    }
}