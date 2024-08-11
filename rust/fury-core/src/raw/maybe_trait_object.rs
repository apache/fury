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
use mem::transmute;
use std::any::TypeId;
use std::mem;

pub struct MaybeTraitObject {
    ptr: *const u8,
    type_id: TypeId,
}

impl MaybeTraitObject {
    pub fn new<T: 'static>(value: T) -> MaybeTraitObject {
        let ptr = unsafe { transmute::<Box<T>, *const u8>(Box::new(value)) };
        let type_id = TypeId::of::<T>();
        MaybeTraitObject { ptr, type_id }
    }

    pub fn to_trait_object<T: 'static>(self) -> Result<T, Error> {
        if self.type_id == TypeId::of::<T>() {
            Ok(unsafe { *(transmute::<*const u8, Box<T>>(self.ptr)) })
        } else {
            Err(Error::ConvertToTraitObjectError {})
        }
    }
}
