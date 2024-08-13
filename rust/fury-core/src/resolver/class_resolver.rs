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

use super::context::{ReadContext, WriteContext};
use crate::error::Error;
use crate::fury::Fury;
use crate::raw::maybe_trait_object::MaybeTraitObject;
use crate::serializer::Serializer;
use std::any::TypeId;
use std::{any::Any, collections::HashMap};

pub type TraitObjectDeserializer = fn(&mut ReadContext) -> Result<MaybeTraitObject, Error>;

pub struct ClassInfo {
    type_def: Vec<u8>,
    fury_type_id: u32,
    rust_type_id: TypeId,
    trait_object_serializer: fn(&dyn Any, &mut WriteContext),
    trait_object_deserializer: HashMap<TypeId, TraitObjectDeserializer>,
}

fn serialize<T: 'static + Serializer>(this: &dyn Any, context: &mut WriteContext) {
    let this = this.downcast_ref::<T>().unwrap();
    T::serialize(this, context)
}

impl ClassInfo {
    pub fn new<T: Serializer>(fury: &Fury, type_id: u32) -> ClassInfo {
        ClassInfo {
            type_def: T::type_def(fury),
            fury_type_id: type_id,
            rust_type_id: TypeId::of::<T>(),
            trait_object_serializer: serialize::<T>,
            trait_object_deserializer: T::get_trait_object_deserializer(),
        }
    }

    pub fn associate<T: Any>(&mut self, type_id: TypeId, func: TraitObjectDeserializer) {
        self.trait_object_deserializer.insert(type_id, func);
    }

    pub fn get_rust_type_id(&self) -> TypeId {
        self.rust_type_id
    }

    pub fn get_fury_type_id(&self) -> u32 {
        self.fury_type_id
    }

    pub fn get_type_def(&self) -> &Vec<u8> {
        &self.type_def
    }

    pub fn get_serializer(&self) -> fn(&dyn Any, &mut WriteContext) {
        self.trait_object_serializer
    }

    pub fn get_trait_object_deserializer<T: 'static>(&self) -> Option<&TraitObjectDeserializer> {
        let type_id = TypeId::of::<T>();
        self.trait_object_deserializer.get(&type_id)
    }
}

#[derive(Default)]
pub struct ClassResolver {
    fury_type_id_map: HashMap<u32, TypeId>,
    class_info_map: HashMap<TypeId, ClassInfo>,
}

impl ClassResolver {
    pub fn get_class_info_by_rust_type(&self, type_id: TypeId) -> &ClassInfo {
        self.class_info_map.get(&type_id).unwrap()
    }

    pub fn get_class_info_by_fury_type(&self, type_id: u32) -> &ClassInfo {
        let type_id = self.fury_type_id_map.get(&type_id).unwrap();
        self.class_info_map.get(type_id).unwrap()
    }

    pub fn register<T: Serializer>(&mut self, class_info: ClassInfo, id: u32) -> &ClassInfo {
        let type_id = TypeId::of::<T>();
        self.fury_type_id_map.insert(id, type_id);
        self.class_info_map.insert(type_id, class_info);
        self.class_info_map.get(&type_id).unwrap()
    }
}
