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
use crate::serializer::Serializer;
use crate::types::{FieldType, FuryGeneralList, SIZE_OF_REF_AND_TYPE};
use crate::resolvers::context::WriteContext;
use std::collections::HashSet;
use std::mem;

impl<T: Serializer + Eq + std::hash::Hash> Serializer for HashSet<T> {
    fn write(&self, context: &mut WriteContext) {
        // length
        context.writer.i32(self.len() as i32);

        let reserved_space =
            (<T as Serializer>::reserved_space() + SIZE_OF_REF_AND_TYPE) * self.len();
        context.writer.reserve(reserved_space);

        // key-value
        for i in self.iter() {
            i.serialize(context);
        }
    }

    fn read(context: &mut ReadContext) -> Result<Self, Error> {
        // length
        let len = context.reader.var_int32();
        let mut result = HashSet::new();
        // key-value
        for _ in 0..len {
            result.insert(<T as Serializer>::deserialize(context)?);
        }
        Ok(result)
    }

    fn reserved_space() -> usize {
        mem::size_of::<i32>()
    }

    fn ty() -> FieldType {
        FieldType::FurySet
    }
}

impl<T: Serializer + Eq + std::hash::Hash> FuryGeneralList for HashSet<T> {}
