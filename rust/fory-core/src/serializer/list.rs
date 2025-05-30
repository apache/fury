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
use crate::fory::Fory;
use crate::resolver::context::ReadContext;
use crate::resolver::context::WriteContext;
use crate::serializer::Serializer;
use crate::types::{FieldType, ForyGeneralList, SIZE_OF_REF_AND_TYPE};
use std::mem;

impl<T> Serializer for Vec<T>
where
    T: Serializer + ForyGeneralList,
{
    fn write(&self, context: &mut WriteContext) {
        context.writer.var_int32(self.len() as i32);
        context
            .writer
            .reserve((<Self as Serializer>::reserved_space() + SIZE_OF_REF_AND_TYPE) * self.len());
        for item in self.iter() {
            item.serialize(context);
        }
    }

    fn read(context: &mut ReadContext) -> Result<Self, Error> {
        // vec length
        let len = context.reader.var_int32();
        (0..len)
            .map(|_| T::deserialize(context))
            .collect::<Result<Vec<_>, Error>>()
    }

    fn reserved_space() -> usize {
        // size of the vec
        mem::size_of::<u32>()
    }

    fn get_type_id(_fory: &Fory) -> i16 {
        FieldType::ARRAY.into()
    }
}

impl<T> ForyGeneralList for Vec<T> where T: Serializer {}
