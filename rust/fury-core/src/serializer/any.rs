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

use std::any::Any;

use crate::{error::Error, fury::Fury, resolver::context::{ReadContext, WriteContext}, types::FieldType};
use super::{polymorph, Serializer};




impl Serializer for Box<dyn Any> {
    fn reserved_space() -> usize {
        0
    }

    fn write(&self, _context: &mut WriteContext) {
        panic!("unreachable")
    }

    fn read(_context: &mut ReadContext) -> Result<Self, Error> {
        panic!("unreachable")
    }

    fn get_type_id(_fury: &Fury) -> i16 {
        FieldType::FuryTypeTag.into()
    }

    fn serialize(&self, context: &mut WriteContext) {
        polymorph::serialize(self.as_ref(), self.as_ref().type_id(), context);
    }

    fn deserialize(context: &mut ReadContext) -> Result<Self, Error> {
        polymorph::deserialize::<Self>(context)
    }
}