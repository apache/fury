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
use crate::serializer::Serializer;
use crate::types::{FieldType, FuryGeneralList};
use crate::write_state::WriteState;
use std::mem;

impl Serializer for String {
    fn reserved_space() -> usize {
        mem::size_of::<i32>()
    }

    fn write(&self, serializer: &mut WriteState) {
        serializer.writer.var_int32(self.len() as i32);
        serializer.writer.bytes(self.as_bytes());
    }

    fn read(deserializer: &mut ReadState) -> Result<Self, Error> {
        let len = deserializer.reader.var_int32();
        Ok(deserializer.reader.string(len as usize))
    }

    fn ty() -> FieldType {
        FieldType::STRING
    }
}

impl FuryGeneralList for String {}
