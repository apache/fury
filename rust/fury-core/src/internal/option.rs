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
use crate::types::{FieldType, FuryGeneralList, RefFlag};
use crate::write_state::WriteState;

impl<T: Serializer> Serializer for Option<T> {
    fn read(deserializer: &mut ReadState) -> Result<Self, Error> {
        Ok(Some(T::read(deserializer)?))
    }

    fn deserialize(deserializer: &mut ReadState) -> Result<Self, Error> {
        // ref flag
        let ref_flag = deserializer.reader.i8();

        if ref_flag == (RefFlag::NotNullValue as i8) || ref_flag == (RefFlag::RefValue as i8) {
            // type_id
            let type_id = deserializer.reader.i16();

            if type_id != T::ty() as i16 {
                Err(Error::FieldType {
                    expected: T::ty(),
                    actial: type_id,
                })
            } else {
                Ok(Some(T::read(deserializer)?))
            }
        } else if ref_flag == (RefFlag::Null as i8) {
            Ok(None)
        } else if ref_flag == (RefFlag::Ref as i8) {
            Err(Error::Ref)
        } else {
            Err(Error::BadRefFlag)
        }
    }

    fn write(&self, serializer: &mut WriteState) {
        if let Some(v) = self {
            T::write(v, serializer)
        } else {
            unreachable!("write should be call by serialize")
        }
    }

    fn serialize(&self, serializer: &mut WriteState) {
        match self {
            Some(v) => {
                // ref flag
                serializer.writer.i8(RefFlag::NotNullValue as i8);
                // type
                serializer.writer.i16(T::ty() as i16);

                v.write(serializer);
            }
            None => {
                serializer.writer.i8(RefFlag::Null as i8);
            }
        }
    }

    fn reserved_space() -> usize {
        std::mem::size_of::<T>()
    }

    fn ty() -> FieldType {
        T::ty()
    }
}

impl<T: Serializer> FuryGeneralList for Option<T> {}
