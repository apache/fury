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

use crate::buffer::{Reader, Writer};
use crate::error::Error;
use crate::read_state::ReadState;
use crate::serializer::Serializer;
use crate::types::Mode;
use crate::write_state::WriteState;

pub struct Fury {
    mode: Mode,
}

impl Default for Fury {
    fn default() -> Self {
        Fury {
            mode: Mode::Compatible,
        }
    }
}

impl Fury {
    pub fn mode(&mut self, mode: Mode) {
        self.mode = mode;
    }

    pub fn get_mode(&self) -> &Mode {
        &self.mode
    }

    pub fn deserialize<T: Serializer>(&self, bf: &[u8]) -> Result<T, Error> {
        let reader = Reader::new(bf);
        let mut deserializer = ReadState::new(self, reader);
        deserializer.head()?;
        <T as Serializer>::deserialize(&mut deserializer)
    }

    pub fn serialize<T: Serializer>(&self, record: &T) -> Vec<u8> {
        let mut writer = Writer::default();
        let mut serializer = WriteState::new(self, &mut writer);
        serializer.head::<T>();
        <T as Serializer>::serialize(record, &mut serializer);
        writer.dump()
    }
}
