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

use super::encoding::Encoding;
use super::error::Error;

#[derive(Debug, PartialEq)]
pub struct MetaString {
    pub original: String,
    pub encoding: Encoding,
    pub special_char1: char,
    pub special_char2: char,
    pub bytes: Vec<u8>,
    pub strip_last_char: bool,
}

impl MetaString {
    pub fn new(
        original: String,
        encoding: Encoding,
        special_char1: char,
        special_char2: char,
        bytes: Vec<u8>,
    ) -> Result<Self, Error> {
        let strip_last_char;
        if encoding != Encoding::Utf8 {
            if bytes.is_empty() {
                return Err(Error::EncodedDataEmpty);
            }
            strip_last_char = (bytes[0] & 0x80) != 0;
        } else {
            strip_last_char = false;
        }
        Ok(MetaString {
            original,
            encoding,
            special_char1,
            special_char2,
            bytes,
            strip_last_char,
        })
    }
}
