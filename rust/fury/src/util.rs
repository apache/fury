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

#[derive(thiserror::Error, Debug)]
pub enum UtilError {
    #[error("Invalid UTF-16 string: missing surrogate pair")]
    MissingSurrogatePair,
}

// Swapping the high 8 bits and the low 8 bits of a 16-bit value
fn swap_endian(value: u16) -> u16 {
    ((value & 0xff) << 8) | ((value & 0xff00) >> 8)
}

pub fn to_utf8(utf16: &[u16], is_little_endian: bool) -> Result<Vec<u8>, UtilError> {
    // Pre-allocating approximate capacity to minimize dynamic resizing
    let mut utf8_bytes = Vec::with_capacity(utf16.len() * 2);
    let mut iter = utf16.iter();
    while let Some(&wc) = iter.next() {
        let wc = if is_little_endian {
            swap_endian(wc)
        } else {
            wc
        };
        match wc {
            // 1-byte UTF-8
            code_point if code_point < 0x80 => {
                utf8_bytes.push(code_point as u8);
            }
            // 2-byte UTF-8
            code_point if code_point < 0x800 => {
                // 110????? 10??????
                // Need 11 bit suffix of wc
                let second = ((code_point & 0b111111) as u8) | 0b10000000;
                let first = ((code_point >> 6 & 0b11111) as u8) | 0b11000000;
                utf8_bytes.push(first);
                utf8_bytes.push(second);
            }
            // Surrogate pair (4-byte UTF-8)
            wc1 if (0xd800..=0xdbff).contains(&wc1) => {
                // Need extra byte
                if let Some(&wc2) = iter.next() {
                    let wc2 = if is_little_endian {
                        swap_endian(wc2)
                    } else {
                        wc2
                    };
                    // utf16 to unicode
                    let code_point =
                        ((((wc1 as u32) - 0xd800) << 10) | ((wc2 as u32) - 0xdc00)) + 0x10000;
                    // 11110??? 10?????? 10?????? 10??????
                    // Need 21 bit suffix of code_point
                    let fourth = ((code_point & 0b111111) as u8) | 0b10000000;
                    let third = ((code_point >> 6 & 0b111111) as u8) | 0b10000000;
                    let second = ((code_point >> 12 & 0b111111) as u8) | 0b10000000;
                    let first = ((code_point >> 18 & 0b111) as u8) | 0b11110000;
                    utf8_bytes.push(first);
                    utf8_bytes.push(second);
                    utf8_bytes.push(third);
                    utf8_bytes.push(fourth);
                } else {
                    return Err(UtilError::MissingSurrogatePair);
                }
            }
            // 3-byte UTF-8
            _ => {
                // 1110???? 10?????? 10??????
                // Need 16 bit suffix of wc, as same as wc itself
                let third = ((wc & 0b111111) as u8) | 0b10000000;
                let second = ((wc >> 6 & 0b111111) as u8) | 0b10000000;
                let first = ((wc >> 12) as u8) | 0b11100000;
                utf8_bytes.push(first);
                utf8_bytes.push(second);
                utf8_bytes.push(third);
            }
        }
    }
    Ok(utf8_bytes)
}
