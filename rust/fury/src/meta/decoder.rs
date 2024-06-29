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
pub struct MetaStringDecoder {
    special_char1: char,
    special_char2: char,
}

impl MetaStringDecoder {
    pub fn new(special_char1: char, special_char2: char) -> Self {
        MetaStringDecoder {
            special_char1,
            special_char2,
        }
    }

    pub fn decode(&self, encoded_data: &[u8], encoding: Encoding) -> Result<String, Error> {
        if encoded_data.is_empty() {
            return Ok("".to_string());
        }
        match encoding {
            Encoding::LowerSpecial => self.decode_lower_special(encoded_data),
            Encoding::LowerUpperDigitSpecial => self.decode_lower_upper_digit_special(encoded_data),
            Encoding::FirstToLowerSpecial => self.decode_rep_first_lower_special(encoded_data),
            Encoding::AllToLowerSpecial => self.decode_rep_all_to_lower_special(encoded_data),
            Encoding::Utf8 => Ok(String::from_utf8_lossy(encoded_data).into_owned()),
        }
    }

    fn decode_lower_special(&self, data: &[u8]) -> Result<String, Error> {
        let mut decoded = String::new();
        let total_bits: usize = data.len() * 8;
        let strip_last_char = (data[0] & 0x80) != 0;
        let bit_mask: usize = 0b11111;
        let mut bit_index = 1;
        while bit_index + 5 <= total_bits && !(strip_last_char && (bit_index + 2 * 5 > total_bits))
        {
            let byte_index = bit_index / 8;
            let intra_byte_index = bit_index % 8;
            let char_value: usize;
            if intra_byte_index > 3 {
                char_value = (((data[byte_index] & 0xFF) as usize) << 8
                    | if byte_index + 1 < data.len() {
                        data.get(byte_index + 1).cloned().unwrap() as usize & 0xFF
                    } else {
                        0
                    })
                    >> (11 - intra_byte_index)
                    & bit_mask;
            } else {
                char_value = (data[byte_index] as usize) >> (3 - intra_byte_index) & bit_mask;
            }
            bit_index += 5;
            decoded.push(self.decode_lower_special_char(char_value as u8)?);
        }
        Ok(decoded)
    }

    fn decode_lower_upper_digit_special(&self, data: &[u8]) -> Result<String, Error> {
        let mut decoded = String::new();
        let num_bits = data.len() * 8;
        let strip_last_char = (data[0] & 0x80) != 0;
        let mut bit_index = 1;
        let bit_mask: usize = 0b111111;

        while bit_index + 6 <= num_bits && !(strip_last_char && (bit_index + 2 * 6 > num_bits)) {
            let byte_index = bit_index / 8;
            let intra_byte_index = bit_index % 8;
            let char_value: usize;
            if intra_byte_index > 2 {
                char_value = (((data[byte_index] & 0xFF) as usize) << 8
                    | if byte_index + 1 < data.len() {
                        data.get(byte_index + 1).cloned().unwrap() as usize & 0xFF
                    } else {
                        0
                    })
                    >> (10 - intra_byte_index)
                    & bit_mask;
            } else {
                char_value = (data[byte_index] as usize) >> (2 - intra_byte_index) & bit_mask;
            }
            bit_index += 6;
            decoded.push(self.decode_lower_upper_digit_special_char(char_value as u8)?);
        }
        Ok(decoded)
    }

    fn decode_lower_special_char(&self, char_value: u8) -> Result<char, Error> {
        match char_value {
            0..=25 => Ok(('a' as u8 + char_value as u8) as char),
            26 => Ok('.'),
            27 => Ok('_'),
            28 => Ok('$'),
            29 => Ok('|'),
            _ => Err(Error::InvalidLowerSpecialValue { value: char_value }),
        }
    }

    fn decode_lower_upper_digit_special_char(&self, char_value: u8) -> Result<char, Error> {
        match char_value {
            0..=25 => Ok(('a' as u8 + char_value as u8) as char),
            26..=51 => Ok(('A' as u8 + char_value as u8 - 26) as char),
            52..=61 => Ok(('0' as u8 + char_value as u8 - 52) as char),
            62 => Ok(self.special_char1),
            63 => Ok(self.special_char2),
            _ => Err(Error::InvalidLowerUpperDigitSpecialValue { value: char_value }),
        }
    }

    fn decode_rep_first_lower_special(&self, data: &[u8]) -> Result<String, Error> {
        let decoded_str = self.decode_lower_special(data)?;
        let mut chars = decoded_str.chars();
        match chars.next() {
            Some(first_char) => {
                let mut result = first_char.to_ascii_uppercase().to_string();
                result.extend(chars);
                Ok(result)
            }
            None => Ok(decoded_str),
        }
    }
    fn decode_rep_all_to_lower_special(&self, data: &[u8]) -> Result<String, Error> {
        let decoded_str = self.decode_lower_special(data)?;
        let mut result = String::new();
        let mut skip = false;
        for (i, char) in decoded_str.chars().enumerate() {
            if skip {
                skip = false;
                continue;
            }
            // Encounter a '|', capitalize the next character
            // and skip the following character.
            if char == '|' {
                if let Some(next_char) = decoded_str.chars().nth(i + 1) {
                    result.push(next_char.to_ascii_uppercase());
                }
                skip = true;
            } else {
                result.push(char);
            }
        }
        Ok(result)
    }
}
