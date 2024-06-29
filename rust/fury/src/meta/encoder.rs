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
use super::meta_string::MetaString;

pub struct MetaStringEncoder {
    special_char1: char,
    special_char2: char,
}

#[derive(Debug)]
struct StringStatistics {
    digit_count: usize,
    upper_count: usize,
    can_lower_upper_digit_special_encoded: bool,
    can_lower_special_encoded: bool,
}

impl MetaStringEncoder {
    pub fn new(special_char1: char, special_char2: char) -> Self {
        MetaStringEncoder {
            special_char1,
            special_char2,
        }
    }

    fn is_latin(&self, s: &str) -> bool {
        s.bytes().all(|b| b.is_ascii())
    }

    pub fn encode(&self, input: &str) -> Result<MetaString, Error> {
        if input.is_empty() {
            return MetaString::new(
                input.to_string(),
                Encoding::Utf8,
                self.special_char1,
                self.special_char2,
                vec![],
            );
        }
        if !self.is_latin(input) {
            return MetaString::new(
                input.to_string(),
                Encoding::Utf8,
                self.special_char1,
                self.special_char2,
                input.as_bytes().to_vec(),
            );
        }
        let encoding = self.compute_encoding(input);

        self.encode_with_encoding(input, encoding)
    }

    fn compute_encoding(&self, input: &str) -> Encoding {
        if input.is_empty() {
            return Encoding::LowerSpecial;
        }
        let statistics = self.compute_statistics(input);
        if statistics.can_lower_special_encoded {
            return Encoding::LowerSpecial;
        }
        if statistics.can_lower_upper_digit_special_encoded {
            if statistics.digit_count != 0 {
                return Encoding::LowerUpperDigitSpecial;
            }
            let upper_count: usize = statistics.upper_count;
            if upper_count == 1 && input.chars().next().unwrap().is_uppercase() {
                return Encoding::FirstToLowerSpecial;
            }
            if ((input.len() + upper_count) * 5) < (input.len() * 6) {
                return Encoding::AllToLowerSpecial;
            }
            return Encoding::LowerUpperDigitSpecial;
        }
        return Encoding::Utf8;
    }

    fn compute_statistics(&self, chars: &str) -> StringStatistics {
        let mut can_lower_upper_digit_special_encoded = true;
        let mut can_lower_special_encoded = true;
        let mut digit_count = 0;
        let mut upper_count = 0;
        for c in chars.chars() {
            if can_lower_upper_digit_special_encoded {
                if !(c.is_lowercase()
                    || c.is_uppercase()
                    || c.is_digit(10)
                    || c == self.special_char1
                    || c == self.special_char2)
                {
                    can_lower_upper_digit_special_encoded = false;
                }
            }
            if can_lower_special_encoded == true {
                if !(c.is_lowercase() || matches!(c, '.' | '_' | '$' | '|')) {
                    can_lower_special_encoded = false;
                }
            }
            if c.is_digit(10) {
                digit_count += 1;
            }
            if c.is_uppercase() {
                upper_count += 1;
            }
        }
        StringStatistics {
            digit_count,
            upper_count,
            can_lower_upper_digit_special_encoded,
            can_lower_special_encoded,
        }
    }

    pub fn encode_with_encoding(
        &self,
        input: &str,
        encoding: Encoding,
    ) -> Result<MetaString, Error> {
        if input.len() >= std::i16::MAX as usize {
            return Err(Error::LengthExceed);
        }
        if encoding != Encoding::Utf8 && !self.is_latin(input) {
            return Err(Error::OnlyAllowASCII);
        };
        if input.is_empty() {
            return MetaString::new(
                input.to_string(),
                Encoding::Utf8,
                self.special_char1,
                self.special_char2,
                vec![],
            );
        };

        match encoding {
            Encoding::LowerSpecial => {
                let encoded_data = self.encode_lower_special(input)?;
                return MetaString::new(
                    input.to_string(),
                    encoding,
                    self.special_char1,
                    self.special_char2,
                    encoded_data,
                );
            }
            Encoding::LowerUpperDigitSpecial => {
                let encoded_data = self.encode_lower_upper_digit_special(input)?;
                return MetaString::new(
                    input.to_string(),
                    encoding,
                    self.special_char1,
                    self.special_char2,
                    encoded_data,
                );
            }
            Encoding::FirstToLowerSpecial => {
                let encoded_data = self.encode_first_to_lower_special(input)?;
                return MetaString::new(
                    input.to_string(),
                    encoding,
                    self.special_char1,
                    self.special_char2,
                    encoded_data,
                );
            }
            Encoding::AllToLowerSpecial => {
                let upper_count = input.chars().filter(|c| c.is_uppercase()).count();
                let encoded_data = self.encode_all_to_lower_special(input, upper_count)?;
                return MetaString::new(
                    input.to_string(),
                    encoding,
                    self.special_char1,
                    self.special_char2,
                    encoded_data,
                );
            }
            Encoding::Utf8 => {
                let encoded_data = input.as_bytes().to_vec();
                return MetaString::new(
                    input.to_string(),
                    Encoding::Utf8,
                    self.special_char1,
                    self.special_char2,
                    encoded_data,
                );
            }
        }
    }

    fn encode_generic(&self, input: &str, bits_per_char: u8) -> Result<Vec<u8>, Error> {
        let total_bits: usize = input.len() * bits_per_char as usize + 1;
        let byte_length: usize = (total_bits + 7) / 8;
        let mut bytes = vec![0; byte_length];
        let mut current_bit = 1;
        for c in input.chars() {
            let value = self.char_to_value(c, bits_per_char)?;
            for i in (0..bits_per_char).rev() {
                if (value & (1 << i)) != 0 {
                    let byte_pos: usize = current_bit / 8;
                    let bit_pos: usize = current_bit % 8;
                    bytes[byte_pos] |= 1 << (7 - bit_pos);
                }
                current_bit += 1;
            }
        }
        if byte_length * 8 >= total_bits + bits_per_char as usize {
            bytes[0] |= 0x80;
        }
        Ok(bytes)
    }
    pub fn encode_lower_special(&self, input: &str) -> Result<Vec<u8>, Error> {
        self.encode_generic(input, 5)
    }

    pub fn encode_lower_upper_digit_special(&self, input: &str) -> Result<Vec<u8>, Error> {
        self.encode_generic(input, 6)
    }

    pub fn encode_first_to_lower_special(&self, input: &str) -> Result<Vec<u8>, Error> {
        let mut chars: Vec<char> = input.chars().collect();
        chars[0] = chars[0].to_lowercase().next().unwrap();
        self.encode_generic(&chars.iter().collect::<String>(), 5)
    }

    pub fn encode_all_to_lower_special(
        &self,
        input: &str,
        upper_count: usize,
    ) -> Result<Vec<u8>, Error> {
        let mut new_chars = Vec::with_capacity(input.len() + upper_count);
        for c in input.chars() {
            if c.is_uppercase() {
                new_chars.push('|');
                new_chars.push(c.to_lowercase().next().unwrap());
            } else {
                new_chars.push(c);
            }
        }
        self.encode_generic(&new_chars.iter().collect::<String>(), 5)
    }

    fn char_to_value(&self, c: char, bits_per_char: u8) -> Result<u8, Error> {
        match bits_per_char {
            5 => match c {
                'a'..='z' => Ok(c as u8 - 'a' as u8),
                '.' => Ok(26),
                '_' => Ok(27),
                '$' => Ok(28),
                '|' => Ok(29),
                _ => Err(Error::UnsupportedLowerSpecialCharacter { ch: c }),
            },
            6 => match c {
                'a'..='z' => Ok(c as u8 - 'a' as u8),
                'A'..='Z' => Ok(c as u8 - 'A' as u8 + 26),
                '0'..='9' => Ok(c as u8 - '0' as u8 + 52),
                _ => {
                    if c == self.special_char1 {
                        Ok(62)
                    } else if c == self.special_char2 {
                        Ok(63)
                    } else {
                        Err(Error::UnsupportedLowerUpperDigitSpecialCharacter { ch: c })
                    }
                }
            },
            _ => unreachable!(),
        }
    }
}
