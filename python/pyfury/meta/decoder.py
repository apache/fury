# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from pyfury.meta.encoding import Encoding


# Decodes MetaString objects back into their original plain text form.
class MetaStringDecoder:
    def __init__(self, special_char1, special_char2):
        self.special_char1 = special_char1
        self.special_char2 = special_char2

    def decode(self, encoded_data, encoding):
        if len(encoded_data) == 0:
            return ""
        return self.decode_with_encoding(encoded_data, encoding)

    def decode_with_encoding(self, encoded_data, encoding):

        if encoding == Encoding.LOWER_SPECIAL:
            return self._decode_lower_special(encoded_data)
        elif encoding == Encoding.LOWER_UPPER_DIGIT_SPECIAL:
            return self._decode_lower_upper_digit_special(encoded_data)
        elif encoding == Encoding.FIRST_TO_LOWER_SPECIAL:
            return self._decode_rep_first_lower_special(encoded_data)
        elif encoding == Encoding.ALL_TO_LOWER_SPECIAL:
            return self._decode_rep_all_to_lower_special(encoded_data)
        elif encoding == Encoding.UTF_8:
            return encoded_data.tobytes().decode("utf-8")
        else:
            raise ValueError(f"Unexpected encoding flag: {encoding}")

    def _decode_lower_special(self, data):
        decoded = []
        num_bits = len(data) * 8  # Total number of bits in the data
        strip_last_char = (data[0] & 0x80) != 0  # Check the first bit of the first byte
        bit_index = 1
        bit_mask = 0b11111
        while bit_index + 5 <= num_bits and not (
            strip_last_char and (bit_index + 2 * 5 > num_bits)
        ):
            byte_index = bit_index // 8
            intra_byte_index = bit_index % 8
            # // Extract the 5-bit character value across byte boundaries if needed
            char_value = (
                (data[byte_index] << 8)
                | (data[byte_index + 1] if byte_index + 1 < len(data) else 0)
            ) >> (11 - intra_byte_index) & bit_mask
            bit_index += 5
            decoded.append(self._decode_lower_special_char(char_value))

        return "".join(decoded)

    def _decode_lower_upper_digit_special(self, data):
        decoded = []
        bit_index = 1
        strip_last_char = (data[0] & 0x80) != 0
        bit_mask = 0b111111
        num_bits = len(data) * 8
        while bit_index + 6 <= num_bits and not (
            strip_last_char and (bit_index + 2 * 6 > num_bits)
        ):
            byte_index = bit_index // 8
            intra_byte_index = bit_index % 8
            # Extract the 6-bit character value across byte boundaries if needed
            char_value = (
                (data[byte_index] << 8)
                | (data[byte_index + 1] if byte_index + 1 < len(data) else 0)
            ) >> (10 - intra_byte_index) & bit_mask
            bit_index += 6
            decoded.append(self._decode_lower_upper_digit_special_char(char_value))
        return "".join(decoded)

    def _decode_lower_special_char(self, char_value):
        if 0 <= char_value <= 25:
            return chr(ord("a") + char_value)
        elif char_value == 26:
            return "."
        elif char_value == 27:
            return "_"
        elif char_value == 28:
            return "$"
        elif char_value == 29:
            return "|"
        else:
            raise ValueError(f"Invalid character value for LOWER_SPECIAL: {char_value}")

    def _decode_lower_upper_digit_special_char(self, char_value):
        if 0 <= char_value <= 25:
            return chr(ord("a") + char_value)
        elif 26 <= char_value <= 51:
            return chr(ord("A") + (char_value - 26))
        elif 52 <= char_value <= 61:
            return chr(ord("0") + (char_value - 52))
        elif char_value == 62:
            return self.special_char1
        elif char_value == 63:
            return self.special_char2
        else:
            raise ValueError(
                f"Invalid character value for LOWER_UPPER_DIGIT_SPECIAL: {char_value}"
            )

    def _decode_rep_first_lower_special(self, data):
        decoded_str = self._decode_lower_special(data)
        return decoded_str.capitalize()

    def _decode_rep_all_to_lower_special(self, data):
        decoded_str = self._decode_lower_special(data)
        result = []
        skip = False
        for i, char in enumerate(decoded_str):
            if skip:
                skip = False
                continue
            if char == "|":
                result.append(decoded_str[i + 1].upper())
                skip = True
            else:
                result.append(char)
        return "".join(result)
