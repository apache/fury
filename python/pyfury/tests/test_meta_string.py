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

import unittest



class TestMetaString(unittest.TestCase):

    def setUp(self):
        self.encoder = MetaStringEncoder('_', '$')
        self.decoder = MetaStringDecoder('_', '$')

    def test_encode_meta_string_lower_special(self):
        encoded = self.encoder.encode_lower_special("abc_def")
        self.assertEqual(len(encoded), 5)
        self.assertEqual(len(self.encoder.encode("org.apache.fury.benchmark.data").encoded_data), 19)
        self.assertEqual(len(self.encoder.encode("MediaContent").encoded_data), 9)
        decoded = self.decoder.decode(encoded, Encoding.LOWER_SPECIAL)
        self.assertEqual(decoded, "abc_def")

        for i in range(128):
            builder = ''.join(chr(ord('a') + j % 26) for j in range(i))
            encoded = self.encoder.encode_lower_special(builder)
            decoded = self.decoder.decode(encoded, Encoding.LOWER_SPECIAL)
            self.assertEqual(decoded, builder)

    def test_encode_meta_string_lower_upper_digit_special(self):
        special_char1 = '.'
        special_char2 = '_'
        encoder = MetaStringEncoder(special_char1, special_char2)
        encoded = encoder.encode_lower_upper_digit_special("ExampleInput123")
        self.assertEqual(len(encoded), 12)
        decoder = MetaStringDecoder(special_char1, special_char2)
        decoded = decoder.decode(encoded, Encoding.LOWER_UPPER_DIGIT_SPECIAL)
        self.assertEqual(decoded, "ExampleInput123")

        for i in range(1, 128):
            string = self.create_string(i, special_char1, special_char2)
            encoded = encoder.encode_lower_upper_digit_special(string)
            decoded = decoder.decode(encoded, Encoding.LOWER_UPPER_DIGIT_SPECIAL)
            self.assertEqual(decoded, string, f"Failed at {i}")

    def create_string(self, length, special_char1, special_char2):
        chars = []
        for j in range(length):
            n = j % 64
            if n < 26:
                chars.append(chr(ord('a') + n))
            elif n < 52:
                chars.append(chr(ord('A') + (n - 26)))
            elif n < 62:
                chars.append(chr(ord('0') + (n - 52)))
            elif n == 62:
                chars.append(special_char1)
            else:
                chars.append(special_char2)
        return ''.join(chars)

    def test_meta_string(self):
        for special_char1, special_char2 in [('.', '_'), ('.', '$'), ('_', '$')]:
            encoder = MetaStringEncoder(special_char1, special_char2)
            for i in range(1, 128):
                try:
                    string = self.create_string(i, special_char1, special_char2)
                    meta_string = encoder.encode(string)
                    self.assertNotEqual(meta_string.encoding, Encoding.UTF_8)
                    self.assertEqual(meta_string.original, string)
                    self.assertEqual(meta_string.special_char1, special_char1)
                    self.assertEqual(meta_string.special_char2, special_char2)
                    decoder = MetaStringDecoder(special_char1, special_char2)
                    new_string = decoder.decode(meta_string.encoded_data, meta_string.encoding)
                    self.assertEqual(new_string, string)
                except Exception as e:
                    self.fail(f"Failed at {i} with exception: {str(e)}")

    def test_encode_empty_string(self):
        for encoding in [Encoding.LOWER_SPECIAL, Encoding.LOWER_UPPER_DIGIT_SPECIAL, Encoding.FIRST_TO_LOWER_SPECIAL, Encoding.ALL_TO_LOWER_SPECIAL, Encoding.UTF_8]:
            meta_string = self.encoder.encode_with_encoding("", encoding)
            self.assertEqual(len(meta_string.encoded_data), 0)
            decoded = self.decoder.decode(meta_string.encoded_data, meta_string.encoding)
            self.assertEqual(decoded, "")

    def test_encode_characters_outside_of_lower_special(self):
        test_string = "abcdefABCDEF1234!@#"
        meta_string = self.encoder.encode(test_string)
        self.assertEqual(meta_string.encoding, Encoding.UTF_8)

    def test_all_to_upper_special_encoding(self):
        test_string = "ABC_DEF"
        meta_string = self.encoder.encode(test_string)
        self.assertEqual(meta_string.encoding, Encoding.LOWER_UPPER_DIGIT_SPECIAL)
        decoded_string = self.decoder.decode(meta_string.encoded_data, meta_string.encoding)
        self.assertEqual(decoded_string, test_string)

    def test_first_to_lower_special_encoding(self):
        test_string = "Aabcdef"
        meta_string = self.encoder.encode(test_string)
        self.assertEqual(meta_string.encoding, Encoding.FIRST_TO_LOWER_SPECIAL)
        decoded_string = self.decoder.decode(meta_string.encoded_data, meta_string.encoding)
        self.assertEqual(decoded_string, test_string)

    def test_utf8_encoding(self):
        test_string = "你好，世界"  # Non-Latin characters
        meta_string = self.encoder.encode(test_string)
        self.assertEqual(meta_string.encoding, Encoding.UTF_8)
        decoded_string = self.decoder.decode(meta_string.encoded_data, meta_string.encoding)
        self.assertEqual(decoded_string, test_string)


if __name__ == "__main__":
    unittest.main()
