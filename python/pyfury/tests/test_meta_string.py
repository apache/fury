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

import pytest
import numpy as np
from pyfury.meta.metastring import (
    MetaStringEncoder,
    MetaStringDecoder,
    Encoding,
)


def test_encode_meta_string_lower_special():
    encoder = MetaStringEncoder("_", "$")
    decoder = MetaStringDecoder("_", "$")
    encoded = encoder._encode_lower_special("abc_def")
    assert len(encoded) == 5
    assert len(encoder.encode("org.apache.fury.benchmark.data").encoded_data) == 19
    assert len(encoder.encode("MediaContent").encoded_data) == 9
    decoded = decoder.decode(encoded, Encoding.LOWER_SPECIAL)
    assert decoded == "abc_def"

    for i in range(128):
        builder = "".join(chr(ord("a") + j % 26) for j in range(i))
        encoded = encoder._encode_lower_special(builder)
        decoded = decoder.decode(encoded, Encoding.LOWER_SPECIAL)
        assert decoded == builder


def test_encode_meta_string_lower_upper_digit_special():
    special_char1 = "."
    special_char2 = "_"
    encoder = MetaStringEncoder(special_char1, special_char2)
    encoded = encoder._encode_lower_upper_digit_special("ExampleInput123")
    assert len(encoded) == 12
    decoder = MetaStringDecoder(special_char1, special_char2)
    decoded = decoder.decode(encoded, Encoding.LOWER_UPPER_DIGIT_SPECIAL)
    assert decoded == "ExampleInput123"

    for i in range(1, 128):
        string = create_string(i, special_char1, special_char2)
        encoded = encoder._encode_lower_upper_digit_special(string)
        decoded = decoder.decode(encoded, Encoding.LOWER_UPPER_DIGIT_SPECIAL)
        assert decoded == string, f"Failed at {i}"


def create_string(length, special_char1, special_char2):
    chars = []
    for j in range(length):
        n = j % 64
        if n < 26:
            chars.append(chr(ord("a") + n))
        elif n < 52:
            chars.append(chr(ord("A") + (n - 26)))
        elif n < 62:
            chars.append(chr(ord("0") + (n - 52)))
        elif n == 62:
            chars.append(special_char1)
        else:
            chars.append(special_char2)
    return "".join(chars)


def test_meta_string():
    for special_char1, special_char2 in [(".", "_"), (".", "$"), ("_", "$")]:
        encoder = MetaStringEncoder(special_char1, special_char2)
        for i in range(1, 128):
            try:
                string = create_string(i, special_char1, special_char2)
                meta_string = encoder.encode(string)
                assert meta_string.encoding != Encoding.UTF_8
                assert meta_string.original == string
                assert meta_string.special_char1 == special_char1
                assert meta_string.special_char2 == special_char2
                decoder = MetaStringDecoder(special_char1, special_char2)
                new_string = decoder.decode(
                    meta_string.encoded_data, meta_string.encoding
                )
                assert new_string == string
            except Exception as e:
                pytest.fail(f"Failed at {i} with exception: {str(e)}")


def test_encode_empty_string():
    encoder = MetaStringEncoder("_", "$")
    decoder = MetaStringDecoder("_", "$")
    for encoding in [
        Encoding.LOWER_SPECIAL,
        Encoding.LOWER_UPPER_DIGIT_SPECIAL,
        Encoding.FIRST_TO_LOWER_SPECIAL,
        Encoding.ALL_TO_LOWER_SPECIAL,
        Encoding.UTF_8,
    ]:
        meta_string = encoder.encode_with_encoding("", encoding)
        assert len(meta_string.encoded_data) == 0
        decoded = decoder.decode(meta_string.encoded_data, meta_string.encoding)
        assert decoded == ""


def test_encode_characters_outside_of_lower_special():
    encoder = MetaStringEncoder("_", "$")

    test_string = "abcdefABCDEF1234!@#"
    meta_string = encoder.encode(test_string)
    assert meta_string.encoding == Encoding.UTF_8


def test_all_to_upper_special_encoding():
    encoder = MetaStringEncoder("_", "$")
    decoder = MetaStringDecoder("_", "$")
    test_string = "ABC_DEF"
    meta_string = encoder.encode(test_string)
    assert meta_string.encoding == Encoding.LOWER_UPPER_DIGIT_SPECIAL
    decoded_string = decoder.decode(meta_string.encoded_data, meta_string.encoding)
    assert decoded_string == test_string


def test_first_to_lower_special_encoding():
    encoder = MetaStringEncoder("_", "$")
    decoder = MetaStringDecoder("_", "$")
    test_string = "Aabcdef"
    meta_string = encoder.encode(test_string)
    assert meta_string.encoding == Encoding.FIRST_TO_LOWER_SPECIAL
    decoded_string = decoder.decode(meta_string.encoded_data, meta_string.encoding)
    assert decoded_string == test_string


def test_utf8_encoding():
    encoder = MetaStringEncoder("_", "$")
    decoder = MetaStringDecoder("_", "$")
    test_string = "你好，世界"  # Non-Latin characters
    meta_string = encoder.encode(test_string)
    assert meta_string.encoding == Encoding.UTF_8
    decoded_string = decoder.decode(meta_string.encoded_data, meta_string.encoding)
    assert decoded_string == test_string


def test_strip_last_char():
    encoder = MetaStringEncoder("_", "$")

    test_string = "abc"  # encoded as 1|00000|00, 001|00010, exactly two bytes
    encoded_meta_string = encoder.encode(test_string)
    assert not encoded_meta_string.strip_last_char

    test_string = "abcde"  # encoded as 1|00000|00, 001|00010, 00011|001, 00xxxxxx, stripped last char
    encoded_meta_string = encoder.encode(test_string)
    assert encoded_meta_string.strip_last_char


def test_empty_string():
    encoder = MetaStringEncoder("_", "$")
    decoder = MetaStringDecoder("_", "$")
    meta_string = encoder.encode("")
    assert np.array_equal(meta_string.encoded_data, np.array([], dtype=np.uint8))

    decoded = decoder.decode(meta_string.encoded_data, meta_string.encoding)
    assert decoded == ""


def test_ascii_encoding():
    encoder = MetaStringEncoder("_", "$")

    test_string = "asciiOnly"
    encoded_meta_string = encoder.encode(test_string)
    assert encoded_meta_string.encoding != Encoding.UTF_8
    assert encoded_meta_string.encoding == Encoding.ALL_TO_LOWER_SPECIAL


def test_non_ascii_encoding():
    encoder = MetaStringEncoder("_", "$")

    test_string = "こんにちは"  # Non-ASCII string
    encoded_meta_string = encoder.encode(test_string)
    assert encoded_meta_string.encoding == Encoding.UTF_8


def test_non_ascii_encoding_and_non_utf8():
    encoder = MetaStringEncoder("_", "$")

    non_ascii_string = "こんにちは"  # Non-ASCII string

    with pytest.raises(
        ValueError, match="Unsupported character for LOWER_SPECIAL encoding: こ"
    ):
        encoder.encode_with_encoding(non_ascii_string, Encoding.LOWER_SPECIAL)
