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


class MetaString:
    def __init__(
        self, original, encoding, special_char1, special_char2, encoded_data, length
    ):
        self.original = original
        self.encoding = encoding
        self.special_char1 = special_char1
        self.special_char2 = special_char2
        self.encoded_data = encoded_data
        self.length = length
        if self.encoding != Encoding.UTF_8:
            self.strip_last_char = (encoded_data[0] & 0x80) != 0
        else:
            self.strip_last_char = False
