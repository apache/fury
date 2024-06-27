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

from enum import Enum


# Defines the types of supported encodings for MetaStrings.
class Encoding(Enum):
    UTF_8 = 0x00
    LOWER_SPECIAL = 0x01
    LOWER_UPPER_DIGIT_SPECIAL = 0x02
    FIRST_TO_LOWER_SPECIAL = 0x03
    ALL_TO_LOWER_SPECIAL = 0x04


# SHORT_MAX_VALUE is used to check whether the length of the value is valid.
Short_MAX_VALUE = 32767
