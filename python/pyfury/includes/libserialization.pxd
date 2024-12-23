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

from libc.stdint cimport int32_t
from libcpp cimport bool as c_bool

cdef extern from "fury/type/type.h" namespace "fury" nogil:

    # Declare the C++ TypeId enum
    cdef enum class TypeId(int32_t):
        BOOL = 1
        INT8 = 2
        INT16 = 3
        INT32 = 4
        VAR_INT32 = 5
        INT64 = 6
        VAR_INT64 = 7
        SLI_INT64 = 8
        FLOAT16 = 9
        FLOAT32 = 10
        FLOAT64 = 11
        STRING = 12
        ENUM = 13
        NAMED_ENUM = 14
        STRUCT = 15
        POLYMORPHIC_STRUCT = 16
        COMPATIBLE_STRUCT = 17
        POLYMORPHIC_COMPATIBLE_STRUCT = 18
        NAMED_STRUCT = 19
        NAMED_POLYMORPHIC_STRUCT = 20
        NAMED_COMPATIBLE_STRUCT = 21
        NAMED_POLYMORPHIC_COMPATIBLE_STRUCT = 22
        EXT = 23
        POLYMORPHIC_EXT = 24
        NAMED_EXT = 25
        NAMED_POLYMORPHIC_EXT = 26
        LIST = 27
        SET = 28
        MAP = 29
        DURATION = 30
        TIMESTAMP = 31
        LOCAL_DATE = 32
        DECIMAL = 33
        BINARY = 34
        ARRAY = 35
        BOOL_ARRAY = 36
        INT8_ARRAY = 37
        INT16_ARRAY = 38
        INT32_ARRAY = 39
        INT64_ARRAY = 40
        FLOAT16_ARRAY = 41
        FLOAT32_ARRAY = 42
        FLOAT64_ARRAY = 43
        ARROW_RECORD_BATCH = 44
        ARROW_TABLE = 45

    cdef c_bool IsNamespacedType(int32_t type_id)
