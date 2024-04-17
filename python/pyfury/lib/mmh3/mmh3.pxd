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

# distutils: language = c++
# cython: embedsignature = True
# cython: language_level = 3
# cython: annotate = True

from libc.stdint cimport uint64_t, uint32_t

cdef extern from "fury/thirdparty/MurmurHash3.h":
    void MurmurHash3_x86_32(void * key, uint64_t len, uint64_t seed, void* out) nogil
    void MurmurHash3_x86_128(void * key, int len, uint32_t seed, void* out) nogil
    void MurmurHash3_x64_128(void * key, int len, uint32_t seed, void* out) nogil

cdef uint32_t hash32(void* key, int length, uint32_t seed) nogil
cdef uint64_t hash64(void* key, int length, uint64_t seed) nogil
cdef void hash128_x86(const void* key, int len, uint32_t seed, void* out) nogil
cdef void hash128_x64(const void* key, int len, uint32_t seed, void* out) nogil
