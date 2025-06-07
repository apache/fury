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
from libc.stdint cimport uint64_t, int64_t, int32_t


cdef uint32_t hash32(void* key, int length, uint32_t seed) nogil:
    cdef int32_t out
    MurmurHash3_x86_32(key, length, seed, &out)
    return out


cdef uint64_t hash64(void* key, int length, uint64_t seed) nogil:
    cdef uint64_t[2] out
    MurmurHash3_x86_128(key, length, seed, &out)
    return out[1]


cdef void hash128_x86(const void* key, int length, uint32_t seed, void* out) nogil:
    MurmurHash3_x86_128(key, length, seed, out)


cdef void hash128_x64(const void* key, int length, uint32_t seed, void* out) nogil:
    MurmurHash3_x64_128(key, length, seed, out)


cpdef tuple hash_unicode(unicode value, uint32_t seed=0):
    return hash_buffer(value.encode('utf8'), seed=seed)


cpdef tuple hash_buffer(const unsigned char[:] value, uint32_t seed=0):
    cdef int64_t[2] out
    MurmurHash3_x64_128(&value[0], value.nbytes, seed, &out)
    return out[0], out[1]
