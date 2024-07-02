/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "string_util.h"
#include <immintrin.h> // AVX2 header
#include <emmintrin.h> // SSE2 header
#include <random>

namespace fury {

    bool isLatin_AVX2(const std::string& str) {
        const char* data = str.data();
        size_t len = str.size();

        // Process 32 characters at a time with AVX2
        size_t i = 0;
        __m256i latin_mask = _mm256_set1_epi8(0x80);
        for (; i + 32 <= len; i += 32) {
            __m256i chars = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data + i));
            __m256i result = _mm256_and_si256(chars, latin_mask);
            if (!_mm256_testz_si256(result, result)) {
                return false;
            }
        }

        // Process remaining characters
        for (; i < len; ++i) {
            if (static_cast<unsigned char>(data[i]) >= 128) {
                return false;
            }
        }

        return true;
    }

    bool isLatin_SSE2(const std::string& str) {
        const char* data = str.data();
        size_t len = str.size();

        // Process 16 characters at a time with SSE2
        size_t i = 0;
        __m128i latin_mask = _mm_set1_epi8(0x80);
        for (; i + 16 <= len; i += 16) {
            __m128i chars = _mm_loadu_si128(reinterpret_cast<const __m128i*>(data + i));
            __m128i result = _mm_and_si128(chars, latin_mask);
            if (!_mm_testz_si128(result, result)) {
                return false;
            }
        }

        // Process remaining characters
        for (; i < len; ++i) {
            if (static_cast<unsigned char>(data[i]) >= 128) {
                return false;
            }
        }

        return true;
    }

    bool isLatin_Baseline(const std::string& str) {
        for (char c : str) {
            if (static_cast<unsigned char>(c) >= 128) {
                return false;
            }
        }
        return true;
    }

    alignas(32) const char latin_lookup[32] = {
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1
    };

    bool isLatin_AVX2_vpshufb(const std::string& str) {
        const char* data = str.data();
        size_t len = str.size();

        __m256i latin_table = _mm256_load_si256(reinterpret_cast<const __m256i*>(latin_lookup));

        size_t i = 0;
        for (; i + 32 <= len; i += 32) {
            __m256i chars = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data + i));
            __m256i result = _mm256_shuffle_epi8(latin_table, chars);
            if (!_mm256_testz_si256(result, result)) {
                return false;
            }
        }

        for (; i < len; ++i) {
            if (static_cast<unsigned char>(data[i]) >= 128) {
                return false;
            }
        }

        return true;
    }

    std::string generateRandomString(size_t length) {
        const char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        std::default_random_engine rng(std::random_device{}());
        std::uniform_int_distribution<> dist(0, sizeof(charset) - 2);

        std::string result;
        result.reserve(length);
        for (size_t i = 0; i < length; ++i) {
            result += charset[dist(rng)];
        }

        return result;
    }

}
