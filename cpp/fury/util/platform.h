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

#if defined(__x86_64__) || defined(_M_X64)
#include <immintrin.h>
#define FURY_HAS_IMMINTRIN
#elif defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>
#define FURY_HAS_NEON
#elif defined(__SSE2__)
#include <emmintrin.h>
#define FURY_HAS_SSE2
#elif defined(__riscv) && __riscv_vector
#include <riscv_vector.h>
#define FURY_HAS_RISCV_VECTOR
#endif
