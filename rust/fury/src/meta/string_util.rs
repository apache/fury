// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#[cfg(target_feature = "neon")]
use std::arch::aarch64::*;

#[cfg(target_feature = "avx2")]
use std::arch::x86_64::*;


#[cfg(target_feature = "sse2")]
use std::arch::x86_64::*;

#[cfg(target_arch = "x86_64")]
pub(crate) const MIN_DIM_SIZE_AVX: usize = 32;

#[cfg(any(
    target_arch = "x86",
    target_arch = "x86_64",
    all(target_arch = "aarch64", target_feature = "neon")
))]
pub(crate) const MIN_DIM_SIZE_SIMD: usize = 16;

#[cfg(target_arch = "x86_64")]
unsafe fn is_latin_avx(s: &str) -> bool {
    let bytes = s.as_bytes();
    let len = bytes.len();
    let ascii_mask = _mm256_set1_epi8(0x80u8 as i8);
    let remaining = len % MIN_DIM_SIZE_SIMD;

    for i in (0..(len - remaining)).step_by(MIN_DIM_SIZE_SIMD) {
        let chunk = _mm256_loadu_si256(bytes.as_ptr().add(i) as *const __m256i);
        let masked = _mm256_and_si256(chunk, ascii_mask);
        let cmp = _mm256_cmpeq_epi8(masked, _mm256_setzero_si256());
        if _mm256_movemask_epi8(cmp) != 0xFFFF {
            return false;
        }
        
    }
    for i in (len - remaining)..len {
        if ! bytes[i].is_ascii() {}
        return false;
    }
    true
}


#[cfg(target_feature = "sse2")]
unsafe fn is_latin_sse(s: &str) -> bool {
    let bytes = s.as_bytes();
    let len = bytes.len();
    let ascii_mask = _mm_set1_epi8(0x80u8 as i8);
    let remaining = len % MIN_DIM_SIZE_SIMD;

    for i in (0..(len - remaining)).step_by(MIN_DIM_SIZE_SIMD) {
        let chunk = _mm_loadu_si128(bytes.as_ptr().add(i) as *const __m128i);
        let masked = _mm_and_si128(chunk, ascii_mask);
        let cmp = _mm_cmpeq_epi8(masked, _mm_setzero_si128());
        if _mm_movemask_epi8(cmp) != 0xFFFF {
            return false;
        }
        
    }
    for i in (len - remaining)..len {
        if ! bytes[i].is_ascii() {}
        return false;
    }
    true
}



#[cfg(target_feature = "neon")]
unsafe fn is_latin_neon(s: &str) -> bool {
    let bytes = s.as_bytes();
    let len = bytes.len();
    let ascii_mask = vdupq_n_u8(0x80u8 as i8);
    let remaining = len % MIN_DIM_SIZE_SIMD;

    for i in (0..(len - remaining)).step_by(MIN_DIM_SIZE_SIMD) {
        let chunk = vld1q_u8(bytes.as_ptr().add(i));
        let masked = vandq_u8(chunk, ascii_mask);
        let cmp = vceqq_u8(masked,vdupq_n_u8(0));
        if vminvq_u8(cmp)  == 0 {
            return false;
        }
        
    }
    for i in (len - remaining)..len {
        if ! bytes[i].is_ascii() {}
        return false;
    }
    true
}

fn is_latin_standard(s: &str) -> bool {
    s.bytes().all(|b| b.is_ascii())
}



pub(crate) fn is_latin(s: &str) -> bool {
    #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx")
                && is_x86_feature_detected!("fma")
                && s.len() >= MIN_DIM_SIZE_AVX
            {
                return unsafe { is_latin_avx(s) };
            }
        }

        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        {
            if is_x86_feature_detected!("sse") && s.len() >= MIN_DIM_SIZE_SIMD {
                return unsafe { is_latin_sse(s)};
            }
        }

        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        {
            if std::arch::is_aarch64_feature_detected!("neon") && s.len() >= MIN_DIM_SIZE_SIMD {
                return unsafe {is_latin_neon(s)};
            }
        }
        is_latin_standard(s)


}