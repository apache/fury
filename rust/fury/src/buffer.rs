// Copyright 2023 The Fury Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    mem, ptr,
    slice::{from_raw_parts, from_raw_parts_mut},
};

use byteorder::{ByteOrder, LittleEndian};

#[derive(Default)]
pub struct Writer {
    bf: Vec<u8>,
    reserved: usize,
}

macro_rules! write_num {
    ($name: ident, $ty: tt) => {
        pub fn $name(&mut self, v: $ty) {
            let c = self.cast::<$ty>(mem::size_of::<$ty>());
            c[0] = v;
            self.move_next(mem::size_of::<$ty>());
        }
    };
}

impl Writer {
    pub fn dump(&self) -> Vec<u8> {
        self.bf.clone()
    }

    pub fn len(&self) -> usize {
        self.bf.len()
    }

    fn move_next(&mut self, additional: usize) {
        unsafe { self.bf.set_len(self.bf.len() + additional) }
    }
    fn ptr(&mut self) -> *mut u8 {
        unsafe {
            let t = self.bf.as_mut_ptr();
            t.add(self.bf.len())
        }
    }

    fn cast<T>(&mut self, len: usize) -> &mut [T] {
        unsafe { from_raw_parts_mut(self.ptr() as *mut T, len) }
    }

    pub fn reserve(&mut self, additional: usize) {
        self.reserved += additional;
        if self.bf.capacity() < self.reserved {
            self.bf.reserve(self.reserved);
        }
    }

    write_num!(u8, u8);
    write_num!(u16, u16);
    write_num!(u32, u32);
    write_num!(u64, u64);
    write_num!(i8, i8);
    write_num!(i16, i16);
    write_num!(i32, i32);
    write_num!(i64, i64);

    pub fn skip(&mut self, len: usize) {
        self.move_next(len);
    }

    pub fn f32(&mut self, value: f32) {
        LittleEndian::write_f32(self.cast::<u8>(4), value);
        self.move_next(4);
    }

    pub fn f64(&mut self, value: f64) {
        LittleEndian::write_f64(self.cast::<u8>(8), value);
        self.move_next(8);
    }

    pub fn var_int32(&mut self, value: i32) {
        if value >> 7 == 0 {
            self.u8(value as u8);
        } else if value >> 14 == 0 {
            let u1 = (value & 0x7F) | 0x80;
            let u2 = value >> 7;
            self.u16(((u1 << 8) | u2) as u16);
        } else if value >> 21 == 0 {
            let u1 = (value & 0x7F) | 0x80;
            let u2 = value >> 7 | 0x80;
            self.u16(((u1 << 8) | u2) as u16);
            self.u8((value >> 14) as u8);
        } else if value >> 28 == 0 {
            let u1 = (value & 0x7F) | 0x80;
            let u2 = value >> 7 | 0x80;
            let u3 = value >> 14 | 0x80;
            let u4 = value >> 21 | 0x80;
            self.u32(((u1 << 24) | (u2 << 16) | (u3 << 8) | u4) as u32);
        } else {
            let u1 = (value & 0x7F) | 0x80;
            let u2 = value >> 7 | 0x80;
            let u3 = value >> 14 | 0x80;
            let u4 = value >> 21 | 0x80;
            self.u32(((u1 << 24) | (u2 << 16) | (u3 << 8) | u4) as u32);
            self.u8((value >> 28) as u8);
        }
    }

    pub fn bytes(&mut self, v: &[u8]) {
        self.reserve(v.len());
        unsafe {
            ptr::copy_nonoverlapping(v.as_ptr(), self.ptr(), v.len());
        }
        self.move_next(v.len());
    }

    pub fn set_bytes(&mut self, offset: usize, data: &[u8]) {
        self.bf
            .get_mut(offset..offset + data.len())
            .expect("//todo")
            .copy_from_slice(data);
    }
}

pub struct Reader<'de> {
    bf: &'de [u8],
    cursor: usize,
}

macro_rules! read_num {
    ($name: ident, $ty: tt) => {
        pub fn $name(&mut self) -> $ty {
            let c = self.cast::<$ty>(mem::size_of::<$ty>());
            let result = c[0];
            self.move_next(mem::size_of::<$ty>());
            result
        }
    };
}

impl<'bf> Reader<'bf> {
    pub fn new(bf: &[u8]) -> Reader {
        Reader { bf, cursor: 0 }
    }

    fn move_next(&mut self, additional: usize) {
        self.cursor += additional;
    }

    fn ptr(&self) -> *const u8 {
        unsafe {
            let t = self.bf.as_ptr();
            t.add(self.cursor)
        }
    }

    fn cast<T>(&self, len: usize) -> &[T] {
        unsafe { from_raw_parts(self.ptr() as *const T, len) }
    }

    read_num!(u8, u8);
    read_num!(u16, u16);
    read_num!(u32, u32);
    read_num!(u64, u64);
    read_num!(i8, i8);
    read_num!(i16, i16);
    read_num!(i32, i32);
    read_num!(i64, i64);

    pub fn f32(&mut self) -> f32 {
        let result = LittleEndian::read_f32(self.cast::<u8>(4));
        self.move_next(4);
        result
    }

    pub fn f64(&mut self) -> f64 {
        let result = LittleEndian::read_f64(self.cast::<u8>(8));
        self.move_next(8);
        result
    }

    pub fn var_int32(&mut self) -> i32 {
        let mut byte_ = self.i8() as i32;
        let mut result = byte_ & 0x7F;
        if (byte_ & 0x80) != 0 {
            byte_ = self.i8() as i32;
            result |= (byte_ & 0x7F) << 7;
            if (byte_ & 0x80) != 0 {
                byte_ = self.i8() as i32;
                result |= (byte_ & 0x7F) << 14;
                if (byte_ & 0x80) != 0 {
                    byte_ = self.i8() as i32;
                    result |= (byte_ & 0x7F) << 21;
                    if (byte_ & 0x80) != 0 {
                        byte_ = self.i8() as i32;
                        result |= (byte_ & 0x7F) << 28;
                    }
                }
            }
        }
        result
    }

    pub fn string(&mut self, len: u32) -> String {
        let result = String::from_utf8_lossy(self.cast::<u8>(len as usize)).to_string();
        self.move_next(len as usize);
        result
    }

    pub fn skip(&mut self, len: u32) {
        self.move_next(len as usize);
    }

    pub fn bytes(&mut self, len: usize) -> &'bf [u8] {
        let result = &self.bf[self.cursor..self.cursor + len];
        self.move_next(len);
        result
    }
}
