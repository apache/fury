use chrono::{NaiveDate, NaiveDateTime};
use std::collections::{HashMap, HashSet};
use std::mem;

use crate::{config_flags, FuryMeta, Language, RefFlag};

use super::buffer::Writer;

fn to_u8_slice<T>(slice: &[T]) -> &[u8] {
    let byte_len = std::mem::size_of_val(slice);
    unsafe { std::slice::from_raw_parts(slice.as_ptr().cast::<u8>(), byte_len) }
}

pub trait Serialize
where
    Self: Sized + FuryMeta,
{
    fn write_vec(value: &Vec<Self>, serializer: &mut SerializerState) {
        serializer.writer.var_int32(value.len() as i32);
        serializer
            .writer
            .reserve((<Self as Serialize>::reserved_space() + 3) * value.len());
        for item in value.iter() {
            item.serialize(serializer);
        }
    }

    fn reserved_space() -> usize;

    fn write(&self, serializer: &mut SerializerState);

    fn serialize(&self, serializer: &mut SerializerState) {
        // ref flag
        serializer.writer.i8(RefFlag::NotNullValueFlag as i8);
        // type
        serializer.writer.i16(if Self::is_vec() {
            Self::vec_ty()
        } else {
            Self::ty()
        } as i16);
        self.write(serializer);
    }
}

macro_rules! impl_num_serialize {
    ($name: ident, $ty:tt) => {
        impl Serialize for $ty {
            fn write(&self, serializer: &mut SerializerState) {
                serializer.writer.$name(*self);
            }

            fn reserved_space() -> usize {
                mem::size_of::<$ty>()
            }
        }
    };
}

macro_rules! impl_num_serialize_and_pritimive_vec {
    ($name: ident, $ty:tt) => {
        impl Serialize for $ty {
            fn write(&self, serializer: &mut SerializerState) {
                serializer.writer.$name(*self);
            }

            fn write_vec(value: &Vec<Self>, serializer: &mut SerializerState) {
                serializer.writer.var_int32(value.len() as i32);
                serializer.writer.bytes(to_u8_slice(value.as_slice()));
            }

            fn reserved_space() -> usize {
                mem::size_of::<$ty>()
            }
        }
    };
}

impl_num_serialize!(u16, u16);
impl_num_serialize!(u32, u32);
impl_num_serialize!(u64, u64);
impl_num_serialize!(i8, i8);

impl_num_serialize_and_pritimive_vec!(u8, u8);
impl_num_serialize_and_pritimive_vec!(i16, i16);
impl_num_serialize_and_pritimive_vec!(i32, i32);
impl_num_serialize_and_pritimive_vec!(i64, i64);
impl_num_serialize_and_pritimive_vec!(f32, f32);
impl_num_serialize_and_pritimive_vec!(f64, f64);

impl Serialize for String {
    fn write(&self, serializer: &mut SerializerState) {
        serializer.writer.var_int32(self.len() as i32);
        serializer.writer.bytes(self.as_bytes());
    }

    fn write_vec(value: &Vec<Self>, serializer: &mut SerializerState) {
        serializer.writer.var_int32(value.len() as i32);
        serializer
            .writer
            .reserve((<Self as Serialize>::reserved_space()) * value.len());

        for x in value.iter() {
            x.write(serializer);
        }
    }

    fn reserved_space() -> usize {
        4
    }
}

impl Serialize for bool {
    fn write(&self, serializer: &mut SerializerState) {
        serializer.writer.u8(if *self { 1 } else { 0 });
    }

    fn write_vec(value: &Vec<Self>, serializer: &mut SerializerState) {
        serializer.writer.var_int32(value.len() as i32);
        serializer.writer.bytes(to_u8_slice(value.as_slice()));
    }

    fn reserved_space() -> usize {
        1
    }
}

impl<T1: Serialize, T2: Serialize> Serialize for HashMap<T1, T2> {
    fn write(&self, serializer: &mut SerializerState) {
        // length
        serializer.writer.var_int32(self.len() as i32);

        let reserved_space = (<T1 as Serialize>::reserved_space() + 3) * self.len()
            + (<T2 as Serialize>::reserved_space() + 3) * self.len();
        serializer.writer.reserve(reserved_space);

        // key-value
        for i in self.iter() {
            i.0.serialize(serializer);
            i.1.serialize(serializer);
        }
    }

    fn reserved_space() -> usize {
        4
    }
}

impl<T: Serialize> Serialize for HashSet<T> {
    fn write(&self, serializer: &mut SerializerState) {
        // length
        serializer.writer.i32(self.len() as i32);

        let reserved_space = (<T as Serialize>::reserved_space() + 3) * self.len();
        serializer.writer.reserve(reserved_space);

        // key-value
        for i in self.iter() {
            i.serialize(serializer);
        }
    }

    fn reserved_space() -> usize {
        4
    }
}

impl Serialize for NaiveDateTime {
    fn write(&self, serializer: &mut SerializerState) {
        serializer.writer.u64(self.timestamp_millis() as u64);
    }

    fn reserved_space() -> usize {
        8
    }
}

lazy_static::lazy_static!(
    static ref EPOCH: NaiveDate = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
);

impl Serialize for NaiveDate {
    fn write(&self, serializer: &mut SerializerState) {
        let days_since_epoch = self.signed_duration_since(*EPOCH).num_days();
        serializer.writer.u64(days_since_epoch as u64);
    }

    fn reserved_space() -> usize {
        8
    }
}

impl<T> Serialize for Vec<T>
where
    T: Serialize,
{
    fn write(&self, serializer: &mut SerializerState) {
        T::write_vec(self, serializer);
    }

    fn reserved_space() -> usize {
        4
    }
}

impl<T> Serialize for Option<T>
where
    T: Serialize,
{
    fn write(&self, serializer: &mut SerializerState) {
        if let Some(v) = self {
            T::write(v, serializer)
        } else {
            unreachable!("write should be call by serialize")
        }
    }

    fn serialize(&self, serializer: &mut SerializerState) {
        match self {
            Some(v) => {
                // ref flag
                serializer.writer.i8(RefFlag::NotNullValueFlag as i8);
                // type
                serializer.writer.i16(<Self as FuryMeta>::ty() as i16);

                v.write(serializer);
            }
            None => {
                serializer.writer.i8(RefFlag::NullFlag as i8);
            }
        }
    }

    fn reserved_space() -> usize {
        mem::size_of::<T>()
    }
}

pub struct SerializerState<'se> {
    pub writer: &'se mut Writer,
    pub tags: Vec<&'static str>,
}

impl<'de> SerializerState<'de> {
    fn new(writer: &mut Writer) -> SerializerState {
        SerializerState {
            writer,
            tags: Vec::new(),
        }
    }

    pub fn write_tag(&mut self, tag: &'static str) {
        const USESTRINGVALUE: u8 = 0;
        const USESTRINGID: u8 = 1;

        let mayby_idx = self.tags.iter().position(|x| *x == tag);
        match mayby_idx {
            Some(idx) => {
                self.writer.u8(USESTRINGID);
                self.writer.i16(idx as i16);
            }
            None => {
                self.writer.u8(USESTRINGVALUE);
                self.writer.skip(8); // todo tag hash
                self.writer.i16(tag.len() as i16);
                self.writer.bytes(tag.as_bytes());
            }
        };
    }

    fn head<T: Serialize>(&mut self) -> &Self {
        const HEAD_SIZE: usize = 10;
        self.writer
            .reserve(<T as Serialize>::reserved_space() + 3 + HEAD_SIZE);

        let mut bitmap = 0;
        bitmap |= config_flags::IS_LITTLE_ENDIAN_FLAG;
        bitmap |= config_flags::IS_CROSS_LANGUAGE_FLAG;
        self.writer.u8(bitmap);
        self.writer.u8(Language::RUST as u8);
        self.writer.skip(4); // native offset
        self.writer.skip(4); // native size
        self
    }
}

pub fn to_buffer<T: Serialize>(record: &T) -> Vec<u8> {
    let mut writer = Writer::default();
    let mut serializer = SerializerState::new(&mut writer);
    serializer.head::<T>();
    <T as Serialize>::serialize(record, &mut serializer);
    writer.dump()
}
