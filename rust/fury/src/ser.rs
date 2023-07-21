use std::collections::{HashMap, HashSet};

use chrono::{NaiveDate, NaiveDateTime};

use crate::{FuryMeta, RefFlag};

use super::buffer::Writer;

pub trait Serialize
where
    Self: Sized + FuryMeta,
{
    fn write_as_vec_item(&self, serializer: &mut SerializerState) {
        self.write(serializer);
    }

    fn write(&self, serializer: &mut SerializerState);

    fn serialize(&self, serializer: &mut SerializerState) {
        // ref flag
        serializer.writer.i8(RefFlag::NotNullValueFlag as i8);
        // type
        serializer.writer.i16(<Self as FuryMeta>::ty() as i16);
        self.write(serializer);
    }
}

macro_rules! impl_num_serialize {
    ($name: ident, $ty:tt) => {
        impl Serialize for $ty {
            fn write(&self, serializer: &mut SerializerState) {
                serializer.writer.$name(*self);
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

            fn write_as_vec_item(&self, serializer: &mut SerializerState) {
                serializer.writer.$name(*self);
            }
        }
    };
}

impl_num_serialize!(u8, u8);
impl_num_serialize!(u16, u16);
impl_num_serialize!(u32, u32);
impl_num_serialize!(u64, u64);
impl_num_serialize!(i8, i8);
impl_num_serialize_and_pritimive_vec!(i16, i16);
impl_num_serialize_and_pritimive_vec!(i32, i32);
impl_num_serialize_and_pritimive_vec!(i64, i64);
impl_num_serialize_and_pritimive_vec!(f32, f32);
impl_num_serialize_and_pritimive_vec!(f64, f64);

impl Serialize for String {
    fn write(&self, serializer: &mut SerializerState) {
        serializer.writer.string_varint32(self);
    }

    fn write_as_vec_item(&self, serializer: &mut SerializerState) {
        serializer.writer.string_varint32(self);
    }
}

impl Serialize for bool {
    fn write(&self, serializer: &mut SerializerState) {
        serializer.writer.u8(if *self { 1 } else { 0 });
    }

    fn write_as_vec_item(&self, serializer: &mut SerializerState) {
        serializer.writer.u8(if *self { 1 } else { 0 });
    }
}

impl<T1: Serialize, T2: Serialize> Serialize for HashMap<T1, T2> {
    fn write(&self, serializer: &mut SerializerState) {
        // length
        serializer.writer.i32(self.len() as i32);

        // key-value
        for i in self.iter() {
            i.0.write(serializer);
            i.1.write(serializer);
        }
    }
}

impl<T: Serialize> Serialize for HashSet<T> {
    fn write(&self, serializer: &mut SerializerState) {
        // length
        serializer.writer.i32(self.len() as i32);

        // key-value
        for i in self.iter() {
            i.write(serializer);
        }
    }
}

impl Serialize for NaiveDateTime {
    fn write(&self, serializer: &mut SerializerState) {
        serializer.writer.u64(self.timestamp_millis() as u64);
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
}

impl<T> Serialize for Vec<T>
where
    T: Serialize,
{
    fn write(&self, serializer: &mut SerializerState) {
        serializer.writer.i32(self.len() as i32);
        // value
        for i in self.iter() {
            T::write_as_vec_item(i, serializer);
        }
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
}

pub struct SerializerState<'se> {
    pub writer: &'se mut Writer,
}

impl<'de> SerializerState<'de> {
    fn new(writer: &mut Writer) -> SerializerState {
        SerializerState { writer }
    }
}

pub fn to_buffer<T: Serialize>(record: &T) -> Vec<u8> {
    let mut writer = Writer::default();
    // todo. computer reserve size on compile time
    writer.reserve(1000);
    let mut serializer = SerializerState::new(&mut writer);
    <T as Serialize>::serialize(record, &mut serializer);
    writer.dump()
}
