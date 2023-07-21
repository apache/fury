use crate::{
    error::Error,
    types::{FuryMeta, RefFlag},
};
use chrono::{DateTime, Days, NaiveDate, NaiveDateTime, TimeZone, Utc};
use std::{
    collections::{HashMap, HashSet},
    mem,
};

use super::buffer::Reader;

fn from_u8_slice<T: Clone>(slice: &[u8]) -> Vec<T> {
    let byte_len = slice.len() / mem::size_of::<T>();
    unsafe { std::slice::from_raw_parts(slice.as_ptr().cast::<T>(), byte_len) }.to_vec()
}

pub trait Deserialize
where
    Self: Sized + FuryMeta,
{
    fn read(deserializer: &mut DeserializerState) -> Result<Self, Error>;

    fn read_vec(deserializer: &mut DeserializerState) -> Result<Vec<Self>, Error> {
        // length
        let len = deserializer.reader.i32();
        // value
        let mut result = Vec::new();
        for _ in 0..len {
            result.push(Self::read(deserializer)?);
        }
        Ok(result)
    }

    fn deserialize(deserializer: &mut DeserializerState) -> Result<Self, Error> {
        // ref flag
        let ref_flag = deserializer.reader.i8();

        if ref_flag == (RefFlag::NotNullValueFlag as i8) {
            // type_id
            let type_id = deserializer.reader.i16();

            if type_id != <Self as FuryMeta>::ty() as i16 {
                Err(Error::FieldType {
                    expected: <Self as FuryMeta>::ty(),
                    actial: type_id,
                })
            } else {
                Ok(Self::read(deserializer)?)
            }
        } else if ref_flag == (RefFlag::NullFlag as i8) {
            Err(Error::Null)
        } else if ref_flag == (RefFlag::RefFlag as i8) {
            Err(Error::Ref)
        } else if ref_flag == (RefFlag::RefValueFlag as i8) {
            Err(Error::RefValue)
        } else {
            Err(Error::BadRefFlag)
        }
    }
}

macro_rules! impl_num_deserialize {
    ($name: ident, $ty:tt) => {
        impl Deserialize for $ty {
            fn read(deserializer: &mut DeserializerState) -> Result<Self, Error> {
                Ok(deserializer.reader.$name())
            }
        }
    };
}

macro_rules! impl_num_deserialize_and_pritimive_vec {
    ($name: ident, $ty:tt) => {
        impl Deserialize for $ty {
            fn read(deserializer: &mut DeserializerState) -> Result<Self, Error> {
                Ok(deserializer.reader.$name())
            }

            fn read_vec(deserializer: &mut DeserializerState) -> Result<Vec<Self>, Error> {
                // length
                let len = deserializer.reader.i32();
                Ok(from_u8_slice::<$ty>(
                    deserializer.reader.bytes::<$ty>(len as usize),
                ))
            }
        }
    };
}

impl_num_deserialize!(u8, u8);
impl_num_deserialize!(u16, u16);
impl_num_deserialize!(u32, u32);
impl_num_deserialize!(u64, u64);
impl_num_deserialize!(i8, i8);
impl_num_deserialize_and_pritimive_vec!(i16, i16);
impl_num_deserialize_and_pritimive_vec!(i32, i32);
impl_num_deserialize_and_pritimive_vec!(i64, i64);
impl_num_deserialize_and_pritimive_vec!(f32, f32);
impl_num_deserialize_and_pritimive_vec!(f64, f64);

impl Deserialize for String {
    fn read(deserializer: &mut DeserializerState) -> Result<Self, Error> {
        let len = deserializer.reader.var_int32();
        Ok(deserializer.reader.string(len as u32))
    }
}

impl Deserialize for bool {
    fn read(deserializer: &mut DeserializerState) -> Result<Self, Error> {
        Ok(deserializer.reader.u8() == 1)
    }
}

impl<T1: Deserialize + Eq + std::hash::Hash, T2: Deserialize> Deserialize for HashMap<T1, T2> {
    fn read(deserializer: &mut DeserializerState) -> Result<Self, Error> {
        // length
        let len = deserializer.reader.i32();
        let mut result = HashMap::new();
        // key-value
        for _ in 0..len {
            result.insert(
                <T1 as Deserialize>::read(deserializer)?,
                <T2 as Deserialize>::read(deserializer)?,
            );
        }
        Ok(result)
    }
}

impl<T: Deserialize + Eq + std::hash::Hash> Deserialize for HashSet<T> {
    fn read(deserializer: &mut DeserializerState) -> Result<Self, Error> {
        // length
        let len = deserializer.reader.i32();
        let mut result = HashSet::new();
        // key-value
        for _ in 0..len {
            result.insert(<T as Deserialize>::read(deserializer)?);
        }
        Ok(result)
    }
}

impl Deserialize for NaiveDateTime {
    fn read(deserializer: &mut DeserializerState) -> Result<Self, Error> {
        let timestamp = deserializer.reader.u64();
        let ret = NaiveDateTime::from_timestamp_millis(timestamp as i64);
        match ret {
            Some(r) => Ok(r),
            None => Err(Error::NaiveDateTime),
        }
    }
}

impl<T: Deserialize> Deserialize for Vec<T> {
    fn read(deserializer: &mut DeserializerState) -> Result<Self, Error> {
        T::read_vec(deserializer)
    }
}

impl<T: Deserialize> Deserialize for Option<T> {
    fn read(deserializer: &mut DeserializerState) -> Result<Self, Error> {
        Ok(Some(T::read(deserializer)?))
    }

    fn deserialize(deserializer: &mut DeserializerState) -> Result<Self, Error> {
        // ref flag
        let ref_flag = deserializer.reader.i8();

        if ref_flag == (RefFlag::NotNullValueFlag as i8) {
            // type_id
            let type_id = deserializer.reader.i16();

            if type_id != <Self as FuryMeta>::ty() as i16 {
                Err(Error::FieldType {
                    expected: <Self as FuryMeta>::ty(),
                    actial: type_id,
                })
            } else {
                Ok(Self::read(deserializer)?)
            }
        } else if ref_flag == (RefFlag::NullFlag as i8) {
            Ok(None)
        } else if ref_flag == (RefFlag::RefFlag as i8) {
            Err(Error::Ref)
        } else if ref_flag == (RefFlag::RefValueFlag as i8) {
            Err(Error::RefValue)
        } else {
            Err(Error::BadRefFlag)
        }
    }
}

lazy_static::lazy_static!(
    static ref EPOCH: DateTime<Utc> = Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap();
);

impl Deserialize for NaiveDate {
    fn read(serializer: &mut DeserializerState) -> Result<Self, Error> {
        let days = serializer.reader.u64();
        match EPOCH.checked_add_days(Days::new(days)) {
            Some(value) => Ok(value.date_naive()),
            None => Err(Error::NaiveDate),
        }
    }
}
pub struct DeserializerState<'de> {
    pub reader: Reader<'de>,
}

impl<'de> DeserializerState<'de> {
    fn new(reader: Reader<'de>) -> DeserializerState<'de> {
        DeserializerState { reader }
    }
}

pub fn from_buffer<T: Deserialize>(bf: &[u8]) -> Result<T, Error> {
    let reader = Reader::new(bf);
    let mut deserializer = DeserializerState::new(reader);
    <T as Deserialize>::deserialize(&mut deserializer)
}
