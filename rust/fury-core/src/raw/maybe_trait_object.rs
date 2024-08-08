use mem::transmute;
use std::any::TypeId;
use std::mem;
use crate::error::Error;

pub struct MaybeTraitObject {
    ptr: *const u8,
    type_id: TypeId
}

impl MaybeTraitObject {
    pub fn new<T: 'static>(value: T) -> MaybeTraitObject {
        let ptr = unsafe { transmute::<Box<T>, *const u8>(Box::new(value)) };
        let type_id = TypeId::of::<T>();
        MaybeTraitObject {
            ptr,
            type_id
        }
    }

    pub fn to_trait_object<T: 'static>(self) -> Result<T, Error> {
        if self.type_id == TypeId::of::<T>() {
            Ok(unsafe {
                *(transmute::<*const u8, Box<T>>(self.ptr))
            })
        } else {
            Err(Error::ConvertToTraitObjectError {})
        }
    }
}

