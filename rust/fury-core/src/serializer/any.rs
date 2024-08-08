use std::any::Any;

use crate::{error::Error, fury::Fury, resolver::context::{ReadContext, WriteContext}, types::FieldType};
use super::{polymorph, Serializer};




impl Serializer for Box<dyn Any> {
    fn reserved_space() -> usize {
        0
    }

    fn write(&self, _context: &mut WriteContext) {
        panic!("unreachable")
    }

    fn read(_context: &mut ReadContext) -> Result<Self, Error> {
        panic!("unreachable")
    }

    fn get_type_id(_fury: &Fury) -> i16 {
        FieldType::FuryTypeTag.into()
    }

    fn serialize(&self, context: &mut WriteContext) {
        polymorph::serialize(self.as_ref(), self.as_ref().type_id(), context);
    }

    fn deserialize(context: &mut ReadContext) -> Result<Self, Error> {
        polymorph::deserialize::<Self>(context)
    }
}