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

mod buffer;
mod deserializer;
mod error;
mod serializer;
mod types;

pub use deserializer::from_buffer;
pub use deserializer::Deserialize;
pub use deserializer::DeserializerState;
pub use error::Error;
pub use serializer::to_buffer;
pub use serializer::Serialize;
pub use serializer::SerializerState;
pub use types::{
    compute_field_hash, compute_string_hash, compute_struct_hash, config_flags, FieldType,
    FuryMeta, Language, RefFlag, SIZE_OF_REF_AND_TYPE,
};
