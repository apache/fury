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

use fury_row::derive_row;
use proc_macro::TokenStream;
use syn::{parse_macro_input, parse_quote, DeriveInput, ItemTrait};
use quote::quote;

mod fury_row;
mod object;
mod util;

#[proc_macro_attribute]
pub fn impl_polymorph(_attr: TokenStream, item: proc_macro::TokenStream) -> TokenStream {
    let mut input: ItemTrait = parse_macro_input!(item as ItemTrait);
    let name = &input.ident;

    let supertrait: syn::TypeParamBound = parse_quote! {
        fury_core::serializer::PolymorphicCast
    };
    
    input.supertraits.insert(input.supertraits.len(), supertrait);

    // 对trait添加supertrait的代码
    let output = quote! {
        #input

        impl fury_core::serializer::Serializer for Box<dyn #name> {
            fn reserved_space() -> usize {
                0
            }

            fn write(&self, _context: &mut fury_core::resolver::context::WriteContext) {
                panic!("unreachable")
            }

            fn read(_context: &mut fury_core::resolver::context::ReadContext) -> Result<Self, Error> {
                panic!("unreachable")
            }

            fn get_type_id(_fury: &Fury) -> i16 {
                fury_core::types::FieldType::FuryTypeTag.into()
            }

            fn serialize(&self, context: &mut fury_core::resolver::context::WriteContext) {
                fury_core::serializer::polymorph::serialize(self.as_ref().as_any(), self.as_ref().type_id(), context);
            }

            fn deserialize(context: &mut fury_core::resolver::context::ReadContext) -> Result<Self, Error> {
                fury_core::serializer::polymorph::deserialize::<Self>(context)
            }
        }

    };

    output.into()
}

#[proc_macro_derive(Fury, attributes(polymorphic_traits))]
pub fn proc_macro_derive_fury_object(input: proc_macro::TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    if let Some(processed) = object::derive_serializer(&input) {
        processed
    } else {
        panic!("macro can not process the target")
    }
}

#[proc_macro_derive(FuryRow)]
pub fn proc_macro_derive_fury_row(input: proc_macro::TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    derive_row(&input)
}
