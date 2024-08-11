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

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Attribute, Ident};

fn parse_attributes(attrs: &[Attribute]) -> Vec<String> {
    let tag = attrs
        .iter()
        .find(|attr| attr.path().is_ident("polymorphic_traits"));

    if tag.is_none() {
        return vec![];
    }

    let expr: syn::ExprLit = tag
        .unwrap()
        .parse_args()
        .expect("should tag contain string value");
    match expr.lit {
        syn::Lit::Str(s) => {
            let v: Vec<String> = s.value().split(',').map(|x| x.to_string()).collect();
            v
        }
        _ => {
            panic!("tag should be string")
        }
    }
}

pub fn gen(name: &Ident, attrs: &[Attribute]) -> TokenStream {
    let traits = parse_attributes(attrs);
    let stream = traits.iter().map(|x| {
        let x: proc_macro2::Ident = syn::Ident::new(&x.to_string(), name.span());
        let name: proc_macro2::Ident = syn::Ident::new(&format!("deserializer_trait_object_{}", x).to_lowercase(), name.span());
        quote! {
            fn #name<T: fury_core::serializer::Serializer + #x + 'static>(context: &mut fury_core::resolver::context::ReadContext) -> Result<fury_core::raw::maybe_trait_object::MaybeTraitObject, fury_core::error::Error> {
                match T::deserialize(context) {
                    Ok(v) => {
                        Ok(fury_core::raw::maybe_trait_object::MaybeTraitObject::new(
                            Box::new(v) as Box<dyn #x>,
                        ))
                    },
                    Err(e) => Err(e),
                }
            }
            ret.insert(TypeId::of::<Box<dyn #x>>(), #name::<Self>);
        }
    });

    quote! {
        fn get_trait_object_deserializer() -> std::collections::hash_map::HashMap<TypeId, fn(context: &mut fury_core::resolver::context::ReadContext) -> Result<fury_core::raw::maybe_trait_object::MaybeTraitObject, fury_core::error::Error>> {
            let mut ret: std::collections::hash_map::HashMap<TypeId, fn(context: &mut fury_core::resolver::context::ReadContext) -> Result<fury_core::raw::maybe_trait_object::MaybeTraitObject, fury_core::error::Error>> = std::collections::hash_map::HashMap::new();
            ret.insert(TypeId::of::<Box<dyn Any>>(), fury_core::serializer::polymorph::as_any_trait_object::<Self>);
            #(#stream)*
            ret
        }
    }
}
