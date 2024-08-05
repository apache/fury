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
use syn::Field;

fn hash(fields: &[&Field]) -> TokenStream {
    let props = fields.iter().map(|field| {
        let ty = &field.ty;
        let name = format!("{}", field.ident.as_ref().expect("should be field name"));
        quote! {
            (#name, <#ty as fury_core::serializer::Serializer>::ty())
        }
    });

    quote! {
        fn fury_hash() -> u32 {
            use std::sync::Once;
            static mut name_hash: u32 = 0u32;
            static name_hash_once: Once = Once::new();
            unsafe {
                name_hash_once.call_once(|| {
                        name_hash = fury_core::types::compute_struct_hash(vec![#(#props),*]);
                });
                name_hash
            }
        }
    }
}

fn type_def(fields: &[&Field]) -> TokenStream {
    let field_infos = fields.iter().map(|field| {
        let ty = &field.ty;
        let name = format!("{}", field.ident.as_ref().expect("should be field name"));
        quote! {
            fury_core::meta::FieldInfo::new(#name, <#ty as fury_core::serializer::Serializer>::ty(fury))
        }
    });
    quote! {
        fn type_def(fury: &fury_core::fury::Fury) -> Vec<u8> {
            fury_core::meta::TypeMeta::from_fields(
                0,
                vec![#(#field_infos),*]
            ).to_bytes().unwrap()
        }
    }
}

pub fn gen_in_struct_impl(fields: &[&Field]) -> TokenStream {
    let _hash_token_stream = hash(fields);
    let type_def_token_stream = type_def(fields);

    quote! {
        #type_def_token_stream
    }
}
pub fn gen() -> TokenStream {
    quote! {
            fn ty(fury: &fury_core::fury::Fury) -> i16 {
                fury.get_class_resolver().get_class_info(std::any::TypeId::of::<Self>()).get_type_id() as i16
            }
    }
}
