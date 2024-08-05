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
        fn hash() -> u32 {
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
            fury_core::meta::FieldInfo::new(#name, <#ty as fury_core::serializer::Serializer>::ty())
        }
    });
    quote! {
        fn type_def() -> &'static [u8] {
            use std::sync::Once;
            static mut type_definition: Vec<u8> = Vec::new();
            static type_definition_once: Once = Once::new();
            unsafe {
                type_definition_once.call_once(|| {
                    type_definition = fury_core::meta::TypeMeta::from_fields(
                        0,
                        vec![#(#field_infos),*]
                    ).to_bytes().unwrap();
                });
                type_definition.as_slice()
            }
        }
    }
}

pub fn gen(fields: &[&Field], tag: &String) -> TokenStream {
    let hash_token_stream = hash(fields);

    let type_def_token_stream = type_def(fields);

    quote! {
            #hash_token_stream

            #type_def_token_stream

            fn tag() -> &'static str {
                #tag
            }

            fn ty() -> fury_core::types::FieldType {
                fury_core::types::FieldType::FuryTypeTag
            }
    }
}
