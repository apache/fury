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

use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};
use syn::Field;

fn create_private_field_name(field: &Field) -> Ident {
    format_ident!("_{}", field.ident.as_ref().expect(""))
}

fn bind(fields: &[&Field]) -> Vec<TokenStream> {
    fields
        .iter()
        .map(|field| {
            let ty = &field.ty;
            let var_name = create_private_field_name(field);
            quote! {
                let mut #var_name: Option<#ty> = None;
            }
        })
        .collect()
}

fn create(fields: &[&Field]) -> Vec<TokenStream> {
    fields
        .iter()
        .map(|field| {
            let name = &field.ident;
            let var_name = create_private_field_name(field);
            quote! {
                #name: #var_name.unwrap()
            }
        })
        .collect()
}

fn read(fields: &[&Field]) -> TokenStream {
    let assign_stmt = fields.iter().map(|field| {
        let ty = &field.ty;
        let name = &field.ident;
        quote! {
            #name: <#ty as fory_core::serializer::Serializer>::deserialize(context)?
        }
    });

    quote! {
        fn read(context: &mut fory_core::resolver::context::ReadContext) -> Result<Self, fory_core::error::Error> {
            Ok(Self {
                #(#assign_stmt),*
            })
        }
    }
}

fn deserialize_compatible(fields: &[&Field]) -> TokenStream {
    let pattern_item = fields.iter().enumerate().map(|(index, field)| {
        let ty = &field.ty;
        let var_name = create_private_field_name(field);
        quote! {
            #index => {
                #var_name = Some(<#ty as fory_core::serializer::Serializer>::deserialize(context)?);
            }
        }
    });
    let bind: Vec<TokenStream> = bind(fields);
    let create: Vec<TokenStream> = create(fields);
    quote! {
        let ref_flag = context.reader.i8();
        if ref_flag == (fory_core::types::RefFlag::NotNullValue as i8) || ref_flag == (fory_core::types::RefFlag::RefValue as i8) {
            let meta_index = context.reader.i16() as usize;
            let meta = context.get_meta(meta_index).clone();
            let fields = meta.get_field_info();
            #(#bind)*
            for (idx, _field_info) in fields.iter().enumerate() {
                match idx {
                    #(#pattern_item),*
                    _ => {
                        panic!("not implement yet");
                    }
                }
            }
            Ok(Self {
                #(#create),*
            })
        } else if ref_flag == (fory_core::types::RefFlag::Null as i8) {
            Err(fory_core::error::AnyhowError::msg("Try to deserialize non-option type to null"))?
        } else if ref_flag == (fory_core::types::RefFlag::Ref as i8) {
            Err(fory_core::error::Error::Ref)
        } else {
            Err(fory_core::error::AnyhowError::msg("Unknown ref flag, value:{ref_flag}"))?
        }
    }
}

pub fn gen(fields: &[&Field]) -> TokenStream {
    let read_token_stream = read(fields);
    let compatible_token_stream = deserialize_compatible(fields);

    quote! {
        fn deserialize(context: &mut fory_core::resolver::context::ReadContext) -> Result<Self, fory_core::error::Error> {
            match context.get_fory().get_mode() {
                fory_core::types::Mode::SchemaConsistent => {
                    fory_core::serializer::deserialize::<Self>(context)
                },
                fory_core::types::Mode::Compatible => {
                    #compatible_token_stream
                }
            }
        }
        #read_token_stream
    }
}
