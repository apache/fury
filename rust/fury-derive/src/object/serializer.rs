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

use crate::object::{misc, read, write};
use crate::util::sorted_fields;
use proc_macro::TokenStream;
use quote::quote;

pub fn derive_serializer(ast: &syn::DeriveInput, tag: &String) -> TokenStream {
    let name = &ast.ident;
    let fields = match &ast.data {
        syn::Data::Struct(s) => sorted_fields(&s.fields),
        _ => {
            panic!("only struct be supported")
        }
    };

    let misc_token_stream = misc::gen(name, &fields, tag);
    let write_token_stream = write::gen(name, &fields);
    let read_token_stream = read::gen(name, &fields);

    let gen = quote! {
        impl fury_core::types::FuryGeneralList for #name {}
        impl fury_core::serializer::Serializer for #name {
            #misc_token_stream
            #write_token_stream
            #read_token_stream
        }
    };
    gen.into()
}
