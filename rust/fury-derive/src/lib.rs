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

use fury_meta::{derive_deserilize, derive_fury_meta, derive_serialize};
use fury_row::derive_row;
use proc_macro::TokenStream;
use syn::{parse_macro_input, DeriveInput};

mod fury_meta;
mod fury_row;

#[proc_macro_derive(Fury, attributes(tag))]
pub fn proc_macro_derive_fury_meta(input: proc_macro::TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let tag = input
        .attrs
        .iter()
        .find(|attr| attr.path().is_ident("tag"))
        .expect("should have tag");
    let expr: syn::ExprLit = tag.parse_args().expect("should tag contain string value");
    let tag = match expr.lit {
        syn::Lit::Str(s) => s.value(),
        _ => {
            panic!("tag should be string")
        }
    };
    let mut token_stream = derive_fury_meta(&input, tag);
    // append serialize impl
    token_stream.extend(derive_serialize(&input));
    // append deserialize impl
    token_stream.extend(derive_deserilize(&input));
    token_stream
}

#[proc_macro_derive(FuryRow)]
pub fn proc_macro_derive_fury_row(input: proc_macro::TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    derive_row(&input)
}
