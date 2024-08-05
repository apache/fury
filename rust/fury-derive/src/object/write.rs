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
use quote::quote;
use syn::Field;

pub fn gen(name: &Ident, fields: &[&Field]) -> TokenStream {
    let accessor_expr = fields.iter().map(|field| {
        let ty = &field.ty;
        let ident = &field.ident;
        quote! {
            <#ty as fury_core::serializer::Serializer>::serialize(&self.#ident, context);
        }
    });

    let reserved_size_expr = fields.iter().map(|field| {
        let ty = &field.ty;
        // each field have one byte ref tag and two byte type id
        quote! {
            <#ty as fury_core::serializer::Serializer>::reserved_space() + fury_core::types::SIZE_OF_REF_AND_TYPE
        }
    });

    let tag_byte_len = format!("{}", name).len();

    quote! {
        fn serialize(&self, context: &mut fury_core::resolvers::context::WriteContext) {
            match context.get_fury().get_mode() {
                fury_core::types::Mode::SchemaConsistent => {
                    fury_core::serializer::serialize(self, context);
                },
                fury_core::types::Mode::Compatible => {
                    context.writer.i8(fury_core::types::RefFlag::NotNullValue as i8);
                    let meta_index = context.push_meta(
                            std::any::TypeId::of::<Self>(),
                            #name::fury_type_def()
                        ) as i16;
                    context.writer.i16(meta_index);
                    self.write(context);
                }
            }
        }


        fn write(&self, context: &mut fury_core::resolvers::context::WriteContext) {
            // write tag string
            context.write_tag(#name::fury_tag());
            // write tag hash
            context.writer.u32(#name::fury_hash());
            // write fields
            #(#accessor_expr)*
        }

        fn reserved_space() -> usize {
            // struct have four byte hash
            #tag_byte_len + 4 + #(#reserved_size_expr)+*
        }
    }
}
