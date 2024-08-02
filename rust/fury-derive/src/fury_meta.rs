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

use proc_macro::TokenStream;
use quote::quote;
use syn::{Field, Fields};

pub fn sorted_fields(fields: &Fields) -> Vec<&Field> {
    let mut fields = fields.iter().collect::<Vec<&Field>>();
    fields.sort_by(|a, b| a.ident.cmp(&b.ident));
    fields
}

pub fn derive_serializer(ast: &syn::DeriveInput, tag: &String) -> TokenStream {
    let name = &ast.ident;
    let fields = match &ast.data {
        syn::Data::Struct(s) => sorted_fields(&s.fields),
        _ => {
            panic!("only struct be supported")
        }
    };

    let name_hash: proc_macro2::Ident =
        syn::Ident::new(&format!("HASH_{}", name).to_uppercase(), name.span());

    let field_infos = fields.iter().map(|field| {
        let ty = &field.ty;
        let name = format!("{}", field.ident.as_ref().expect("should be field name"));
        quote! {
            fury_core::meta::FieldInfo::new(#name, <#ty as fury_core::serializer::Serializer>::ty())
        }
    });

    let accessor_exprs = fields.iter().map(|field| {
        let ty = &field.ty;
        let ident = &field.ident;
        quote! {
            <#ty as fury_core::serializer::Serializer>::serialize(&self.#ident, serializer);
        }
    });

    let reserved_size_exprs = fields.iter().map(|field| {
        let ty = &field.ty;
        // each field have one byte ref tag and two byte type id
        quote! {
            <#ty as fury_core::serializer::Serializer>::reserved_space() + fury_core::types::SIZE_OF_REF_AND_TYPE
        }
    });

    let tag_bytelen = format!("{}", name).len();

    let exprs = fields.iter().map(|field| {
        let ty = &field.ty;
        let ident = &field.ident;
        quote! {
            #ident: <#ty as fury_core::serializer::Serializer>::deserialize(state)?
        }
    });

    let props = fields.iter().map(|field| {
        let ty = &field.ty;
        let name = format!("{}", field.ident.as_ref().expect("should be field name"));
        quote! {
            (#name, <#ty as fury_core::serializer::Serializer>::ty())
        }
    });

    let gen = quote! {
        impl fury_core::types::FuryGeneralList for #name {}
        impl fury_core::serializer::Serializer for #name {
            fn tag() -> &'static str {
                #tag
            }

            fn hash() -> u32 {
                lazy_static::lazy_static! {
                    static ref #name_hash: u32 = fury_core::types::compute_struct_hash(vec![#(#props),*]);
                }
                *(#name_hash)
            }

            fn type_def() -> &'static [u8] {
                lazy_static::lazy_static! {
                    static ref type_definition: Vec<u8> = fury_core::meta::TypeMeta::from_fields(
                        0,
                        vec![#(#field_infos),*]
                    ).to_bytes().unwrap();
                }
                type_definition.as_slice()
            }

            fn ty() -> fury_core::types::FieldType {
                fury_core::types::FieldType::FuryTypeTag
            }

            fn write(&self, serializer: &mut fury_core::write_state::WriteState) {
                // write tag string
                serializer.write_tag(<#name as fury_core::serializer::Serializer>::tag());
                // write tag hash
                serializer.writer.u32(<#name as fury_core::serializer::Serializer>::hash());
                // write fields
                #(#accessor_exprs)*
            }

            fn read(state: &mut fury_core::read_state::ReadState) -> Result<Self, fury_core::error::Error> {
                // read tag string
                state.read_tag()?;
                // read tag hash
                let hash = state.reader.u32();
                let expected = <#name as fury_core::serializer::Serializer>::hash();
                if(hash != expected) {
                    Err(fury_core::error::Error::StructHash{ expected, actial: hash })
                } else {
                    Ok(Self {
                        #(#exprs),*
                    })
                }
            }

            fn reserved_space() -> usize {
                // struct have four byte hash
                #tag_bytelen + 4 + #(#reserved_size_exprs)+*
            }
        }
    };
    gen.into()
}
