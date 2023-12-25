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

pub fn derive_fury_meta(ast: &syn::DeriveInput, tag: String) -> TokenStream {
    let name = &ast.ident;
    let fields = match &ast.data {
        syn::Data::Struct(s) => sorted_fields(&s.fields),
        _ => {
            panic!("only struct be supported")
        }
    };
    let props = fields.iter().map(|field| {
        let ty = &field.ty;
        let name = format!("{}", field.ident.as_ref().expect("should be field name"));
        quote! {
            (#name, <#ty as fury::__derive::FuryMeta>::ty(), <#ty as fury::__derive::FuryMeta>::tag())
        }
    });
    let name_hash_static: proc_macro2::Ident =
        syn::Ident::new(&format!("HASH_{}", name).to_uppercase(), name.span());

    let gen = quote! {

        lazy_static::lazy_static! {
            static ref #name_hash_static: u32 = fury::__derive::compute_struct_hash(vec![#(#props),*]);
        }

        impl fury::__derive::FuryMeta for #name {
            fn tag() -> &'static str {
                #tag
            }

            fn hash() -> u32 {
                *(#name_hash_static)
            }

            fn ty() -> fury::__derive::FieldType {
                fury::__derive::FieldType::FuryTypeTag
            }
        }
    };
    gen.into()
}

pub fn derive_serialize(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let fields = match &ast.data {
        syn::Data::Struct(s) => sorted_fields(&s.fields),
        _ => {
            panic!("only struct be supported")
        }
    };

    let accessor_exprs = fields.iter().map(|field| {
        let ty = &field.ty;
        let ident = &field.ident;
        quote! {
            <#ty as fury::__derive::Serialize>::serialize(&self.#ident, serializer);
        }
    });

    let reserved_size_exprs = fields.iter().map(|field| {
        let ty = &field.ty;
        // each field have one byte ref tag and two byte type id
        quote! {
            <#ty as fury::__derive::Serialize>::reserved_space() + fury::__derive::SIZE_OF_REF_AND_TYPE
        }
    });

    let tag_bytelen = format!("{}", name).len();

    let gen = quote! {
        impl fury::__derive::Serialize for #name {
            fn write(&self, serializer: &mut fury::__derive::SerializerState) {
                // write tag string
                serializer.write_tag(<#name as fury::__derive::FuryMeta>::tag());
                // write tag hash
                serializer.writer.u32(<#name as fury::__derive::FuryMeta>::hash());
                // write fields
                #(#accessor_exprs)*
            }

            fn reserved_space() -> usize {
                // struct have four byte hash
                #tag_bytelen + 4 + #(#reserved_size_exprs)+*
            }
        }
    };
    gen.into()
}

pub fn derive_deserilize(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let fields = match &ast.data {
        syn::Data::Struct(s) => sorted_fields(&s.fields),
        _ => {
            panic!("only struct be supported")
        }
    };

    let exprs = fields.iter().map(|field| {
        let ty = &field.ty;
        let ident = &field.ident;
        quote! {
            #ident: <#ty as fury::__derive::Deserialize>::deserialize(deserializer)?
        }
    });

    let gen = quote! {
        impl<'de> fury::__derive::Deserialize for #name {
            fn read(deserializer: &mut fury::__derive::DeserializerState) -> Result<Self, fury::__derive::Error> {
                // read tag string
                deserializer.read_tag()?;
                // read tag hash
                let hash = deserializer.reader.u32();
                let expected = <#name as fury::__derive::FuryMeta>::hash();
                if(hash != expected) {
                    Err(fury::__derive::Error::StructHash{ expected, actial: hash })
                } else {
                    Ok(Self {
                        #(#exprs),*
                    })
                }
            }
        }
    };
    gen.into()
}
