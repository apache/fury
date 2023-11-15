// Copyright 2023 The Fury Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, Field, Fields};

fn sorted_fields(fields: &Fields) -> Vec<&Field> {
    let mut fields = fields.iter().collect::<Vec<&Field>>();
    fields.sort_by(|a, b| a.ident.cmp(&b.ident));
    fields
}

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


fn derive_fury_meta(ast: &syn::DeriveInput, tag: String) -> TokenStream {
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
            (#name, <#ty as fury::FuryMeta>::ty(), <#ty as fury::FuryMeta>::tag())
        }
    });
    let name_hash_static: proc_macro2::Ident =
        syn::Ident::new(&format!("HASH_{}", name).to_uppercase(), name.span());

    let gen = quote! {

        lazy_static::lazy_static! {
            static ref #name_hash_static: u32 = fury::compute_struct_hash(vec![#(#props),*]);
        }

        impl fury::FuryMeta for #name {
            fn tag() -> &'static str {
                #tag
            }

            fn hash() -> u32 {
                *(#name_hash_static)
            }

            fn ty() -> fury::FieldType {
                fury::FieldType::FuryTypeTag
            }
        }
    };
    gen.into()
}


fn derive_serialize(ast: &syn::DeriveInput) -> TokenStream {
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
            <#ty as fury::Serialize>::serialize(&self.#ident, serializer);
        }
    });

    let reserved_size_exprs = fields.iter().map(|field| {
        let ty = &field.ty;
        // each field have one byte ref tag and two byte type id
        quote! {
            <#ty as fury::Serialize>::reserved_space() + 3
        }
    });

    let tag_bytelen = format!("{}", name).len();

    let gen = quote! {
        impl fury::Serialize for #name {
            fn write(&self, serializer: &mut fury::SerializerState) {
                // write tag string
                serializer.write_tag(<#name as fury::FuryMeta>::tag());
                // write tag hash
                serializer.writer.u32(<#name as fury::FuryMeta>::hash());
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


fn derive_deserilize(ast: &syn::DeriveInput) -> TokenStream {
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
            #ident: <#ty as fury::Deserialize>::deserialize(deserializer)?
        }
    });

    let gen = quote! {
        impl<'de> fury::Deserialize for #name {
            fn read(deserializer: &mut fury::DeserializerState) -> Result<Self, fury::Error> {
                // read tag string
                deserializer.read_tag()?;
                // read tag hash
                let hash = deserializer.reader.u32();
                let expected = <#name as fury::FuryMeta>::hash();
                if(hash != expected) {
                    Err(fury::Error::StructHash{ expected, actial: hash })
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

