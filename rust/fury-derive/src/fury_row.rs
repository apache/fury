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

use crate::fury_meta::sorted_fields;

pub fn derive_row(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let fields = match &ast.data {
        syn::Data::Struct(s) => sorted_fields(&s.fields),
        _ => {
            panic!("only struct be supported")
        }
    };

    let write_exprs = fields.iter().map(|field| {
        let ty = &field.ty;
        let ident = field.ident.as_ref().expect("field should provide ident");

        quote! {
            if <#ty as fury::row::Row<'a>>::schema().is_container() {
                row_writer.write_offset_size(0);
                row_writer.point_to(<#ty as fury::row::Row<'a>>::schema().num_fields());
                <#ty as fury::row::Row<'a>>::write(&v.#ident, row_writer);
            } else {
                let size = <#ty as fury::row::Row<'a>>::write(&v.#ident, row_writer);
                row_writer.write_offset_size(size)
            }
        }
    });

    let getter_exprs = fields.iter().enumerate().map(|(index, field)| {
        let ty = &field.ty;
        let ident = field.ident.as_ref().expect("field should provide ident");
        let getter_name: proc_macro2::Ident =
            syn::Ident::new(&format!("get_{}", ident), ident.span());

        quote! {
            pub fn #getter_name(&self) -> <#ty as fury::row::Row<'a>>::ReadResult {
                if <#ty as fury::row::Row<'a>>::schema().is_container() {
                    let row_reader = self.row_reader.point_to(<#ty as fury::row::Row<'a>>::schema().num_fields(), self.row_reader.get_offset_size_absolute(#index).0 as usize);
                    <#ty as fury::row::Row<'a>>::read(#index, row_reader)
                } else {
                    <#ty as fury::row::Row<'a>>::read(#index, self.row_reader)
                }
            }
        }
    });

    let getter: proc_macro2::Ident =
        syn::Ident::new(&format!("{}FuryRowGetter", name), name.span());

    let num_fields = fields.len();

    let gen = quote! {
        struct #getter<'a> {
            row_reader: fury::row::RowReader<'a>
        }

        impl<'a> #getter<'a> {
            #(#getter_exprs)*
        }

        impl<'a> fury::row::Row<'a> for #name {

            type ReadResult = #getter<'a>;

            fn write(v: &Self, row_writer: &mut fury::row::RowWriter) -> usize {
                let start = row_writer.writer.len();
                #(#write_exprs);*;
                let end = row_writer.writer.len();
                end - start
            }

            fn read(idx: usize, row_reader: fury::row::RowReader<'a>) -> Self::ReadResult {
                #getter::<'a>{ row_reader }
            }

            fn schema() -> fury::row::Schema {
                fury::row::Schema::new(#num_fields, true)
            }
        }
    };
    gen.into()
}
