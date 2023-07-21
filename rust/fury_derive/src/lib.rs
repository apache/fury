use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(FuryMeta, attributes(tag))]
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
    derive_fury_meta(&input, tag)
}

fn derive_fury_meta(ast: &syn::DeriveInput, tag: String) -> TokenStream {
    let name = &ast.ident;
    let fields = match &ast.data {
        syn::Data::Struct(s) => &s.fields,
        _ => {
            panic!("only struct be supported")
        }
    };

    let props = fields.iter().map(|field| {
        let ty = &field.ty;
        quote! {
            (<#ty as fury::FuryMeta>::ty(), <#ty as fury::FuryMeta>::tag())
        }
    });
    let name_hash_static: proc_macro2::Ident =
        syn::Ident::new(&format!("HASH_{}", name).to_uppercase(), name.span());

    let gen = quote! {

        lazy_static::lazy_static! {
            static ref #name_hash_static: u32 = fury::compute_tag_hash(vec![#(#props),*]);
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

#[proc_macro_derive(Serialize, attributes(tag))]
pub fn proc_macro_derive_serialize(input: proc_macro::TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    derive_serialize(&input)
}

fn derive_serialize(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let fields = match &ast.data {
        syn::Data::Struct(s) => &s.fields,
        _ => {
            panic!("only struct be supported")
        }
    };

    let exprs = fields.iter().map(|field| {
        let ty = &field.ty;
        let ident = &field.ident;
        quote! {
            <#ty as fury::Serialize>::serialize(&self.#ident, serializer);
        }
    });

    let gen = quote! {
        impl fury::Serialize for #name {
            fn write(&self, serializer: &mut fury::SerializerState) {
                // write tag hash
                serializer.writer.u32(<#name as fury::FuryMeta>::hash());
                // write fields
                #(#exprs)*
            }
        }
    };
    gen.into()
}

#[proc_macro_derive(Deserialize, attributes(tag))]
pub fn proc_macro_derive_deserialize(input: proc_macro::TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    derive_deserilize(&input)
}

fn derive_deserilize(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let fields = match &ast.data {
        syn::Data::Struct(s) => &s.fields,
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
