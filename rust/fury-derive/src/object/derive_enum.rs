use proc_macro2::TokenStream;
use quote::quote;
use syn::DataEnum;

pub fn gen_type_def(_data_enum: &DataEnum) -> TokenStream {
    quote! {
        fn type_def(fury: &fury_core::fury::Fury) -> Vec<u8> {
            Vec::new()
        }
    }
}

pub fn gen_write(data_enum: &DataEnum) -> TokenStream {
    let variant_idents: Vec<_> = data_enum.variants.iter().map(|v| &v.ident).collect();
    let variant_values: Vec<_> = (0..variant_idents.len()).map(|v| v as i32).collect();

    quote! {
        fn write(&self, context: &mut fury_core::resolver::context::WriteContext) {
            match self {
                #(
                    Self::#variant_idents => {
                        context.writer.var_int32(#variant_values);
                    }
                )*
            }
        }

        fn reserved_space() -> usize {
            4
        }
    }
}

pub fn gen_read(data_enum: &DataEnum) -> TokenStream {
    let variant_idents: Vec<_> = data_enum.variants.iter().map(|v| &v.ident).collect();
    let variant_values: Vec<_> = (0..variant_idents.len()).map(|v| v as i32).collect();

    quote! {
       fn read(
           context: &mut fury_core::resolver::context::ReadContext,
       ) -> Result<Self, fury_core::error::Error> {
           let v = context.reader.var_int32();
           match v {
               #(
                   #variant_values => Ok(Self::#variant_idents),
               )*
               _ => panic!("unknown value"),
           }
       }
    }
}
