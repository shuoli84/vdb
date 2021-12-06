use proc_macro::{self, TokenStream};
use quote::quote;
use syn::{parse_macro_input, Attribute, Data, DataEnum, DataStruct, DeriveInput, Ident};

#[proc_macro_derive(Value, attributes(vdb_value))]
pub fn derive(input: TokenStream) -> TokenStream {
    let DeriveInput {
        ident, data, attrs, ..
    } = parse_macro_input!(input);

    match data {
        Data::Struct(data_struct) => process_struct(data_struct, ident, &attrs),
        Data::Enum(data_enum) => process_enum(data_enum, ident, &attrs),
        Data::Union(_) => {
            unimplemented!()
        }
    }
}

struct FieldOpt {
    index: u8,
}

fn process_struct(data_struct: DataStruct, ident: Ident, _attrs: &[Attribute]) -> TokenStream {
    // attrs::get_attrs_value(attrs, "vdb_value", "index").expect("index must specified");
    let fields = data_struct.fields;

    let mut field_and_opts = vec![];

    for field in fields {
        let index = attrs::get_attrs_value(&field.attrs, "vdb_value", "index")
            .expect("index must specified");
        let opt = FieldOpt {
            index: index.parse().expect("failed to parse index"),
        };
        field_and_opts.push((field, opt));
    }

    let mut fields_des_block = quote! {};
    let mut fields_ser_block = quote! {};

    for (field, opt) in field_and_opts.into_iter() {
        let ident = field.ident.unwrap();
        let index = opt.index;

        fields_des_block.extend(quote! {
            #index => {
                self.#ident.from_input(input)?;
            }
        });

        fields_ser_block.extend(quote! {
            output.write_field_header(self.#ident.ty(), #index);
            self.#ident.to_output(output);

        });
    }

    let output = quote! {
        impl vdb_value::Value for #ident {
            fn ty(&self) -> vdb_value::Ty {
                vdb_value::Ty::Struct
            }

            fn from_input(&mut self, input: &mut vdb_value::InputProtocol<'_>) -> Result<(), vdb_value::Error> {
                while let Some((ty, index)) = input.read_non_stop_field()? {
                    match index {
                        #fields_des_block
                        _ => {
                            input.skip_field(ty)?;
                        }
                    }
                }

                Ok(())
            }

            fn to_output(&self, output: &mut vdb_value::OutProtocol<'_>) {
                #fields_ser_block

                output.write_stop();
            }
        }
    };
    output.into()
}

fn process_enum(data_enum: DataEnum, ident: Ident, attrs: &[Attribute]) -> TokenStream {
    let _ = (data_enum, ident, attrs);
    unimplemented!()
}

mod attrs {
    use proc_macro2::TokenTree;
    use syn::Attribute;

    pub fn get_attrs_value(attrs: &[Attribute], group: &str, name: &str) -> Option<String> {
        for attr in attrs {
            if let Some(ident) = attr.path.get_ident() {
                if ident.to_string().eq(group) {
                    let mut iter = attr.tokens.clone().into_iter();
                    match iter.next()? {
                        TokenTree::Group(group) => {
                            let mut iter = group.stream().into_iter();

                            let (attr_name, _, value) = (iter.next()?, iter.next()?, iter.next()?);
                            if attr_name.to_string().as_str().eq(name) {
                                return Some(value.to_string());
                            }
                        }
                        _ => {
                            return None;
                        }
                    }
                }
            }
        }

        None
    }
}
