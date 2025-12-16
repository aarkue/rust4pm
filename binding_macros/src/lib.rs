use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_macro_input, FnArg, ItemFn, Pat};

#[proc_macro_attribute]
pub fn register_binding(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);
    let fn_name = &input_fn.sig.ident;
    let wrapper_name = format_ident!("{}_wrapper", fn_name);

    let args_info: Vec<_> = input_fn
        .sig
        .inputs
        .iter()
        .map(|arg| match arg {
            FnArg::Typed(pat_type) => {
                let pat = &pat_type.pat;
                let ty = &pat_type.ty;
                let arg_name = match &**pat {
                    Pat::Ident(p) => p.ident.to_string(),
                    _ => panic!("Simple args only"),
                };
                (arg_name, ty)
            }
            _ => panic!("Self not supported"),
        })
        .collect();

    // 1. Extraction Logic
    let extractions = args_info.iter().map(|(name, ty)| {
        quote! { crate::bindings::extract_param::<#ty>(arg_map, #name, state)? }
    });

    // 2. Schema Logic
    let schema_gens = args_info.iter().map(|(name, ty)| {
        quote! { args_schema.insert(#name.to_string(), <#ty as crate::bindings::SchemaProvider>::get_schema_gen()); }
    });

    let arg_names = args_info.iter().map(|(name, _)| name);

    let expanded = quote! {
        #input_fn

        #[cfg(feature = "bindings")]
        const _: () = {
            use crate::bindings::{Binding, AppState};
            use serde_json::Value;

            fn #wrapper_name(args: &Value, state: &AppState) -> Result<Value, String> {
                let arg_map = args.as_object().ok_or("Args must be JSON object")?;
                let result = #fn_name( #(#extractions),* );
                serde_json::to_value(result).map_err(|e| e.to_string())
            }

            inventory::submit! {
                Binding {
                    name: stringify!(#fn_name),
                    handler: #wrapper_name,
                    args: || {

                        let mut args_schema = ::std::collections::HashMap::new();
                        #(#schema_gens)*
                        args_schema
                    },
                    schema: || {
                        // let mut args_schema = serde_json::Map::new();
                        // #(#schema_gens)*
                        serde_json::json!({
                            "type": "object",
                            "title": stringify!(#fn_name),
                            // "args_schema": args_schema,
                            "required": vec![ #( #arg_names ),* ]
                        })
                    }
                }
            }
        };
    };
    TokenStream::from(expanded)
}

#[proc_macro_derive(RegistryEntity)]
pub fn derive_registry_entity(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as syn::DeriveInput);
    let name = &input.ident;
    let name_str = name.to_string();

    let expanded = quote! {
        #[cfg(feature = "bindings")]
        impl<'a> crate::bindings::FromContext<'a> for &'a #name {
            fn from_context(value: &serde_json::Value, state: &'a crate::bindings::AppState) -> Result<Self, String> {
                let id = value.as_str().ok_or("Expected String ID")?;
                let item = state.items.get(id).ok_or_else(|| format!("Item '{}' not found", id))?;

                // Matches the Enum Variant (Manual Maintenance approach)
                if let crate::bindings::RegistryItem::#name(inner) = item {
                    Ok(inner)
                } else {
                    Err(format!("ID '{}' is not a {}", id, #name_str))
                }
            }
        }

        #[cfg(feature = "bindings")]
        impl<'a> crate::bindings::SchemaProvider for &'a #name {
            fn get_schema_gen() -> serde_json::Value {
                serde_json::json!({
                    "type": "string",
                    "title": #name_str,
                    "x-registry-ref": #name_str,
                    "x-widget": "entity-selector"
                })
            }
        }
    };
    TokenStream::from(expanded)
}
