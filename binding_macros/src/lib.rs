use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::meta::ParseNestedMeta;
use syn::{parse_macro_input, Attribute, FnArg, ItemFn, Lifetime, Pat};

use syn::fold::{self, Fold};
use syn::{AngleBracketedGenericArguments, GenericArgument, Type, TypeReference};

/// Name of big data types, which are handled over app state instead of being (de-)serialized
const BIG_TYPES_NAMES: &[&str] = &["EventLogActivityProjection", "IndexLinkedOCEL", "EventLog"];

/// Removes/elide lifetimes and other special cases (i.e., certain generics) from types
struct LifetimeStripper;

impl Fold for LifetimeStripper {
    /// Remove/elide lifetimes from type references `&'a T -> &'_ T`
    fn fold_type_reference(&mut self, mut node: TypeReference) -> TypeReference {
        // Replace lifetime with placeholder (_)
        node.lifetime = Some(syn::Lifetime::new("'_", proc_macro2::Span::call_site()));
        // Recurse
        fold::fold_type_reference(self, node)
    }

    /// Remove/elide lifetimes from generic structs `MyStruct<'a, T> -> MyStruct<'_, T>`
    fn fold_angle_bracketed_generic_arguments(
        &mut self,
        mut node: AngleBracketedGenericArguments,
    ) -> AngleBracketedGenericArguments {
        // Modify all lifetime arguments
        node.args = node
            .args
            .into_iter()
            .map(|arg| {
                if matches!(arg, GenericArgument::Lifetime(_)) {
                    GenericArgument::Lifetime(Lifetime::new("'_", proc_macro2::Span::call_site()))
                } else {
                    arg
                }
            })
            .collect();

        // Recurse
        fold::fold_angle_bracketed_generic_arguments(self, node)
    }
    /// Handle `impl Trait` types specially
    fn fold_type(&mut self, ty: Type) -> Type {
        if let Type::ImplTrait(it) = &ty {
            if it.bounds.len() != 1 {
                return fold::fold_type(self, ty);
            }
            if let Some(syn::TypeParamBound::Trait(really_it)) = it.bounds.first() {
                let really_it_str = quote::quote!(#really_it).to_string();
                let ret = match really_it_str.as_str() {
                    "AsRef < Path >"
                    | "AsRef < std :: path :: Path >"
                    | "AsRef < path :: Path >" => {
                        syn::parse_quote!(std::path::PathBuf)
                    }
                    "AsRef < str >" => syn::parse_quote!(String),
                    _ => {
                        return fold::fold_type(self, ty);
                    }
                };
                return ret;
            };
        }
        fold::fold_type(self, ty)
    }
}

/// Strip lifetimes: Helper function to use in your main macro logic
fn strip_lifetimes(ty: Type) -> Type {
    let mut stripper = LifetimeStripper;
    stripper.fold_type(ty)
}

fn is_big_type_ref(ty: &Type) -> bool {
    if matches!(ty, Type::Reference(_)) {
        let ty_str = quote::quote!(#ty).to_string();
        BIG_TYPES_NAMES.iter().any(|tn| ty_str.ends_with(tn))
    } else {
        false
    }
}

fn is_big_type(ty: &Type) -> Option<String> {
    let ty_str = quote::quote!(#ty).to_string();
    BIG_TYPES_NAMES
        .iter()
        .find(|tn| ty_str.ends_with(**tn))
        .map(|s| s.to_string())
}

#[derive(Default)]
struct RegisterBindingAttrs {
    stringify_error: bool,
    debug_output: bool,
    custom_name: Option<String>,
}

impl RegisterBindingAttrs {
    fn parse(&mut self, meta: ParseNestedMeta) -> syn::parse::Result<()> {
        if meta.path.is_ident("debug_output") {
            self.debug_output = true;
        } else if meta.path.is_ident("stringify_error") {
            self.stringify_error = true;
        } else if meta.path.is_ident("name") {
            let value: syn::LitStr = meta.value()?.parse()?;
            self.custom_name = Some(value.value());
        }
        Ok(())
    }
}

struct ArgOptions {
    default_value: Option<syn::Expr>,
}

fn parse_arg_attributes(attrs: &[Attribute]) -> ArgOptions {
    let mut opts = ArgOptions {
        default_value: None,
    };
    for attr in attrs {
        if attr.path().is_ident("bind") {
            let _ = attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("default") {
                    if meta.input.peek(syn::Token![=]) {
                        let expr: syn::Expr = meta.value()?.parse()?;
                        opts.default_value = Some(expr);
                    } else {
                        opts.default_value = Some(syn::parse_quote!(Default::default()));
                    }
                }
                Ok(())
            });
        }
    }
    opts
}

#[proc_macro_attribute]
pub fn register_binding(args: TokenStream, item: TokenStream) -> TokenStream {
    let mut input_fn = parse_macro_input!(item as ItemFn);
    let fn_ident = &input_fn.sig.ident;

    let mut attrs = RegisterBindingAttrs::default();
    let attr_parser = syn::meta::parser(|meta| attrs.parse(meta));
    parse_macro_input!(args with attr_parser);

    let binding_name_str = attrs.custom_name.unwrap_or_else(|| fn_ident.to_string());
    let wrapper_name = format_ident!("{}_wrapper", fn_ident);

    let docs: Vec<String> = input_fn
        .attrs
        .iter()
        .filter(|attr| attr.path().is_ident("doc"))
        .filter_map(|attr| match &attr.meta {
            syn::Meta::NameValue(syn::MetaNameValue {
                value:
                    syn::Expr::Lit(syn::ExprLit {
                        lit: syn::Lit::Str(s),
                        ..
                    }),
                ..
            }) => Some(s.value()),
            _ => None,
        })
        .flat_map(|s| {
            s.lines()
                .map(|l| l.strip_prefix(' ').unwrap_or(l).to_string())
                .collect::<Vec<_>>()
        })
        .collect();

    let args_info: Vec<_> = input_fn
        .sig
        .inputs
        .iter()
        .map(|arg| match arg {
            FnArg::Typed(pat_type) => {
                let pat = &pat_type.pat;
                let ty = &pat_type.ty;

                let arg_opts = parse_arg_attributes(&pat_type.attrs);

                let arg_name = match &**pat {
                    Pat::Ident(p) => p.ident.to_string(),
                    _ => panic!("Simple args only"),
                };

                let ty_no_life = strip_lifetimes(*ty.clone());
                let ty_as_str = quote::quote!(#ty_no_life).to_string();
                let change_from_ref = matches!(ty_no_life, Type::Reference(_))
                    && !(BIG_TYPES_NAMES.iter().any(|tn| ty_as_str.ends_with(tn)));
                let type_without_ref = match &ty_no_life {
                    Type::Reference(type_reference) if change_from_ref => {
                        *type_reference.elem.clone()
                    }
                    x => x.clone(),
                };
                (
                    arg_name,
                    ty_no_life,
                    change_from_ref,
                    type_without_ref,
                    arg_opts,
                )
            }
            _ => panic!("Self not supported"),
        })
        .collect();

    // 1. Extraction Logic
    let extractions = args_info.iter().map(|(name, _ty, is_ref, ty_without_ref, opts)| {
        let maybe_ref = if *is_ref {
            quote! {&}
        } else {
            quote! {}
        };
        if let Some(default_expr) = &opts.default_value {
            quote! {
                #maybe_ref crate::bindings::extract_param::<#ty_without_ref>(arg_map, #name, state)
                    .unwrap_or_else(|_| #default_expr)
            }
        } else {
            quote! {
                #maybe_ref crate::bindings::extract_param::<#ty_without_ref>(arg_map, #name, state)?
            }
        }
    });

    // 2. Schema Logic
    let schema_gens = args_info.iter().map(|(name, _ty, _is_ref, ty_without_ref, _)| {
        if is_big_type_ref(ty_without_ref) {
             let ty_str = quote::quote!(#ty_without_ref).to_string();
             let type_name = BIG_TYPES_NAMES.iter().find(|tn| ty_str.ends_with(**tn)).unwrap();
             quote! {
                 args_schema.push((#name.to_string(), serde_json::json!({
                    "type": "string",
                    "title": #type_name,
                    "x-registry-ref": #type_name,
                    "x-widget": "entity-selector"
                })));
             }
        } else {
            quote! { args_schema.push((#name.to_string(), serde_json::to_value(schemars::schema_for!(#ty_without_ref)).unwrap())); }
        }
    });

    // 3. Return Type Schema Logic
    let raw_ret_type = match &input_fn.sig.output {
        syn::ReturnType::Default => syn::parse_quote!(()), // Handle "void" -> unit type
        syn::ReturnType::Type(_, ty) => *ty.clone(),
    };

    // Strip lifetimes from return type
    let mut ret_type = strip_lifetimes(raw_ret_type);

    // If debug_output is set, the actual return type is String
    if attrs.debug_output {
        ret_type = syn::parse_quote!(String);
    } else if attrs.stringify_error {
        // If stringify_error is set, we expect a Result<T, E> (or io::Result<T>).
        // We need to transform the schema type to Result<T, String>.
        if let Type::Path(tp) = &ret_type {
            if let Some(segment) = tp.path.segments.last() {
                // Heuristic: If it looks like a Result (std or io), grab the first generic arg (Ok type)
                if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                    if let Some(ok_type) = args.args.first() {
                        ret_type = syn::parse_quote!(::std::result::Result<#ok_type, String>);
                    }
                }
            }
        }
    }

    let required_arg_names = args_info
        .iter()
        .filter(|(_, _, _, _, opts)| opts.default_value.is_none())
        .map(|(name, _, _, _, _)| name);

    // 4. Generate the Execution Logic
    let execution_block = if let Some(type_name) = is_big_type(&ret_type) {
        let variant_ident = format_ident!("{}", type_name);
        quote! {
            let result = {
                let state_guard = state_lock.items.read().map_err(|e| e.to_string())?;
                let state = &*state_guard;
                #fn_ident( #(#extractions),* )
            };
            let id = format!("res_{}", uuid::Uuid::new_v4());
            state_lock.add(&id, crate::bindings::RegistryItem::#variant_ident(result));
            serde_json::to_value(id).map_err(|e| e.to_string())
        }
    } else {
        let serialization_logic = if attrs.debug_output {
            quote! {
                let final_result = format!("{:?}", result);
                serde_json::to_value(final_result).map_err(|e| e.to_string())
            }
        } else if attrs.stringify_error {
            quote! {
                let final_result = result.map_err(|e| e.to_string());
                serde_json::to_value(final_result).map_err(|e| e.to_string())
            }
        } else {
            quote! {
                serde_json::to_value(result).map_err(|e| e.to_string())
            }
        };

        quote! {
            let state_guard = state_lock.items.read().map_err(|e| e.to_string())?;
            let state = &*state_guard;
            let result = #fn_ident( #(#extractions),* );
            #serialization_logic
        }
    };

    let ret_type_schema = if let Some(type_name) = is_big_type(&ret_type) {
        quote! {
            serde_json::json!({
               "type": "string",
               "title": #type_name,
               "x-registry-ref": #type_name,
               "x-widget": "entity-selector"
           })
        }
    } else {
        quote! {
            serde_json::to_value(schemars::schema_for!(#ret_type)).unwrap()
        }
    };

    // Strip #[bind] attributes
    for input in &mut input_fn.sig.inputs {
        if let FnArg::Typed(pat_type) = input {
            pat_type.attrs.retain(|attr| !attr.path().is_ident("bind"));
        }
    }

    let docs_fn_name = format_ident!("{}_docs", fn_ident);
    let args_fn_name = format_ident!("{}_args", fn_ident);
    let required_args_fn_name = format_ident!("{}_required_args", fn_ident);
    let return_type_fn_name = format_ident!("{}_return_type", fn_ident);

    let expanded = quote! {
        #input_fn

        #[cfg(feature = "bindings")]
        const _: () = {
            use crate::bindings::{Binding, AppState};
            use serde_json::Value;
            use std::sync::RwLock;

            fn #wrapper_name(args: &Value, state_lock: &AppState) -> Result<Value, String> {
                let arg_map = args.as_object().ok_or("Args must be JSON object")?;
                #execution_block
            }

            fn #docs_fn_name() -> Vec<String> {
                vec![#(#docs.to_string(),)*]
            }

            fn #args_fn_name() -> Vec<(String, Value)> {
                let mut args_schema = ::std::vec::Vec::new();
                #(#schema_gens)*
                args_schema
            }

            fn #required_args_fn_name() -> Vec<String> {
                vec![#(#required_arg_names.to_string(),)*]
            }

            fn #return_type_fn_name() -> Value {
                #ret_type_schema
            }

            inventory::submit! {
                Binding {
                    id: concat!(module_path!(), "::", stringify!(#fn_ident)),
                    name: #binding_name_str,
                    handler: #wrapper_name,
                    docs: #docs_fn_name,
                    module: module_path!(),
                    source_path: file!(),
                    source_line: line!(),
                    args: #args_fn_name,
                    required_args: #required_args_fn_name,
                    return_type: #return_type_fn_name,
                }
            }
        };
    };
    TokenStream::from(expanded)
}

/// Returns the list of "Big Types" known to the macro crate as a static string slice array.
/// Used for consistency testing.
#[proc_macro]
pub fn big_types_list(_item: TokenStream) -> TokenStream {
    let types = BIG_TYPES_NAMES;
    let expanded = quote! {
        &[#(#types),*]
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
            fn from_context(value: &serde_json::Value, state: &'a crate::bindings::InnerAppState) -> Result<Self, String> {
                let id = value.as_str().ok_or("Expected String ID")?;
                let item = state.get(id).ok_or_else(|| format!("Item '{}' not found", id))?;

                if let crate::bindings::RegistryItem::#name(inner) = item {
                    Ok(inner)
                } else {
                    Err(format!("ID '{}' is not a {}", id, #name_str))
                }
            }
        }
    };
    TokenStream::from(expanded)
}
