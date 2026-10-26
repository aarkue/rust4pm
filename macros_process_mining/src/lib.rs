use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::meta::ParseNestedMeta;
use syn::{parse_macro_input, Attribute, FnArg, ItemFn, Lifetime, Pat};

use syn::fold::{self, Fold};
use syn::{AngleBracketedGenericArguments, GenericArgument, Type, TypeReference};

/// The optional argument added to bindings whose result is stored in the registry, letting the
/// caller pick the id instead of receiving a generated one.
const OUTPUT_ID_ARG: &str = "output_id";

/// Name of big data types, which are handled over app state instead of being (de-)serialized
const BIG_TYPES_NAMES: &[&str] = &[
    "EventLog",
    "OCEL",
    "EventLogActivityProjection",
    "SlimLinkedOCEL",
    "IndexLinkedOCEL",
    "TabularSource",
];

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
                    "LinkedOCELAccess < 'a >" => syn::parse_quote!(
                        crate::core::event_data::object_centric::linked_ocel::SlimLinkedOCEL
                    ),
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

/// Find the longest matching big type name that is a suffix of the given string.
///
/// Using the longest match avoids ambiguity when one type name is a suffix of another
/// (e.g., "OCEL" is a suffix of "SlimLinkedOCEL").
fn longest_big_type_match(s: &str) -> Option<String> {
    BIG_TYPES_NAMES
        .iter()
        .filter(|tn| s.ends_with(**tn))
        .max_by_key(|tn| tn.len())
        .map(|s| s.to_string())
}

fn is_big_type_ref(ty: &Type) -> bool {
    if matches!(ty, Type::Reference(_)) {
        let ty_str = quote::quote!(#ty).to_string();
        longest_big_type_match(&ty_str).is_some()
    } else {
        false
    }
}

fn is_big_type(ty: &Type) -> Option<String> {
    let ty_str = quote::quote!(#ty).to_string();
    longest_big_type_match(&ty_str)
}

/// Check if a type is a mutable reference to a big type (e.g., `&mut SlimLinkedOCEL`)
/// Returns the big type name if it is.
fn is_mut_big_type_ref(ty: &Type) -> Option<String> {
    if let Type::Reference(TypeReference {
        mutability: Some(_),
        elem,
        ..
    }) = ty
    {
        let elem_str = quote::quote!(#elem).to_string();
        longest_big_type_match(&elem_str)
    } else {
        None
    }
}

#[derive(Default)]
struct RegisterBindingAttrs {
    stringify_error: bool,
    debug_output: bool,
    custom_name: Option<String>,
    returns_handle: bool,
}

impl RegisterBindingAttrs {
    fn parse(&mut self, meta: ParseNestedMeta) -> syn::parse::Result<()> {
        if meta.path.is_ident("debug_output") {
            self.debug_output = true;
        } else if meta.path.is_ident("stringify_error") {
            self.stringify_error = true;
        } else if meta.path.is_ident("returns_handle") {
            self.returns_handle = true;
        } else if meta.path.is_ident("name") {
            let value: syn::LitStr = meta.value()?.parse()?;
            self.custom_name = Some(value.value());
        } else {
            return Err(meta.error(
                "unknown #[register_binding] option, expected one of `debug_output`, \
                 `stringify_error`, `returns_handle`, `name = \"..\"`",
            ));
        }
        Ok(())
    }
}

struct ArgOptions {
    default_value: Option<syn::Expr>,
    /// `#[bind(state)]`: not a JSON argument but a read-only [`StateRef`] over the registry
    is_state: bool,
    /// `#[bind(state_mut)]`: not a JSON argument but a writable [`StateRefMut`] over the
    /// registry. Unlike `state`, this forces the write lock for the whole call.
    is_state_mut: bool,
    /// `#[bind(handle)]`: a reference to a `CustomRegistryValue`, passed as a registry id.
    is_handle: bool,
}

fn parse_arg_attributes(attrs: &[Attribute]) -> syn::parse::Result<ArgOptions> {
    let mut opts = ArgOptions {
        default_value: None,
        is_state: false,
        is_state_mut: false,
        is_handle: false,
    };
    for attr in attrs {
        if attr.path().is_ident("bind") {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("state") {
                    opts.is_state = true;
                } else if meta.path.is_ident("state_mut") {
                    opts.is_state_mut = true;
                } else if meta.path.is_ident("handle") {
                    opts.is_handle = true;
                } else if meta.path.is_ident("default") {
                    if meta.input.peek(syn::Token![=]) {
                        let expr: syn::Expr = meta.value()?.parse()?;
                        opts.default_value = Some(expr);
                    } else {
                        opts.default_value = Some(syn::parse_quote!(Default::default()));
                    }
                } else {
                    return Err(meta.error(
                        "unknown #[bind] option, expected one of `state`, `state_mut`, \
                         `handle`, `default`, `default = <expr>`",
                    ));
                }
                Ok(())
            })?;
        }
    }
    Ok(opts)
}

/// One function argument in the required format for codegen
struct ArgInfo {
    name: String,
    change_from_ref: bool,
    ty_without_ref: Type,
    opts: ArgOptions,
    /// The big type name, for a `&mut` big-type argument
    mut_big_type_name: Option<String>,
    /// The referenced type and mutability, for a `#[bind(handle)]` argument
    handle_elem: Option<(Type, bool)>,
}

impl ArgInfo {
    /// Whether this argument needs the write lock
    /// (like a `&mut` big type)
    fn is_mut_handle(&self) -> bool {
        matches!(self.handle_elem, Some((_, true)))
    }
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

    let args_info: syn::parse::Result<Vec<ArgInfo>> = input_fn
        .sig
        .inputs
        .iter()
        .map(|arg| match arg {
            FnArg::Typed(pat_type) => {
                let pat = &pat_type.pat;
                let ty = &pat_type.ty;

                let arg_opts = parse_arg_attributes(&pat_type.attrs)?;

                let arg_name =
                    match &**pat {
                        Pat::Ident(p) => p.ident.to_string(),
                        _ => return Err(syn::Error::new_spanned(
                            pat,
                            "#[register_binding] needs a plain `name: Type` argument, because the \
                             name is what a caller sends the argument under.",
                        )),
                    };

                let ty_no_life = strip_lifetimes(*ty.clone());
                let ty_as_str = quote::quote!(#ty_no_life).to_string();
                // A handle is passed as an id and resolved through `FromContext`, like a big type.
                let handle_elem = match (&ty_no_life, arg_opts.is_handle) {
                    (Type::Reference(r), true) => Some((*r.elem.clone(), r.mutability.is_some())),
                    (_, true) => {
                        return Err(syn::Error::new_spanned(
                            ty,
                            "#[bind(handle)] is only valid on a reference argument \
                             (`&T` or `&mut T`), where `T: CustomRegistryValue`.",
                        ))
                    }
                    (_, false) => None,
                };
                let change_from_ref = matches!(ty_no_life, Type::Reference(_))
                    && !arg_opts.is_handle
                    && !(BIG_TYPES_NAMES.iter().any(|tn| ty_as_str.ends_with(tn)));
                let type_without_ref = match &ty_no_life {
                    Type::Reference(type_reference) if change_from_ref => {
                        match &*type_reference.elem {
                            // `&[T]` -> extract a `Vec<T>`; `&Vec<T>` coerces back to `&[T]` at the
                            // call site. (Does not apply to `&[&str]`: `Vec<&str>` is not owned.)
                            Type::Slice(slice) => {
                                let elem = &slice.elem;
                                syn::parse_quote!(Vec<#elem>)
                            }
                            _ => *type_reference.elem.clone(),
                        }
                    }
                    x => x.clone(),
                };
                let mut_big_type_name = if handle_elem.is_some() {
                    None
                } else {
                    is_mut_big_type_ref(&ty_no_life)
                };
                Ok(ArgInfo {
                    name: arg_name,
                    change_from_ref,
                    ty_without_ref: type_without_ref,
                    opts: arg_opts,
                    mut_big_type_name,
                    handle_elem,
                })
            }
            FnArg::Receiver(receiver) => Err(syn::Error::new_spanned(
                receiver,
                "#[register_binding] is for free functions. A method's `self` has no argument \
                 name a caller could send it under.",
            )),
        })
        .collect();

    // Strip `#[bind]` before `#input_fn` is emitted on any path, including the error paths below.
    for input in &mut input_fn.sig.inputs {
        if let FnArg::Typed(pat_type) = input {
            pat_type.attrs.retain(|attr| !attr.path().is_ident("bind"));
        }
    }

    let args_info = match args_info {
        Ok(args_info) => args_info,
        Err(e) => {
            let err = e.to_compile_error();
            return TokenStream::from(quote! { #input_fn #err });
        }
    };

    // Whether the call needs the registry's write lock rather than its read lock: a `&mut`
    // big-type argument, a `#[bind(handle)]` on a `&mut` reference, or `#[bind(state_mut)]`
    // all resolve through the same write-locked `__state_guard` in the execution block below.
    let needs_write_lock = args_info
        .iter()
        .any(|a| a.mut_big_type_name.is_some() || a.is_mut_handle() || a.opts.is_state_mut);

    // A `#[bind(state)]` cannot be combined with anything needing the write lock (conflicting
    // ownership/mut borrow of the same registry guard).
    if needs_write_lock && args_info.iter().any(|a| a.opts.is_state) {
        let err = syn::Error::new_spanned(
            &input_fn.sig,
            "#[bind(state)] cannot be combined with a `&mut` big-type, `#[bind(handle)]`, or \
             `#[bind(state_mut)]` argument. Return the value instead of taking it by `&mut`.",
        )
        .to_compile_error();
        return TokenStream::from(quote! { #input_fn #err });
    }

    // `#[bind(state_mut)]` already gives full mutable access to every item in the registry, so
    // it must be the only thing asking for the write lock: combining it with a `&mut` big-type
    // or `#[bind(handle)]` argument would borrow the same guard mutably twice.
    let state_mut_count = args_info.iter().filter(|a| a.opts.is_state_mut).count();
    if state_mut_count > 1 {
        let err = syn::Error::new_spanned(
            &input_fn.sig,
            "#[bind(state_mut)] may only be used once per binding.",
        )
        .to_compile_error();
        return TokenStream::from(quote! { #input_fn #err });
    }
    if state_mut_count == 1
        && args_info
            .iter()
            .any(|a| !a.opts.is_state_mut && (a.mut_big_type_name.is_some() || a.is_mut_handle()))
    {
        let err = syn::Error::new_spanned(
            &input_fn.sig,
            "#[bind(state_mut)] cannot be combined with a `&mut` big-type or `#[bind(handle)]` \
             argument; it already has full mutable access to the registry, so reach that item \
             through `state_mut` instead.",
        )
        .to_compile_error();
        return TokenStream::from(quote! { #input_fn #err });
    }

    if needs_write_lock {
        if let Some(shared) = args_info.iter().find(|a| {
            a.mut_big_type_name.is_none()
                && !a.is_mut_handle()
                && !a.opts.is_state_mut
                && (a.handle_elem.is_some() || is_big_type_ref(&a.ty_without_ref))
        }) {
            let name = &shared.name;
            let err = syn::Error::new_spanned(
                &input_fn.sig,
                format!(
                    "#[register_binding] on a function taking a `&mut` big-type, \
                     `#[bind(handle)]`, or `#[bind(state_mut)]` argument cannot also take \
                     `{name}` by shared reference. Take it by `&mut` too, or take it by value."
                ),
            )
            .to_compile_error();
            return TokenStream::from(quote! { #input_fn #err });
        }
    }

    // 1. Extraction Logic (for the read-locked path only; a `#[bind(state_mut)]` argument
    // forces `needs_write_lock`, so this map's default arm never runs for one; it is handled in
    // the write-locked `execution_block` below instead.)
    let extractions = args_info.iter().map(|a| {
        let (name, is_ref, ty_without_ref, opts) = (&a.name, a.change_from_ref, &a.ty_without_ref, &a.opts);
        if opts.is_state {
            return quote! { ::process_mining::bindings::StateRef::new(state) };
        }
        let maybe_ref = if is_ref {
            quote! {&}
        } else {
            quote! {}
        };
        if let Some(default_expr) = &opts.default_value {
            quote! {
                #maybe_ref ::process_mining::bindings::extract_param::<#ty_without_ref>(arg_map, #name, state, || Some(#default_expr))?
            }
        } else {
            quote! {
                #maybe_ref ::process_mining::bindings::extract_param::<#ty_without_ref>(arg_map, #name, state, || None)?
            }
        }
    });

    // 2. Schema Logic
    let schema_gens = args_info.iter().map(|a| {
        let (name, ty_without_ref, opts) = (&a.name, &a.ty_without_ref, &a.opts);
        if opts.is_state || opts.is_state_mut {
            return quote! {};
        }
        if let Some((elem, _)) = &a.handle_elem {
             quote! {
                 args_schema.push((#name.to_string(), ::process_mining::__private::serde_json::json!({
                    "type": "string",
                    "title": <#elem as ::process_mining::bindings::CustomRegistryValue>::kind_name(),
                    "x-registry-ref": <#elem as ::process_mining::bindings::CustomRegistryValue>::kind_name(),
                    "x-widget": "entity-selector"
                })));
             }
        } else if is_big_type_ref(ty_without_ref) {
             let ty_str = quote::quote!(#ty_without_ref).to_string();
             let type_name = longest_big_type_match(&ty_str).unwrap();
             quote! {
                 args_schema.push((#name.to_string(), ::process_mining::__private::serde_json::json!({
                    "type": "string",
                    "title": #type_name,
                    "x-registry-ref": #type_name,
                    "x-widget": "entity-selector"
                })));
             }
        } else {
            quote! { args_schema.push((#name.to_string(), ::process_mining::__private::serde_json::to_value(::process_mining::__private::schemars::schema_for!(#ty_without_ref)).unwrap())); }
        }
    });

    // 3. Return Type Schema Logic
    let raw_ret_type = match &input_fn.sig.output {
        syn::ReturnType::Default => syn::parse_quote!(()), // Handle "void" -> unit type
        syn::ReturnType::Type(_, ty) => *ty.clone(),
    };

    // Strip lifetimes from return type
    let mut ret_type = strip_lifetimes(raw_ret_type);

    if attrs.returns_handle && attrs.debug_output {
        let err = syn::Error::new_spanned(
            &input_fn.sig,
            "#[register_binding(returns_handle)] cannot be combined with `debug_output`: \
             one stores the value and returns its id, the other formats it away.",
        )
        .to_compile_error();
        return TokenStream::from(quote! { #input_fn #err });
    }

    // If debug_output is set, the actual return type is String
    if attrs.debug_output {
        ret_type = syn::parse_quote!(String);
    } else if attrs.stringify_error {
        // If stringify_error is set, we expect a Result<T, E> (or io::Result<T>).
        // We unwrap the Result and use only the Ok type for the schema,
        // since errors are propagated via the handler's Result return.
        if let Type::Path(tp) = &ret_type {
            if let Some(segment) = tp.path.segments.last() {
                // Heuristic: If it looks like a Result (std or io), grab the first generic arg (Ok type)
                if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                    if let Some(ok_type) = args.args.first() {
                        ret_type = syn::parse_quote!(#ok_type);
                    }
                }
            }
        }
    }

    // Handling the two ways a result ends up in the registry instead of being serialized
    let returns_registry_handle = attrs.returns_handle || is_big_type(&ret_type).is_some();

    let output_id_schema = if returns_registry_handle {
        quote! {
            args_schema.push((#OUTPUT_ID_ARG.to_string(), ::process_mining::__private::serde_json::json!({
                "type": ["string", "null"],
                "title": #OUTPUT_ID_ARG,
                "description": "Store the result under this id instead of a generated one."
            })));
        }
    } else {
        quote! {}
    };

    let output_id_extraction = if returns_registry_handle {
        quote! {
            let __output_id = ::process_mining::bindings::extract_param_json::<Option<String>>(arg_map, #OUTPUT_ID_ARG, || Some(None))?;
        }
    } else {
        quote! {}
    };

    let stored_id = quote! {
        let id = __output_id
            .unwrap_or_else(|| format!("res_{}", ::process_mining::__private::uuid::Uuid::new_v4()));
    };

    // A `#[bind(state)]`/`#[bind(state_mut)]` argument has no schema, so requiring it by name
    // would tell a host to demand an argument it cannot describe and the caller cannot supply.
    let required_arg_names = args_info
        .iter()
        .filter(|a| a.opts.default_value.is_none() && !a.opts.is_state && !a.opts.is_state_mut)
        .map(|a| &a.name);

    // 4. Generate the Execution Logic
    let extractions: Vec<_> = extractions.collect();

    // Apply error handling if requested, independent of whether the return type is a big type or not.
    let error_handling = if attrs.stringify_error && !attrs.debug_output {
        quote! { let result = result.map_err(|e| e.to_string())?; }
    } else {
        quote! {}
    };

    let serialization_logic = if attrs.debug_output {
        quote! {
            let final_result = format!("{:?}", result);
            ::process_mining::__private::serde_json::to_vec(&final_result).map_err(|e| e.to_string())
        }
    } else {
        quote! {
            ::process_mining::__private::serde_json::to_vec(&result).map_err(|e| e.to_string())
        }
    };

    let execution_block = if needs_write_lock {
        // Mutable big type path: use write lock
        // 1. Generate JSON extractions for non-mut-big-type params (no state needed)
        let json_extractions: Vec<_> = args_info
            .iter()
            .filter(|a| a.mut_big_type_name.is_none() && !a.is_mut_handle() && !a.opts.is_state_mut)
            .map(|a| {
                let (name, is_ref, ty_without_ref, opts) = (&a.name, a.change_from_ref, &a.ty_without_ref, &a.opts);
                let param_ident = format_ident!("__param_{}", name);
                let maybe_ref = if is_ref {
                    quote! { & }
                } else {
                    quote! {}
                };
                if let Some(default_expr) = &opts.default_value {
                    quote! {
                        let #param_ident = #maybe_ref ::process_mining::bindings::extract_param_json::<#ty_without_ref>(arg_map, #name, || Some(#default_expr))?;
                    }
                } else {
                    quote! {
                        let #param_ident = #maybe_ref ::process_mining::bindings::extract_param_json::<#ty_without_ref>(arg_map, #name, || None)?;
                    }
                }
            })
            .collect();

        // 2. Generate mutable big type extractions from state
        let mut_extractions: Vec<_> = args_info
            .iter()
            .filter_map(|a| {
                let name = &a.name;
                let param_ident = format_ident!("__param_{}", name);
                if a.opts.is_state_mut {
                    return Some(quote! {
                        let #param_ident = ::process_mining::bindings::StateRefMut::new(&mut __state_guard);
                    });
                }
                if let Some((elem, true)) = &a.handle_elem {
                    return Some(quote! {
                        let #param_ident = {
                            let __id = arg_map.get(#name).and_then(|v| v.as_str())
                                .ok_or_else(|| format!("Missing required argument {}", #name))?;
                            __state_guard.get_mut(__id)
                                .ok_or_else(|| format!("Item '{}' not found", __id))?
                                .as_custom_mut::<#elem>()
                                .ok_or_else(|| format!("ID '{}' is not a {}", __id,
                                    <#elem as ::process_mining::bindings::CustomRegistryValue>::kind_name()))?
                        };
                    });
                }
                let type_name = a.mut_big_type_name.as_ref()?;
                let variant_ident = format_ident!("{}", type_name);
                Some(quote! {
                    let #param_ident = {
                        let __id = arg_map.get(#name).and_then(|v| v.as_str())
                            .ok_or_else(|| format!("Missing required argument {}", #name))?;
                        match __state_guard.get_mut(__id)
                            .ok_or_else(|| format!("Item '{}' not found", __id))? {
                            ::process_mining::bindings::RegistryItem::#variant_ident(inner) => inner,
                            _ => return Err(format!("ID '{}' is not a {}", __id, #type_name)),
                        }
                    };
                })
            })
            .collect();

        // 3. Generate call arguments in original order
        let call_args: Vec<_> = args_info
            .iter()
            .map(|a| {
                let param_ident = format_ident!("__param_{}", a.name);
                quote! { #param_ident }
            })
            .collect();

        // The write guard is still live here, and `std::sync::RwLock` is not reentrant, so this
        // has to insert through the guard rather than call `state_lock.add`, which would take the
        // lock again and deadlock.
        let mut_serialization = if attrs.returns_handle {
            quote! {
                #stored_id
                __state_guard.insert(id.clone(), ::process_mining::bindings::RegistryItem::custom(result));
                ::process_mining::__private::serde_json::to_vec(&id).map_err(|e| e.to_string())
            }
        } else if let Some(type_name) = is_big_type(&ret_type) {
            let variant_ident = format_ident!("{}", type_name);
            quote! {
                #stored_id
                __state_guard.insert(id.clone(), ::process_mining::bindings::RegistryItem::#variant_ident(result));
                ::process_mining::__private::serde_json::to_vec(&id).map_err(|e| e.to_string())
            }
        } else {
            serialization_logic.clone()
        };

        quote! {
            #(#json_extractions)*
            let mut __state_guard = state_lock.write();
            #(#mut_extractions)*
            let result = #fn_ident( #(#call_args),* );
            #error_handling
            #mut_serialization
        }
    } else if attrs.returns_handle {
        quote! {
            let result = {
                let state_guard = state_lock.read();
                let state = &*state_guard;
                #fn_ident( #(#extractions),* )
            };
            #error_handling
            #stored_id
            state_lock.add(&id, ::process_mining::bindings::RegistryItem::custom(result));
            ::process_mining::__private::serde_json::to_vec(&id).map_err(|e| e.to_string())
        }
    } else if let Some(type_name) = is_big_type(&ret_type) {
        let variant_ident = format_ident!("{}", type_name);
        quote! {
            let result = {
                let state_guard = state_lock.read();
                let state = &*state_guard;
                #fn_ident( #(#extractions),* )
            };
            #error_handling
            #stored_id
            state_lock.add(&id, ::process_mining::bindings::RegistryItem::#variant_ident(result));
            ::process_mining::__private::serde_json::to_vec(&id).map_err(|e| e.to_string())
        }
    } else {
        quote! {
            let state_guard = state_lock.read();
            let state = &*state_guard;
            let result = #fn_ident( #(#extractions),* );
            #error_handling
            #serialization_logic
        }
    };

    let ret_type_schema = if attrs.returns_handle {
        quote! {
            ::process_mining::__private::serde_json::json!({
               "type": "string",
               "title": <#ret_type as ::process_mining::bindings::CustomRegistryValue>::kind_name(),
               "x-registry-ref": <#ret_type as ::process_mining::bindings::CustomRegistryValue>::kind_name(),
               "x-widget": "entity-selector"
           })
        }
    } else if let Some(type_name) = is_big_type(&ret_type) {
        quote! {
            ::process_mining::__private::serde_json::json!({
               "type": "string",
               "title": #type_name,
               "x-registry-ref": #type_name,
               "x-widget": "entity-selector"
           })
        }
    } else {
        quote! {
            ::process_mining::__private::serde_json::to_value(::process_mining::__private::schemars::schema_for!(#ret_type)).unwrap()
        }
    };

    // Shadowing the caller's own argument would silently change what the binding is passed, so
    // refuse instead. The function itself is still emitted, to keep its call sites resolving.
    if returns_registry_handle && args_info.iter().any(|a| a.name == OUTPUT_ID_ARG) {
        let msg = format!(
            "#[register_binding] on `{}`: its result is stored in the registry, so the binding \
             already takes an `{}` argument. Rename the function's own argument.",
            fn_ident, OUTPUT_ID_ARG
        );
        return TokenStream::from(quote! {
            #input_fn
            ::core::compile_error!(#msg);
        });
    }

    let docs_fn_name = format_ident!("{}_docs", fn_ident);
    let args_fn_name = format_ident!("{}_args", fn_ident);
    let required_args_fn_name = format_ident!("{}_required_args", fn_ident);
    let return_type_fn_name = format_ident!("{}_return_type", fn_ident);

    // `cfg(feature = "bindings")` resolves in the crate being compiled, so a downstream crate with
    // no feature by that name would silently register nothing.
    let registration_cfg = if std::env::var("CARGO_PKG_NAME").as_deref() == Ok("process_mining") {
        quote! { #[cfg(feature = "bindings")] }
    } else {
        quote! {}
    };

    let expanded = quote! {
        #input_fn

        #registration_cfg
        const _: () = {
            use ::process_mining::bindings::{Binding, AppState};
            use ::process_mining::__private::serde_json::Value;

            fn #wrapper_name(args: &Value, state_lock: &AppState) -> Result<Vec<u8>, String> {
                let arg_map = args.as_object().ok_or("Args must be JSON object")?;
                #output_id_extraction
                #execution_block
            }

            fn #docs_fn_name() -> Vec<String> {
                vec![#(#docs.to_string(),)*]
            }

            fn #args_fn_name() -> Vec<(String, Value)> {
                let mut args_schema = ::std::vec::Vec::new();
                #(#schema_gens)*
                #output_id_schema
                args_schema
            }

            fn #required_args_fn_name() -> Vec<String> {
                vec![#(#required_arg_names.to_string(),)*]
            }

            fn #return_type_fn_name() -> Value {
                #ret_type_schema
            }

            ::process_mining::__private::inventory::submit! {
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

/// Let a `CustomRegistryValue` implementor be taken by `#[bind(handle)] &T`.
///
/// Generates the `FromContext` impl that turns the incoming id into a borrow of the stored value.
///
/// Unlike [`macro@RegistryEntity`], the impl is not behind `#[cfg(feature = "bindings")]`: that
/// cfg is evaluated in the deriving crate's feature namespace, and a downstream crate need not
/// have a feature of that name.
#[proc_macro_derive(CustomRegistryEntity)]
pub fn derive_custom_registry_entity(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as syn::DeriveInput);
    let name = &input.ident;

    let expanded = quote! {
        impl<'a> ::process_mining::bindings::FromContext<'a> for &'a #name {
            fn from_context(value: &::process_mining::__private::serde_json::Value, state: &'a ::process_mining::bindings::InnerAppState) -> Result<Self, String> {
                let id = value.as_str().ok_or("Expected String ID")?;
                let item = state.get(id).ok_or_else(|| format!("Item '{}' not found", id))?;
                item.as_custom::<#name>().ok_or_else(|| {
                    format!(
                        "ID '{}' is not a {}",
                        id,
                        <#name as ::process_mining::bindings::CustomRegistryValue>::kind_name()
                    )
                })
            }
        }
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
        impl<'a> ::process_mining::bindings::FromContext<'a> for &'a #name {
            fn from_context(value: &::process_mining::__private::serde_json::Value, state: &'a ::process_mining::bindings::InnerAppState) -> Result<Self, String> {
                let id = value.as_str().ok_or("Expected String ID")?;
                let item = state.get(id).ok_or_else(|| format!("Item '{}' not found", id))?;

                if let ::process_mining::bindings::RegistryItem::#name(inner) = item {
                    Ok(inner)
                } else {
                    Err(format!("ID '{}' is not a {}", id, #name_str))
                }
            }
        }
    };
    TokenStream::from(expanded)
}
