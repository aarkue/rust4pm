use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::meta::ParseNestedMeta;
use syn::{parse_macro_input, Attribute, FnArg, ItemFn, Lifetime, Pat};

use syn::fold::{self, Fold};
use syn::{AngleBracketedGenericArguments, GenericArgument, Type, TypeReference};

/// The argument added to bindings whose result is stored in the registry, naming the id to store
/// it under. Optional for a binding returning only the stored item, which otherwise gets a
/// generated id. Required for one returning a `(stored, value)` pair, whose call result is
/// `value`, so the caller would not learn a generated id.
const OUTPUT_ID_ARG: &str = "output_id";

/// Schema key on the required `output_id` of a `(stored, value)` binding, naming the stored kind.
const REGISTRY_OUT_KEY: &str = "x-registry-out";

/// How a binding's stored result goes into the registry
enum StoredKind {
    /// `#[register_binding(returns_handle)]`: a `CustomRegistryValue` of this type
    Custom(Type),
    /// One of [`BIG_TYPES_NAMES`]
    Big(String),
}

impl StoredKind {
    fn kind_name(&self) -> proc_macro2::TokenStream {
        match self {
            Self::Custom(ty) => quote! {
                <#ty as ::process_mining::bindings::CustomRegistryValue>::kind_name()
            },
            Self::Big(name) => quote! { #name },
        }
    }

    fn registry_item(&self, value: proc_macro2::TokenStream) -> proc_macro2::TokenStream {
        match self {
            Self::Custom(_) => quote! { ::process_mining::bindings::RegistryItem::custom(#value) },
            Self::Big(name) => {
                let variant = format_ident!("{}", name);
                quote! { ::process_mining::bindings::RegistryItem::#variant(#value) }
            }
        }
    }
}

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
        // `&(impl Trait + ?Sized)` parses as a parenthesized type.
        if let Type::Paren(p) = ty {
            return self.fold_type(*p.elem);
        }
        if let Type::ImplTrait(it) = &ty {
            let mut traits = it.bounds.iter().filter(|b| {
                !matches!(b, syn::TypeParamBound::Trait(t) if matches!(t.modifier, syn::TraitBoundModifier::Maybe(_)))
            });
            let (Some(only), None) = (traits.next(), traits.next()) else {
                return fold::fold_type(self, ty);
            };
            if let syn::TypeParamBound::Trait(really_it) = only {
                let really_it_str = quote::quote!(#really_it).to_string();
                let ret = match really_it_str.as_str() {
                    "AsRef < Path >"
                    | "AsRef < std :: path :: Path >"
                    | "AsRef < path :: Path >" => {
                        syn::parse_quote!(std::path::PathBuf)
                    }
                    "AsRef < str >" => syn::parse_quote!(String),
                    "LinkedOCELAccess < 'a >" | "ReadableOCEL" => syn::parse_quote!(
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

/// `&[&str]`, which has no owned form to deserialize into
fn is_str_slice(ty: &Type) -> bool {
    let Type::Reference(r) = ty else { return false };
    let Type::Slice(slice) = &*r.elem else {
        return false;
    };
    matches!(&*slice.elem, Type::Reference(inner) if quote::quote!(#inner).to_string().ends_with("str"))
}

#[derive(Default)]
struct RegisterBindingAttrs {
    stringify_error: bool,
    debug_output: bool,
    returns_handle: bool,
    controlled: bool,
    /// `media_type = "..."`: the media type of a returned string, e.g., `image/svg+xml`,
    /// as `contentMediaType` in the return schema
    media_type: Option<syn::LitStr>,
}

impl RegisterBindingAttrs {
    fn parse(&mut self, meta: ParseNestedMeta) -> syn::parse::Result<()> {
        if meta.path.is_ident("debug_output") {
            self.debug_output = true;
        } else if meta.path.is_ident("stringify_error") {
            self.stringify_error = true;
        } else if meta.path.is_ident("returns_handle") {
            self.returns_handle = true;
        } else if meta.path.is_ident("controlled") {
            self.controlled = true;
        } else if meta.path.is_ident("media_type") {
            self.media_type = Some(meta.value()?.parse()?);
        } else {
            return Err(meta.error(
                "unknown #[register_binding] option, expected one of `debug_output`, \
                 `stringify_error`, `returns_handle`, `media_type = \"...\"`",
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
    /// `#[bind(ctx)]`: not a JSON argument but the call's `&Control` (progress, cancel, partials)
    is_ctx: bool,
}

fn parse_arg_attributes(attrs: &[Attribute]) -> syn::parse::Result<ArgOptions> {
    let mut opts = ArgOptions {
        default_value: None,
        is_state: false,
        is_state_mut: false,
        is_handle: false,
        is_ctx: false,
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
                } else if meta.path.is_ident("ctx") {
                    opts.is_ctx = true;
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
                         `handle`, `ctx`, `default`, `default = <expr>`",
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
    /// A `&[&str]`, extracted as a `Vec<String>` and borrowed
    str_slice: bool,
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

    let binding_name_str = fn_ident.to_string();
    let wrapper_name = format_ident!("{}_wrapper", fn_ident);

    // Kept as expressions, so `#[doc = include_str!(..)]` reaches the binding too.
    let docs: Vec<&syn::Expr> = input_fn
        .attrs
        .iter()
        .filter(|attr| attr.path().is_ident("doc"))
        .filter_map(|attr| match &attr.meta {
            syn::Meta::NameValue(nv) => Some(&nv.value),
            _ => None,
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
                let str_slice = is_str_slice(&ty_no_life);
                let change_from_ref = matches!(ty_no_life, Type::Reference(_))
                    && !arg_opts.is_handle
                    && !str_slice
                    && !(BIG_TYPES_NAMES.iter().any(|tn| ty_as_str.ends_with(tn)));
                let type_without_ref = match &ty_no_life {
                    _ if str_slice => syn::parse_quote!(Vec<String>),
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
                    str_slice,
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

    // 1. Extraction for the read-locked path. A `#[bind(state_mut)]` argument forces
    // `needs_write_lock`, so the write-locked `execution_block` below handles it.
    let extractions = args_info.iter().map(|a| {
        let (name, is_ref, ty_without_ref, opts) = (&a.name, a.change_from_ref, &a.ty_without_ref, &a.opts);
        if opts.is_state {
            return quote! { ::process_mining::bindings::StateRef::new(state) };
        }
        if opts.is_ctx {
            return quote! { __ctl };
        }
        let maybe_ref = if is_ref {
            quote! {&}
        } else {
            quote! {}
        };
        let default = match &opts.default_value {
            Some(default_expr) => quote! { Some(#default_expr) },
            None => quote! { None },
        };
        let extracted = quote! {
            ::process_mining::bindings::extract_param::<#ty_without_ref>(arg_map, #name, state, || #default)?
        };
        if a.str_slice {
            quote! { &#extracted.iter().map(String::as_str).collect::<Vec<&str>>() }
        } else {
            quote! { #maybe_ref #extracted }
        }
    });

    // 2. Schema Logic
    let schema_gens = args_info.iter().map(|a| {
        let (name, ty_without_ref, opts) = (&a.name, &a.ty_without_ref, &a.opts);
        if opts.is_state || opts.is_state_mut || opts.is_ctx {
            return quote! {};
        }
        // `x-registry-mut` tells the runtime the item is mutated in place, so it must not be
        // swapped for a converted copy and must not outlive a panic in the body.
        let mark_mut = if a.mut_big_type_name.is_some() || a.is_mut_handle() {
            quote! { args_schema.last_mut().unwrap().1["x-registry-mut"] = true.into(); }
        } else {
            quote! {}
        };
        if let Some((elem, _)) = &a.handle_elem {
             quote! {
                 args_schema.push((#name.to_string(), ::process_mining::__private::serde_json::json!({
                    "type": "string",
                    "title": <#elem as ::process_mining::bindings::CustomRegistryValue>::kind_name(),
                    "x-registry-ref": <#elem as ::process_mining::bindings::CustomRegistryValue>::kind_name(),
                    "x-widget": "entity-selector"
                })));
                 #mark_mut
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
                 #mark_mut
             }
        } else {
            // A `#[bind(default)]` value goes into the schema, so hosts can show and use it
            let set_default = match &opts.default_value {
                Some(expr) => quote! {
                    let default_value: #ty_without_ref = #expr;
                    if let Ok(value) = ::process_mining::__private::serde_json::to_value(&default_value) {
                        let schema = &mut args_schema.last_mut().unwrap().1;
                        let value = ::process_mining::__private::sorted_sets(schema, value);
                        schema["default"] = value;
                    }
                },
                None => quote! {},
            };
            quote! {
                args_schema.push((#name.to_string(), ::process_mining::__private::serde_json::to_value(::process_mining::__private::schemars::schema_for!(#ty_without_ref)).unwrap()));
                #set_default
            }
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

    // A `(stored, value)` pair stores its first element and returns its second.
    let returned_value_type: Option<Type> = match &ret_type {
        Type::Tuple(t) if !attrs.debug_output && t.elems.len() == 2 => {
            let first = &t.elems[0];
            (attrs.returns_handle || is_big_type(first).is_some()).then(|| t.elems[1].clone())
        }
        _ => None,
    };
    let stored_type = match (&ret_type, &returned_value_type) {
        (Type::Tuple(t), Some(_)) => t.elems[0].clone(),
        _ => ret_type.clone(),
    };
    let stored_kind = if attrs.returns_handle {
        Some(StoredKind::Custom(stored_type.clone()))
    } else {
        is_big_type(&stored_type).map(StoredKind::Big)
    };
    let returns_registry_handle = stored_kind.is_some();

    let output_id_schema = match (&stored_kind, &returned_value_type) {
        (Some(kind), Some(_)) => {
            let kind_name = kind.kind_name();
            quote! {
                args_schema.push((#OUTPUT_ID_ARG.to_string(), ::process_mining::__private::serde_json::json!({
                    "type": "string",
                    "title": #OUTPUT_ID_ARG,
                    "description": "Store the produced item under this id, replacing any item already stored there.",
                    #REGISTRY_OUT_KEY: #kind_name
                })));
            }
        }
        (Some(_), None) => quote! {
            args_schema.push((#OUTPUT_ID_ARG.to_string(), ::process_mining::__private::serde_json::json!({
                "type": ["string", "null"],
                "title": #OUTPUT_ID_ARG,
                "description": "Store the result under this id instead of a generated one."
            })));
        },
        (None, _) => quote! {},
    };

    let output_id_extraction = match (&stored_kind, &returned_value_type) {
        (Some(_), Some(_)) => quote! {
            let __output_id = ::process_mining::bindings::extract_param_json::<::std::string::String>(arg_map, #OUTPUT_ID_ARG, || None)?;
        },
        (Some(_), None) => quote! {
            let __output_id = ::process_mining::bindings::extract_param_json::<Option<String>>(arg_map, #OUTPUT_ID_ARG, || Some(None))?;
        },
        (None, _) => quote! {},
    };

    // A cancelled call stores nothing, even when the body finished anyway.
    let cancelled_check = quote! {
        if __ctl.is_cancelled() {
            return Err(::process_mining::bindings::CallError::Cancelled);
        }
    };
    let to_json = |value: proc_macro2::TokenStream| {
        quote! {
            ::process_mining::__private::serde_json::to_vec(#value).map_err(|e| ::process_mining::bindings::CallError::Failed(e.to_string()))
        }
    };
    // Stores `result` (or its first element) through `insert`, and serializes the response.
    // Serializing a pair's value before the insert keeps a failure from leaving the item behind.
    let store_result = |insert: &dyn Fn(proc_macro2::TokenStream) -> proc_macro2::TokenStream| {
        let kind = stored_kind.as_ref()?;
        Some(if returned_value_type.is_some() {
            let insert = insert(kind.registry_item(quote! { __stored }));
            let respond = to_json(quote! { &result });
            quote! {
                let (__stored, result) = result;
                #cancelled_check
                let id = __output_id;
                let __bytes = #respond?;
                #insert
                Ok(__bytes)
            }
        } else {
            let insert = insert(kind.registry_item(quote! { result }));
            let respond = to_json(quote! { &id });
            quote! {
                #cancelled_check
                let id = __output_id
                    .unwrap_or_else(|| format!("res_{}", ::process_mining::__private::uuid::Uuid::new_v4()));
                #insert
                #respond
            }
        })
    };

    let required_output_id = if returned_value_type.is_some() && returns_registry_handle {
        vec![OUTPUT_ID_ARG.to_string()]
    } else {
        Vec::new()
    };

    // A `#[bind(state)]`/`#[bind(state_mut)]` argument has no schema, so requiring it by name
    // would tell a host to demand an argument it cannot describe and the caller cannot supply.
    let required_arg_names = args_info
        .iter()
        .filter(|a| {
            a.opts.default_value.is_none()
                && !a.opts.is_state
                && !a.opts.is_state_mut
                && !a.opts.is_ctx
        })
        .map(|a| &a.name)
        .chain(&required_output_id);

    // 4. Generate the Execution Logic
    let extractions: Vec<_> = extractions.collect();

    if let Some(a) = input_fn
        .sig
        .inputs
        .iter()
        .zip(&args_info)
        .find_map(|(arg, info)| match arg {
            FnArg::Typed(pt) if is_control_ref(&pt.ty) && !info.opts.is_ctx => Some(pt),
            _ => None,
        })
    {
        let err = syn::Error::new_spanned(
            a,
            "a `&Control` argument needs `#[bind(ctx)]` (inside `process_mining`, \
             `#[controlled]` above `#[register_binding]` also works)",
        )
        .to_compile_error();
        return TokenStream::from(quote! { #input_fn #err });
    }
    if attrs.controlled && args_info.iter().any(|a| a.opts.is_ctx) {
        let err = syn::Error::new_spanned(
            &input_fn.sig,
            "below #[controlled], the binding already passes the call's control, so the \
             function cannot take `#[bind(ctx)]`.",
        )
        .to_compile_error();
        return TokenStream::from(quote! { #input_fn #err });
    }
    // `controlled` calls `Control::<name>`, which returns what the function returns, wrapped in
    // `Result<_, Cancelled>` unless it already is a `Result`.
    let call = |args: proc_macro2::TokenStream| {
        if !attrs.controlled {
            return quote! { #fn_ident(#args) };
        }
        let returns_result = matches!(&input_fn.sig.output, syn::ReturnType::Type(_, ty)
            if matches!(&**ty, Type::Path(tp) if tp.path.segments.last().is_some_and(|s| s.ident == "Result")));
        let unwrap_cancelled = if returns_result {
            quote! {}
        } else {
            quote! { ? }
        };
        quote! { ::process_mining::Control::#fn_ident(__ctl, #args) #unwrap_cancelled }
    };
    // Only references (registry items, handles) and `#[bind(state)]` read the registry; every other
    // argument is plain JSON, so such a binding runs without holding the registry lock.
    let reads_state = args_info
        .iter()
        .any(|a| a.opts.is_state || matches!(a.ty_without_ref, Type::Reference(_)));
    let call_extracted = if reads_state {
        call(quote! { #(#extractions),* })
    } else {
        let json_extractions = args_info.iter().map(|a| {
            if a.opts.is_ctx {
                return quote! { __ctl };
            }
            let (name, ty) = (&a.name, &a.ty_without_ref);
            let default = match &a.opts.default_value {
                Some(default_expr) => quote! { Some(#default_expr) },
                None => quote! { None },
            };
            let extracted = quote! {
                ::process_mining::bindings::extract_param_json::<#ty>(arg_map, #name, || #default)?
            };
            if a.str_slice {
                quote! { &#extracted.iter().map(String::as_str).collect::<Vec<&str>>() }
            } else if a.change_from_ref {
                quote! { &#extracted }
            } else {
                extracted
            }
        });
        call(quote! { #(#json_extractions),* })
    };
    let state_guard = if reads_state {
        quote! {
            let state_guard = state_lock.read();
            let state = &*state_guard;
        }
    } else {
        quote! {}
    };

    // Apply error handling if requested, independent of whether the return type is a big type or not.
    let error_handling = if attrs.stringify_error && !attrs.debug_output {
        quote! { let result = result.map_err(|e| e.to_string())?; }
    } else {
        quote! {}
    };

    let serialization_logic = if attrs.debug_output {
        quote! {
            let final_result = format!("{:?}", result);
            ::process_mining::__private::serde_json::to_vec(&final_result).map_err(|e| ::process_mining::bindings::CallError::Failed(e.to_string()))
        }
    } else {
        quote! {
            ::process_mining::__private::serde_json::to_vec(&result).map_err(|e| ::process_mining::bindings::CallError::Failed(e.to_string()))
        }
    };

    let execution_block = if needs_write_lock {
        // Mutable big type path: use write lock
        // 1. Generate JSON extractions for non-mut-big-type params (no state needed)
        let json_extractions: Vec<_> = args_info
            .iter()
            .filter(|a| {
                a.mut_big_type_name.is_none()
                    && !a.is_mut_handle()
                    && !a.opts.is_state_mut
                    && !a.opts.is_ctx
            })
            .map(|a| {
                let (name, is_ref, ty_without_ref, opts) = (&a.name, a.change_from_ref, &a.ty_without_ref, &a.opts);
                let param_ident = format_ident!("__param_{}", name);
                let maybe_ref = if is_ref {
                    quote! { & }
                } else {
                    quote! {}
                };
                let default = match &opts.default_value {
                    Some(default_expr) => quote! { Some(#default_expr) },
                    None => quote! { None },
                };
                let extracted = quote! {
                    ::process_mining::bindings::extract_param_json::<#ty_without_ref>(arg_map, #name, || #default)?
                };
                if a.str_slice {
                    let owned = format_ident!("__owned_{}", name);
                    let borrowed = format_ident!("__borrowed_{}", name);
                    quote! {
                        let #owned = #extracted;
                        let #borrowed: Vec<&str> = #owned.iter().map(String::as_str).collect();
                        let #param_ident = &#borrowed[..];
                    }
                } else {
                    quote! { let #param_ident = #maybe_ref #extracted; }
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
                if a.opts.is_ctx {
                    return Some(quote! { let #param_ident = __ctl; });
                }
                // The same errors `extract_param` reports on the read path.
                let id_and_item = quote! {
                    let __id = arg_map.get(#name)
                        .ok_or_else(|| format!("Missing required argument {}", #name))?
                        .as_str()
                        .ok_or_else(|| format!("Invalid Argument: {}\nExpected String ID", #name))?;
                    let __item = __state_guard.get_mut(__id).ok_or_else(|| {
                        format!("Invalid Argument: {}\nItem '{}' not found", #name, __id)
                    })?;
                };
                if let Some((elem, true)) = &a.handle_elem {
                    return Some(quote! {
                        let #param_ident = {
                            #id_and_item
                            __item.as_custom_mut::<#elem>().ok_or_else(|| {
                                format!("Invalid Argument: {}\nID '{}' is not a {}", #name, __id,
                                    <#elem as ::process_mining::bindings::CustomRegistryValue>::kind_name())
                            })?
                        };
                    });
                }
                let type_name = a.mut_big_type_name.as_ref()?;
                let variant_ident = format_ident!("{}", type_name);
                Some(quote! {
                    let #param_ident = {
                        #id_and_item
                        match __item {
                            ::process_mining::bindings::RegistryItem::#variant_ident(inner) => inner,
                            _ => return Err(format!(
                                "Invalid Argument: {}\nID '{}' is not a {}", #name, __id, #type_name
                            ).into()),
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
        let mut_serialization = store_result(&|item| {
            quote! { __state_guard.insert(::std::clone::Clone::clone(&id), #item); }
        })
        .unwrap_or_else(|| serialization_logic.clone());

        let call_with_args = call(quote! { #(#call_args),* });
        quote! {
            #(#json_extractions)*
            let mut __state_guard = state_lock.write();
            #(#mut_extractions)*
            let result = #call_with_args;
            #error_handling
            #mut_serialization
        }
    } else if let Some(store) =
        store_result(&|item| quote! { state_lock.add(::std::clone::Clone::clone(&id), #item); })
    {
        quote! {
            let result = {
                #state_guard
                #call_extracted
            };
            #error_handling
            #store
        }
    } else {
        quote! {
            #state_guard
            let result = #call_extracted;
            #error_handling
            #serialization_logic
        }
    };

    let set_media_type = match &attrs.media_type {
        Some(media_type) => quote! { schema["contentMediaType"] = #media_type.into(); },
        None => quote! {},
    };

    let ret_type_schema = match (&stored_kind, &returned_value_type) {
        (Some(kind), None) => {
            let kind_name = kind.kind_name();
            quote! {
                ::process_mining::__private::serde_json::json!({
                   "type": "string",
                   "title": #kind_name,
                   "x-registry-ref": #kind_name,
                   "x-widget": "entity-selector"
               })
            }
        }
        (_, Some(value_type)) => quote! {
            ::process_mining::__private::serde_json::to_value(::process_mining::__private::schemars::schema_for!(#value_type)).unwrap()
        },
        (None, None) => quote! {
            ::process_mining::__private::serde_json::to_value(::process_mining::__private::schemars::schema_for!(#ret_type)).unwrap()
        },
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
            // Shadows the caller's names (e.g., a `use std::io::Result;`) for the generated code
            // below.
            #[allow(unused_imports)]
            use ::std::{
                boxed::Box,
                option::Option::{self, None, Some},
                result::Result::{self, Err, Ok},
                string::{String, ToString},
                vec::Vec,
            };

            fn #wrapper_name(
                args: &Value,
                state_lock: &AppState,
                __ctl: &::process_mining::Control<'_>,
            ) -> Result<Vec<u8>, ::process_mining::bindings::CallError> {
                let arg_map = args.as_object().ok_or("Args must be JSON object")?;
                #output_id_extraction
                #execution_block
            }

            fn #docs_fn_name() -> Vec<String> {
                let docs: &[&str] = &[#(#docs),*];
                docs.iter()
                    .flat_map(|d| d.lines())
                    .map(|l| l.strip_prefix(' ').unwrap_or(l).to_string())
                    .collect()
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
                #[allow(unused_mut)]
                let mut schema = #ret_type_schema;
                #set_media_type
                schema
            }

            ::process_mining::__private::inventory::submit! {
                Binding {
                    id: concat!(env!("CARGO_CRATE_NAME"), "::", #binding_name_str),
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

fn is_control_ref(ty: &Type) -> bool {
    matches!(ty, Type::Reference(r) if matches!(&*r.elem, Type::Path(p)
        if p.path.segments.last().is_some_and(|s| s.ident == "Control")))
}

/// `T` of a `Result<T, Cancelled>`, which a call without a control cannot fail
fn cancelled_ok_type(output: &syn::ReturnType) -> Option<&Type> {
    let syn::ReturnType::Type(_, ty) = output else {
        return None;
    };
    let Type::Path(tp) = &**ty else { return None };
    let last = tp.path.segments.last()?;
    let syn::PathArguments::AngleBracketed(args) = &last.arguments else {
        return None;
    };
    match (last.ident == "Result", args.args.first(), args.args.get(1)) {
        (true, Some(GenericArgument::Type(ok)), Some(GenericArgument::Type(Type::Path(err))))
            if err
                .path
                .segments
                .last()
                .is_some_and(|s| s.ident == "Cancelled") =>
        {
            Some(ok)
        }
        _ => None,
    }
}

/// Write an operation once, as a function taking `ctl: &Control<'_>` last.
///
/// Emits the function without `ctl`, which runs without progress and returns `T` for a
/// `Result<T, Cancelled>`, and `Control::<name>`, which reports to `ctl` and stops once it is
/// cancelled. Both carry the docs and attributes. A `#[register_binding]` below registers the
/// function with the call's control.
///
/// Only for `process_mining` itself, as it adds methods to `process_mining::Control`. Other crates
/// take the control with `#[bind(ctx)]`.
#[doc(hidden)]
#[proc_macro_attribute]
pub fn controlled(args: TokenStream, item: TokenStream) -> TokenStream {
    if !args.is_empty() {
        return syn::Error::new(
            proc_macro2::Span::call_site(),
            "#[controlled] takes no arguments",
        )
        .to_compile_error()
        .into();
    }
    let ItemFn {
        attrs,
        vis,
        sig,
        block,
    } = parse_macro_input!(item as ItemFn);
    let mut inputs: Vec<FnArg> = sig.inputs.iter().cloned().collect();
    let ctl = match inputs.pop() {
        Some(FnArg::Typed(pt)) if is_control_ref(&pt.ty) => pt,
        _ => {
            return syn::Error::new_spanned(
                &sig,
                "#[controlled] needs `ctl: &Control<'_>` as the last argument",
            )
            .to_compile_error()
            .into()
        }
    };
    let ctl_pat = &ctl.pat;
    let ctl_ty = &ctl.ty;

    let mut arg_idents = Vec::new();
    let mut plain_inputs = Vec::new();
    let mut method_inputs = Vec::new();
    for arg in &inputs {
        let FnArg::Typed(pt) = arg else {
            return syn::Error::new_spanned(arg, "#[controlled] is for free functions")
                .to_compile_error()
                .into();
        };
        let Pat::Ident(pi) = &*pt.pat else {
            return syn::Error::new_spanned(
                &pt.pat,
                "#[controlled] needs plain `name: Type` arguments",
            )
            .to_compile_error()
            .into();
        };
        arg_idents.push(pi.ident.clone());
        let mut plain = pt.clone();
        if let Pat::Ident(pi) = &mut *plain.pat {
            pi.mutability = None;
        }
        plain_inputs.push(plain);
        let mut method = pt.clone();
        method.attrs.retain(|a| !a.path().is_ident("bind"));
        method_inputs.push(method);
    }

    let mut docs = Vec::new();
    let mut shared = Vec::new();
    let mut binding = Vec::new();
    for attr in attrs {
        if attr.path().is_ident("doc") {
            docs.push(attr);
        } else if attr
            .path()
            .segments
            .last()
            .is_some_and(|s| s.ident == "register_binding")
        {
            let path = attr.path();
            let rewritten: Attribute = match &attr.meta {
                syn::Meta::List(list) => {
                    let tokens = &list.tokens;
                    syn::parse_quote!(#[#path(controlled, #tokens)])
                }
                _ => syn::parse_quote!(#[#path(controlled)]),
            };
            binding.push(rewritten);
        } else {
            shared.push(attr);
        }
    }

    let ident = &sig.ident;
    let generics = &sig.generics;
    let where_clause = &sig.generics.where_clause;
    let output = &sig.output;
    let (plain_output, unwrap) = match cancelled_ok_type(output) {
        Some(ok) => (
            quote! { -> #ok },
            quote! { .expect(::process_mining::utils::control::NEVER_CANCELLED) },
        ),
        None => (quote! { #output }, quote! {}),
    };

    quote! {
        #(#docs)*
        #(#shared)*
        #(#binding)*
        #vis fn #ident #generics (#(#plain_inputs),*) #plain_output #where_clause {
            ::process_mining::Control::new().#ident(#(#arg_idents),*) #unwrap
        }

        impl ::process_mining::Control<'_> {
            #(#docs)*
            #(#shared)*
            #vis fn #ident #generics (&self, #(#method_inputs),*) #output #where_clause {
                let #ctl_pat: #ctl_ty = self;
                #block
            }
        }
    }
    .into()
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
            fn from_context(value: &::process_mining::__private::serde_json::Value, state: &'a ::process_mining::bindings::InnerAppState) -> ::core::result::Result<Self, ::std::string::String> {
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
            fn from_context(value: &::process_mining::__private::serde_json::Value, state: &'a ::process_mining::bindings::InnerAppState) -> ::core::result::Result<Self, ::std::string::String> {
                let id = value.as_str().ok_or("Expected String ID")?;
                let item = state.get(id).ok_or_else(|| format!("Item '{}' not found", id))?;

                if let ::process_mining::bindings::RegistryItem::#name(inner) = item {
                    ::core::result::Result::Ok(inner)
                } else {
                    ::core::result::Result::Err(format!("ID '{}' is not a {}", id, #name_str))
                }
            }
        }
    };
    TokenStream::from(expanded)
}
