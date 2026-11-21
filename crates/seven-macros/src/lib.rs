//! The three attributes of a seven-line cell.
//!
//! - `#[cell(name = "orders", version = 1, replicas = 3)]` on the first item of the file: brings the prelude into
//!   scope and generates `fn main`, which runs the cell (serve a replica, call it, `up`, prove).
//! - `#[record(cell = orders)]` on a struct: serde, `Clone`, `Debug` and the `Record` binding to the cell.
//! - `#[op(cap = "orders.place")]` on `async fn op(cx: Cx, arg: T) -> Result<R>`: registers the op; the replica
//!   runs it only for a capability chain holding that right. `grant = Passkey` also needs a one-use owner grant
//!   bound to the caller and to this exact input.
//! - `home = Device` on `#[cell]`: one replica, on one device (`replicas = 1` is then the only allowed count).
//! - `skill = "..."` on `#[cell]`: the text `describe` hands to an agent.
//!
//! Every macro writes code you could write by hand against the `seven` crate.
use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as Ts2};
use quote::{format_ident, quote};
use syn::parse::Parser;
use syn::punctuated::Punctuated;
use syn::{parse_macro_input, Expr, ExprLit, FnArg, Ident, ItemFn, ItemStruct, Lit, Meta, Pat, Token};

/// The path of the runtime crate as the user's Cargo.toml names it (`seven`, or renamed, e.g. `ce`).
fn runtime() -> (Ts2, String) {
    use proc_macro_crate::{crate_name, FoundCrate};
    let name = match crate_name("seven") {
        Ok(FoundCrate::Name(n)) => n,
        _ => "seven".to_string(),
    };
    let id = Ident::new(&name, Span::call_site());
    (quote!(::#id), name)
}

struct Args(Vec<(String, Expr)>);

impl Args {
    fn parse(attr: TokenStream) -> syn::Result<Args> {
        let metas = Punctuated::<Meta, Token![,]>::parse_terminated.parse(attr)?;
        let mut out = Vec::new();
        for m in metas {
            match m {
                Meta::NameValue(nv) => out.push((nv.path.get_ident().map(|i| i.to_string()).unwrap_or_default(), nv.value)),
                other => return Err(syn::Error::new_spanned(other, "expected `key = value`")),
            }
        }
        Ok(Args(out))
    }
    fn get(&self, k: &str) -> Option<&Expr> {
        self.0.iter().find(|(n, _)| n == k).map(|(_, e)| e)
    }
    fn str(&self, k: &str) -> Option<String> {
        match self.get(k)? {
            Expr::Lit(ExprLit { lit: Lit::Str(s), .. }) => Some(s.value()),
            Expr::Path(p) => p.path.get_ident().map(|i| i.to_string()),
            _ => None,
        }
    }
    fn int(&self, k: &str) -> Option<u64> {
        match self.get(k)? {
            Expr::Lit(ExprLit { lit: Lit::Int(i), .. }) => i.base10_parse().ok(),
            _ => None,
        }
    }
}

fn err(span: Span, msg: &str) -> TokenStream {
    syn::Error::new(span, msg).to_compile_error().into()
}

/// `#[cell(name = "orders", version = 1, replicas = 3)]`.
#[proc_macro_attribute]
pub fn cell(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = match Args::parse(attr) {
        Ok(a) => a,
        Err(e) => return e.to_compile_error().into(),
    };
    let Some(name) = args.str("name") else { return err(Span::call_site(), "#[cell] needs name = \"...\"") };
    let version = args.int("version").unwrap_or(1) as u32;
    let device = match args.str("home").as_deref() {
        None | Some("Fleet") => false,
        Some("Device") => true,
        Some(_) => return err(Span::call_site(), "home is Fleet (the default) or Device"),
    };
    let replicas = args.int("replicas").unwrap_or(if device { 1 } else { 3 }) as u32;
    if device && replicas != 1 {
        return err(Span::call_site(), "home = Device means replicas = 1: the cell is that one device's own and cannot be placed on a second");
    }
    if !device && (replicas < 3 || replicas % 2 == 0) {
        return err(Span::call_site(), "replicas must be odd and at least 3 (3, 5, 7): a majority must survive the loss of any one");
    }
    let home = if device { quote!(Device) } else { quote!(Fleet) };
    let skill = args.str("skill").unwrap_or_default();
    let (rt, _) = runtime();
    let item = Ts2::from(item);
    quote! {
        #[allow(unused_imports)]
        use #rt::prelude::*;
        #item
        fn main() {
            #rt::run(#rt::CellDef { name: #name, version: #version, replicas: #replicas, skill: #skill, home: #rt::Home::#home })
        }
    }
    .into()
}

/// `#[record(cell = orders)]`.
#[proc_macro_attribute]
pub fn record(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = match Args::parse(attr) {
        Ok(a) => a,
        Err(e) => return e.to_compile_error().into(),
    };
    let Some(cell) = args.str("cell") else { return err(Span::call_site(), "#[record] needs cell = <cell name>") };
    let s = parse_macro_input!(item as ItemStruct);
    let (rt, rt_name) = runtime();
    let serde_path = format!("{rt_name}::serde");
    let ident = &s.ident;
    let name = ident.to_string();
    let fnames: Vec<String> = s.fields.iter().map(|f| f.ident.as_ref().map(|i| i.to_string()).unwrap_or_default()).collect();
    let ftypes = s.fields.iter().map(|f| {
        let t = &f.ty;
        quote!(stringify!(#t))
    });
    quote! {
        #[derive(#rt::serde::Serialize, #rt::serde::Deserialize, ::core::clone::Clone, ::core::fmt::Debug)]
        #[serde(crate = #serde_path)]
        #s
        impl #rt::Record for #ident {
            const CELL: &'static str = #cell;
            const NAME: &'static str = #name;
        }
        #rt::inventory::submit! {
            #rt::RecordDef { cell: #cell, name: #name, fields: &[#((#fnames, #ftypes)),*] }
        }
    }
    .into()
}

/// `#[op(cap = "orders.place")]` on `async fn place(cx: Cx, o: Order) -> Result<Receipt>`.
#[proc_macro_attribute]
pub fn op(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = match Args::parse(attr) {
        Ok(a) => a,
        Err(e) => return e.to_compile_error().into(),
    };
    let Some(cap) = args.str("cap") else { return err(Span::call_site(), "#[op] needs cap = \"<cell>.<right>\"") };
    let grant = match args.str("grant").as_deref() {
        None => quote!(None),
        Some("Passkey") => quote!(Passkey),
        Some(_) => return err(Span::call_site(), "grant is Passkey (or left out)"),
    };
    let f = parse_macro_input!(item as ItemFn);
    if f.sig.asyncness.is_none() {
        return err(f.sig.ident.span(), "an op is an async fn");
    }
    let inputs: Vec<&FnArg> = f.sig.inputs.iter().collect();
    if inputs.len() != 2 {
        return err(f.sig.ident.span(), "an op takes (cx: Cx, input: T)");
    }
    let FnArg::Typed(arg) = inputs[1] else { return err(f.sig.ident.span(), "an op is a free fn") };
    if !matches!(&*arg.pat, Pat::Ident(_)) {
        return err(f.sig.ident.span(), "name the op's input");
    }
    let in_ty = &arg.ty;
    let out_ty = match &f.sig.output {
        syn::ReturnType::Type(_, t) => quote!(#t),
        syn::ReturnType::Default => quote!(()),
    };
    let (rt, _) = runtime();
    let ident = &f.sig.ident;
    let name = ident.to_string();
    let call = format_ident!("__seven_op_{}", ident);
    quote! {
        #f
        #[doc(hidden)]
        fn #call(cx: #rt::Cx, v: #rt::serde_json::Value) -> #rt::BoxFut<#rt::serde_json::Value> {
            ::std::boxed::Box::pin(async move {
                let a: #in_ty = #rt::serde_json::from_value(v).map_err(#rt::Error::input)?;
                let r = #ident(cx, a).await?;
                #rt::serde_json::to_value(&r).map_err(#rt::Error::input)
            })
        }
        #rt::inventory::submit! {
            #rt::OpDef { name: #name, cap: #cap, input: stringify!(#in_ty), output: stringify!(#out_ty), grant: #rt::Grant::#grant, call: #call }
        }
    }
    .into()
}
