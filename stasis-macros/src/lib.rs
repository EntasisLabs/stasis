use proc_macro::TokenStream;

use quote::{format_ident, quote};
use syn::parse::Parser;
use syn::{
    Expr, ExprLit, FnArg, ItemFn, Lit, LitStr, MetaNameValue, PathArguments, ReturnType, Type,
    punctuated::Punctuated,
};

#[proc_macro_attribute]
pub fn stasis_tool(attr: TokenStream, item: TokenStream) -> TokenStream {
    let parser = Punctuated::<MetaNameValue, syn::Token![,]>::parse_terminated;
    let args = match parser.parse(attr) {
        Ok(args) => args,
        Err(err) => return err.to_compile_error().into(),
    };

    let mut tool_name: Option<LitStr> = None;
    let mut description: Option<LitStr> = None;
    let mut crate_path_literal: Option<LitStr> = None;
    let mut output_schema_enabled = false;

    for arg in args {
        if arg.path.is_ident("name") {
            match arg.value {
                Expr::Lit(ExprLit {
                    lit: Lit::Str(value),
                    ..
                }) => {
                    tool_name = Some(value);
                }
                _ => {
                    return syn::Error::new_spanned(arg.value, "name must be a string literal")
                        .to_compile_error()
                        .into();
                }
            }
            continue;
        }

        if arg.path.is_ident("description") {
            match arg.value {
                Expr::Lit(ExprLit {
                    lit: Lit::Str(value),
                    ..
                }) => {
                    description = Some(value);
                }
                _ => {
                    return syn::Error::new_spanned(
                        arg.value,
                        "description must be a string literal",
                    )
                    .to_compile_error()
                    .into();
                }
            }
            continue;
        }

        if arg.path.is_ident("crate_path") {
            match arg.value {
                Expr::Lit(ExprLit {
                    lit: Lit::Str(value),
                    ..
                }) => {
                    crate_path_literal = Some(value);
                }
                _ => {
                    return syn::Error::new_spanned(
                        arg.value,
                        "crate_path must be a string literal",
                    )
                    .to_compile_error()
                    .into();
                }
            }
            continue;
        }

        if arg.path.is_ident("output_schema") {
            match arg.value {
                Expr::Lit(ExprLit {
                    lit: Lit::Bool(value),
                    ..
                }) => {
                    output_schema_enabled = value.value;
                }
                _ => {
                    return syn::Error::new_spanned(
                        arg.value,
                        "output_schema must be a bool literal",
                    )
                    .to_compile_error()
                    .into();
                }
            }
            continue;
        }

        return syn::Error::new_spanned(
            arg.path,
            "unsupported attribute key (expected: name, description, crate_path, output_schema)",
        )
        .to_compile_error()
        .into();
    }

    let item_fn = syn::parse_macro_input!(item as ItemFn);
    let fn_ident = item_fn.sig.ident.clone();

    let tool_name = match tool_name {
        Some(name) => name,
        None => {
            return syn::Error::new_spanned(
                item_fn.sig.ident,
                "missing required attribute argument: name = \"...\"",
            )
            .to_compile_error()
            .into();
        }
    };

    if item_fn.sig.asyncness.is_none() {
        return syn::Error::new_spanned(item_fn.sig.fn_token, "stasis_tool function must be async")
            .to_compile_error()
            .into();
    }

    if !item_fn.sig.generics.params.is_empty() || item_fn.sig.generics.where_clause.is_some() {
        return syn::Error::new_spanned(
            item_fn.sig.generics,
            "stasis_tool functions must not use generics",
        )
        .to_compile_error()
        .into();
    }

    if item_fn.sig.inputs.len() != 1 {
        return syn::Error::new_spanned(
            item_fn.sig.inputs,
            "stasis_tool function must accept exactly one typed argument",
        )
        .to_compile_error()
        .into();
    }

    let input_ty = match item_fn.sig.inputs.first() {
        Some(FnArg::Typed(pat_ty)) => pat_ty.ty.clone(),
        Some(FnArg::Receiver(receiver)) => {
            return syn::Error::new_spanned(
                receiver,
                "stasis_tool function must not use a self receiver",
            )
            .to_compile_error()
            .into();
        }
        None => unreachable!(),
    };

    let output_ty = match extract_result_output_type(&item_fn.sig.output) {
        Ok(output) => output,
        Err(err) => return err.to_compile_error().into(),
    };

    let struct_name = format_ident!("{}Tool", to_pascal_case(&fn_ident.to_string()));
    let ctor_name = format_ident!("{}_tool", fn_ident);

    let crate_path_lit = crate_path_literal.unwrap_or_else(|| LitStr::new("stasis", fn_ident.span()));
    let crate_path = match syn::parse_str::<syn::Path>(&crate_path_lit.value()) {
        Ok(path) => path,
        Err(err) => return err.to_compile_error().into(),
    };

    let description_expr = match description {
        Some(value) => quote! { ::core::option::Option::Some(#value) },
        None => quote! { ::core::option::Option::None },
    };

    let output_schema_impl = if output_schema_enabled {
        quote! {
            fn output_schema(&self) -> ::core::option::Option<#crate_path::macro_support::serde_json::Value> {
                fn __assert_output_schema_traits<T: #crate_path::macro_support::schemars::JsonSchema>() {}
                __assert_output_schema_traits::<#output_ty>();

                let schema = #crate_path::macro_support::schemars::schema_for!(#output_ty);
                #crate_path::macro_support::serde_json::to_value(schema.schema).ok()
            }
        }
    } else {
        quote! {}
    };

    let expanded = quote! {
        #item_fn

        #[derive(Clone, Copy, Debug, Default)]
        pub struct #struct_name;

        #[#crate_path::macro_support::async_trait::async_trait]
        impl #crate_path::application::orchestration::tool_registry::StasisTool for #struct_name {
            fn name(&self) -> &'static str {
                #tool_name
            }

            fn description(&self) -> ::core::option::Option<&'static str> {
                #description_expr
            }

            fn input_schema(&self) -> ::core::option::Option<#crate_path::macro_support::serde_json::Value> {
                fn __assert_input_traits<T: #crate_path::macro_support::schemars::JsonSchema + #crate_path::macro_support::serde::de::DeserializeOwned>() {}
                __assert_input_traits::<#input_ty>();

                let schema = #crate_path::macro_support::schemars::schema_for!(#input_ty);
                #crate_path::macro_support::serde_json::to_value(schema.schema).ok()
            }

            #output_schema_impl

            async fn invoke(
                &self,
                input: #crate_path::macro_support::serde_json::Value,
            ) -> #crate_path::domain::errors::Result<#crate_path::macro_support::serde_json::Value> {
                fn __assert_output_traits<T: #crate_path::macro_support::serde::Serialize>() {}
                __assert_output_traits::<#output_ty>();

                let parsed_input: #input_ty = #crate_path::macro_support::serde_json::from_value(input).map_err(|err| {
                    #crate_path::domain::errors::StasisError::PortFailure(
                        format!("invalid input for tool '{}': {}", #tool_name, err)
                    )
                })?;

                let output: #output_ty = #fn_ident(parsed_input).await?;

                #crate_path::macro_support::serde_json::to_value(output).map_err(|err| {
                    #crate_path::domain::errors::StasisError::PortFailure(
                        format!("failed to serialize output for tool '{}': {}", #tool_name, err)
                    )
                })
            }
        }

        pub fn #ctor_name() -> #struct_name {
            #struct_name
        }
    };

    expanded.into()
}

fn extract_result_output_type(output: &ReturnType) -> syn::Result<Type> {
    let ReturnType::Type(_, ty) = output else {
        return Err(syn::Error::new_spanned(
            output,
            "stasis_tool function must return Result<OutputType>",
        ));
    };

    let Type::Path(type_path) = ty.as_ref() else {
        return Err(syn::Error::new_spanned(
            ty,
            "stasis_tool function must return Result<OutputType>",
        ));
    };

    let Some(segment) = type_path.path.segments.last() else {
        return Err(syn::Error::new_spanned(
            type_path,
            "unable to parse function return type",
        ));
    };

    if segment.ident != "Result" {
        return Err(syn::Error::new_spanned(
            segment,
            "stasis_tool function must return Result<OutputType>",
        ));
    }

    let PathArguments::AngleBracketed(args) = &segment.arguments else {
        return Err(syn::Error::new_spanned(
            segment,
            "stasis_tool function must return Result<OutputType>",
        ));
    };

    let Some(first_arg) = args.args.first() else {
        return Err(syn::Error::new_spanned(
            args,
            "stasis_tool function must return Result<OutputType>",
        ));
    };

    let syn::GenericArgument::Type(output_ty) = first_arg else {
        return Err(syn::Error::new_spanned(
            first_arg,
            "stasis_tool function must return Result<OutputType>",
        ));
    };

    Ok(output_ty.clone())
}

fn to_pascal_case(value: &str) -> String {
    let mut output = String::new();

    for part in value
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|part| !part.is_empty())
    {
        let mut chars = part.chars();
        if let Some(first) = chars.next() {
            output.push(first.to_ascii_uppercase());
            output.push_str(chars.as_str());
        }
    }

    if output.is_empty() {
        "StasisToolGenerated".to_string()
    } else {
        output
    }
}
