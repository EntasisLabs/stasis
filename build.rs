use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    let input_path = "dashboard_assets/styles/input.css";
    let out_path = "dashboard_assets/static/dashboard.css";
    let tailwind_bin = PathBuf::from("node_modules/.bin/tailwindcss");

    println!("cargo:rerun-if-changed={input_path}");
    println!("cargo:rerun-if-changed=templates/dashboard");
    println!("cargo:rerun-if-changed={}", tailwind_bin.display());

    if let Some(parent) = Path::new(out_path).parent() {
        fs::create_dir_all(parent).expect("create dashboard static dir");
    }

    if tailwind_bin.exists() {
        match Command::new(&tailwind_bin)
            .args(["-i", input_path, "-o", out_path, "--minify"])
            .status()
        {
            Ok(status) if status.success() => return,
            Ok(_) => {
                println!(
                    "cargo:warning=tailwindcss exited with failure; using prebuilt {out_path} if present"
                );
            }
            Err(err) => {
                println!(
                    "cargo:warning=tailwindcss invocation failed ({err}); using prebuilt {out_path} if present"
                );
            }
        }
    } else {
        println!(
            "cargo:warning=tailwindcss not found at {}; using prebuilt {out_path}",
            tailwind_bin.display()
        );
    }

    assert!(
        Path::new(out_path).exists(),
        "tailwindcss unavailable and prebuilt {out_path} is missing"
    );
}
