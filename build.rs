use std::fs;
use std::path::Path;
use std::process::Command;

fn main() {
    let input_path = "dashboard_assets/styles/input.css";
    let out_path = "dashboard_assets/static/dashboard.css";

    println!("cargo:rerun-if-changed={input_path}");
    println!("cargo:rerun-if-changed=templates/dashboard");

    if let Some(parent) = Path::new(out_path).parent() {
        fs::create_dir_all(parent).expect("create dashboard static dir");
    }

    let status = Command::new("node_modules/.bin/tailwindcss")
        .args(["-i", input_path, "-o", out_path, "--minify"])
        .status()
        .expect("failed to run tailwindcss — run `npm install` first");

    assert!(status.success(), "tailwindcss exited with failure");
}
