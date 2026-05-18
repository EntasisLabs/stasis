use std::fs;
use std::path::Path;

fn main() {
    let scss_path = "dashboard_assets/styles/dashboard.scss";
    let out_path = "dashboard_assets/static/dashboard.css";

    println!("cargo:rerun-if-changed={scss_path}");

    let css = grass::from_path(
        scss_path,
        &grass::Options::default().style(grass::OutputStyle::Compressed),
    )
    .expect("compile dashboard scss");

    if let Some(parent) = Path::new(out_path).parent() {
        fs::create_dir_all(parent).expect("create dashboard static dir");
    }

    fs::write(out_path, css).expect("write compiled dashboard css");
}
