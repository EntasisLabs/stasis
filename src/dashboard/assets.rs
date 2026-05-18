use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "dashboard_assets/static"]
pub struct DashboardAssets;

pub fn load(path: &str) -> Option<(Vec<u8>, &'static str)> {
    let file = DashboardAssets::get(path)?;
    let content_type = match path.rsplit('.').next() {
        Some("css") => "text/css; charset=utf-8",
        Some("js") => "application/javascript; charset=utf-8",
        Some("svg") => "image/svg+xml",
        _ => "application/octet-stream",
    };

    Some((file.data.to_vec(), content_type))
}
