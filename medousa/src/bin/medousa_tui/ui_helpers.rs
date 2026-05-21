use ratatui::{
    layout::{Constraint, Direction, Layout},
    style::Color,
};

pub(crate) fn ui_bg() -> Color {
    Color::Rgb(18, 22, 29)
}

pub(crate) fn ui_panel_bg() -> Color {
    Color::Rgb(26, 32, 41)
}

pub(crate) fn ui_subtle_panel_bg() -> Color {
    Color::Rgb(23, 28, 36)
}

pub(crate) fn ui_modal_bg() -> Color {
    Color::Rgb(31, 38, 49)
}

pub(crate) fn ui_border() -> Color {
    Color::Rgb(71, 89, 105)
}

pub(crate) fn ui_accent_primary() -> Color {
    Color::Rgb(64, 186, 213)
}

pub(crate) fn ui_accent_warn() -> Color {
    Color::Rgb(245, 189, 99)
}

pub(crate) fn centered_rect(
    area: ratatui::layout::Rect,
    width_percent: u16,
    height_percent: u16,
) -> ratatui::layout::Rect {
    let height_margin = 100u16.saturating_sub(height_percent);
    let width_margin = 100u16.saturating_sub(width_percent);

    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage(height_margin / 2),
            Constraint::Percentage(height_percent),
            Constraint::Percentage(height_margin - (height_margin / 2)),
        ])
        .split(area);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(width_margin / 2),
            Constraint::Percentage(width_percent),
            Constraint::Percentage(width_margin - (width_margin / 2)),
        ])
        .split(vertical[1])[1]
}
