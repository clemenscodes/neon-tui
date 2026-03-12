use ratatui::layout::Rect;
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};
use ratatui::Frame;

use crate::app::App;

pub fn render(f: &mut Frame, app: &App, area: Rect) {
    let follow_indicator = if app.log_follow {
        Span::styled(" FOLLOW ", Style::default().fg(Color::Green))
    } else {
        Span::styled(" SCROLL ", Style::default().fg(Color::Yellow))
    };

    let log_name = app
        .state
        .components
        .get(app.log_source)
        .map(|c| c.name.as_str())
        .unwrap_or("unknown");

    let visible_height = area.height.saturating_sub(2) as usize;
    let total_lines = app.log_lines.len();

    let scroll_offset = if app.log_follow {
        total_lines.saturating_sub(visible_height)
    } else {
        app.log_scroll
            .min(total_lines.saturating_sub(visible_height))
    };

    let visible_lines: Vec<Line> = app
        .log_lines
        .iter()
        .skip(scroll_offset)
        .take(visible_height)
        .map(|line| Line::raw(line.as_str()))
        .collect();

    let paragraph = Paragraph::new(visible_lines).block(
        Block::default()
            .borders(Borders::ALL)
            .title(vec![Span::raw(format!(" {log_name} ")), follow_indicator])
            .title_bottom(" Esc back | h/l source | f follow | j/k scroll | gg/G top/bottom "),
    );

    f.render_widget(paragraph, area);
}
