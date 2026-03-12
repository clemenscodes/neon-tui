use std::time::SystemTime;

use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::widgets::{Block, Borders, Cell, Row, Table, TableState};
use ratatui::Frame;

use crate::app::App;
use crate::neon::state::Status;

pub fn render(f: &mut Frame, app: &App, area: Rect) {
    if !app.state.initialized {
        let block = Block::default()
            .borders(Borders::ALL)
            .title(" Components ")
            .title_bottom(" Press I to initialize Neon ");
        f.render_widget(block, area);
        return;
    }

    let pid_header = if app.config.docker.mode { "Container" } else { "PID" };
    let header = Row::new(vec![
        Cell::from("Component"),
        Cell::from("Status"),
        Cell::from(pid_header),
        Cell::from("Port"),
        Cell::from("Uptime"),
    ])
    .style(
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    );

    let rows: Vec<Row> = app
        .state
        .components
        .iter()
        .enumerate()
        .map(|(i, comp)| {
            let status_style = match comp.status {
                Status::Up => Style::default().fg(Color::Green),
                Status::Down => Style::default().fg(Color::Red),
            };

            let uptime = comp
                .start_time
                .and_then(|t| SystemTime::now().duration_since(t).ok())
                .map(|d| format_duration(d.as_secs()))
                .unwrap_or_else(|| "-".to_string());

            let pid = if app.config.docker.mode {
                comp.docker_container
                    .as_deref()
                    .map(|name| short_container(name, &app.config.docker.compose_project))
                    .unwrap_or_else(|| "-".to_string())
            } else {
                comp.pid
                    .map(|p| p.to_string())
                    .unwrap_or_else(|| "-".to_string())
            };

            let row = Row::new(vec![
                Cell::from(comp.name.clone()),
                Cell::from(format!("{} {}", comp.status.symbol(), comp.status.label()))
                    .style(status_style),
                Cell::from(pid),
                Cell::from(comp.port.to_string()),
                Cell::from(uptime),
            ]);

            if i == app.selected_index {
                row.style(
                    Style::default()
                        .bg(Color::DarkGray)
                        .add_modifier(Modifier::BOLD),
                )
            } else {
                row
            }
        })
        .collect();

    let widths = [
        ratatui::layout::Constraint::Length(24),
        ratatui::layout::Constraint::Length(10),
        ratatui::layout::Constraint::Length(16),
        ratatui::layout::Constraint::Length(8),
        ratatui::layout::Constraint::Min(8),
    ];

    let table = Table::new(rows, widths)
        .header(header)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Components ")
                .title_bottom(" S start all | X stop all | Enter view logs | s start | x stop "),
        )
        .row_highlight_style(Style::default());

    let mut state = TableState::default();
    state.select(Some(app.selected_index));
    f.render_stateful_widget(table, area, &mut state);
}

/// Strip the Compose project prefix and numeric index suffix from a container name.
/// `eliteonlineshop-storage-broker-1` → `storage-broker`
fn short_container(name: &str, project: &str) -> String {
    let s = name
        .strip_prefix(&format!("{project}-"))
        .unwrap_or(name);
    // Strip trailing -N index
    if let Some(pos) = s.rfind('-') {
        if s[pos + 1..].chars().all(|c| c.is_ascii_digit()) {
            return s[..pos].to_string();
        }
    }
    s.to_string()
}

fn format_duration(secs: u64) -> String {
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else {
        let h = secs / 3600;
        let m = (secs % 3600) / 60;
        format!("{h}h {m}m")
    }
}
