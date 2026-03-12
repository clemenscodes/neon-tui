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

    let header = Row::new(vec![
        Cell::from("Component"),
        Cell::from("Status"),
        Cell::from("PID"),
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

            let pid = comp
                .pid
                .map(|p| p.to_string())
                .unwrap_or_else(|| "-".to_string());

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
        ratatui::layout::Constraint::Length(8),
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
