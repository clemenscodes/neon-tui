use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::widgets::{Block, Borders, Cell, Row, Table, TableState};
use ratatui::Frame;

use crate::app::App;

pub fn render(f: &mut Frame, app: &App, area: Rect) {
    if !app.state.initialized {
        let block = Block::default()
            .borders(Borders::ALL)
            .title(" Tenants ")
            .title_bottom(" Press I to initialize Neon ");
        f.render_widget(block, area);
        return;
    }

    if app.state.tenants.is_empty() {
        let block = Block::default()
            .borders(Borders::ALL)
            .title(" Tenants ")
            .title_bottom(" No tenants found ");
        f.render_widget(block, area);
        return;
    }

    let header = Row::new(vec![
        Cell::from("Tenant ID"),
        Cell::from("Default"),
        Cell::from("Timelines"),
    ])
    .style(
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    );

    let rows: Vec<Row> = app
        .state
        .tenants
        .iter()
        .enumerate()
        .map(|(i, tenant)| {
            let default_marker = if tenant.is_default { "★" } else { "" };

            let row = Row::new(vec![
                Cell::from(tenant.id.clone()),
                Cell::from(default_marker),
                Cell::from(tenant.timelines.to_string()),
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
        ratatui::layout::Constraint::Length(36),
        ratatui::layout::Constraint::Length(9),
        ratatui::layout::Constraint::Min(10),
    ];

    let table = Table::new(rows, widths)
        .header(header)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Tenants ")
                .title_bottom(" Tenant management (read-only) "),
        )
        .row_highlight_style(Style::default());

    let mut state = TableState::default();
    state.select(Some(app.selected_index));
    f.render_stateful_widget(table, area, &mut state);
}
