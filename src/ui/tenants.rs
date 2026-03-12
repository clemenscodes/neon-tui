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
        Cell::from("Default / Branch"),
        Cell::from("Timeline"),
    ])
    .style(
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    );

    // Build a flat list of rows: one tenant row + timeline sub-rows for each tenant.
    let mut rows: Vec<Row> = Vec::new();
    let mut flat_index: usize = 0;

    for tenant in &app.state.tenants {
        let default_marker = if tenant.is_default { "★" } else { "" };

        let tenant_row = Row::new(vec![
            Cell::from(tenant.id.clone()),
            Cell::from(default_marker),
            Cell::from(""),
        ]);

        let tenant_row = if flat_index == app.selected_index {
            tenant_row.style(
                Style::default()
                    .bg(Color::DarkGray)
                    .add_modifier(Modifier::BOLD),
            )
        } else {
            tenant_row
        };
        rows.push(tenant_row);
        flat_index += 1;

        for tl in &tenant.timelines {
            // Show first 12 chars of timeline id, truncated with "..."
            let tl_short = if tl.id.len() > 12 {
                format!("{}...", &tl.id[..12])
            } else {
                tl.id.clone()
            };
            let branch_col = tl
                .branch_name
                .as_deref()
                .unwrap_or("(no branch)")
                .to_string();

            let sub_row = Row::new(vec![
                Cell::from(format!("  \u{250b}\u{2501} {tl_short}")),
                Cell::from(branch_col),
                Cell::from(""),
            ]);

            let sub_row = if flat_index == app.selected_index {
                sub_row.style(
                    Style::default()
                        .bg(Color::DarkGray)
                        .add_modifier(Modifier::BOLD),
                )
            } else {
                sub_row.style(Style::default().fg(Color::DarkGray))
            };
            rows.push(sub_row);
            flat_index += 1;
        }
    }

    let widths = [
        ratatui::layout::Constraint::Length(36),
        ratatui::layout::Constraint::Length(20),
        ratatui::layout::Constraint::Min(12),
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
