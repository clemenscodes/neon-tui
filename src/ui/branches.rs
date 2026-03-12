use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::widgets::{Block, Borders, Cell, Row, Table, TableState};
use ratatui::Frame;

use crate::app::App;
use crate::neon::command;
use crate::neon::state::Status;

pub fn render(f: &mut Frame, app: &App, area: Rect) {
    if !app.state.initialized {
        let block = Block::default()
            .borders(Borders::ALL)
            .title(" Branches ")
            .title_bottom(" Press I to initialize Neon ");
        f.render_widget(block, area);
        return;
    }

    if app.state.branches.is_empty() {
        let block = Block::default()
            .borders(Borders::ALL)
            .title(" Branches ")
            .title_bottom(" No branches. Press n to create one. ");
        f.render_widget(block, area);
        return;
    }

    let header = Row::new(vec![
        Cell::from("Branch"),
        Cell::from("Status"),
        Cell::from("PG Port"),
        Cell::from("PID"),
        Cell::from("Connection URL"),
    ])
    .style(
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    );

    let rows: Vec<Row> = app
        .state
        .branches
        .iter()
        .enumerate()
        .map(|(i, branch)| {
            let status_style = match branch.status {
                Status::Up => Style::default().fg(Color::Green),
                Status::Down => Style::default().fg(Color::Red),
            };

            let depth = tree_depth(branch, &app.state.branches);
            let name = if depth == 0 {
                if branch.is_default {
                    format!("{} *", branch.name)
                } else {
                    branch.name.clone()
                }
            } else {
                let indent = "  ".repeat(depth - 1);
                if branch.is_default {
                    format!("{indent}┗━ {} *", branch.name)
                } else {
                    format!("{indent}┗━ {}", branch.name)
                }
            };

            let url = command::connection_url(&app.config, &branch.name);
            let pid = branch
                .pid
                .map(|p| p.to_string())
                .unwrap_or_else(|| "-".to_string());

            let row = Row::new(vec![
                Cell::from(name),
                Cell::from(format!(
                    "{} {}",
                    branch.status.symbol(),
                    branch.status.label()
                ))
                .style(status_style),
                Cell::from(branch.pg_port.to_string()),
                Cell::from(pid),
                Cell::from(url),
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
        ratatui::layout::Constraint::Length(30),
        ratatui::layout::Constraint::Length(10),
        ratatui::layout::Constraint::Length(9),
        ratatui::layout::Constraint::Length(8),
        ratatui::layout::Constraint::Min(30),
    ];

    let table = Table::new(rows, widths)
        .header(header)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Branches ")
                .title_bottom(" n new | d del | s start | x stop | c url | p psql | Enter logs "),
        )
        .row_highlight_style(Style::default());

    let mut state = TableState::default();
    state.select(Some(app.selected_index));
    f.render_stateful_widget(table, area, &mut state);
}

/// Compute how deep a branch is in the tree (0 = root, 1 = child of root, etc.)
fn tree_depth(
    branch: &crate::neon::state::BranchInfo,
    all: &[crate::neon::state::BranchInfo],
) -> usize {
    let mut depth = 0;
    let mut current = branch;
    while let Some(parent_name) = &current.parent {
        depth += 1;
        if let Some(parent) = all.iter().find(|b| &b.name == parent_name) {
            current = parent;
        } else {
            break;
        }
    }
    depth
}
