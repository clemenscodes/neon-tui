use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Tabs};
use ratatui::Frame;

use crate::app::{App, Mode, Panel, View};
use crate::ui::{branches, components, dialogs, help, logs, tenants};

pub fn render(f: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Header bar
            Constraint::Min(1),    // Content
            Constraint::Length(1), // Status bar
        ])
        .split(f.area());

    render_header(f, app, chunks[0]);

    match app.view {
        View::Panels => match app.panel {
            Panel::Components => components::render(f, app, chunks[1]),
            Panel::Branches => branches::render(f, app, chunks[1]),
            Panel::Tenants => tenants::render(f, app, chunks[1]),
        },
        View::Logs => logs::render(f, app, chunks[1]),
    }

    render_status_bar(f, app, chunks[2]);

    // Overlays
    match app.mode {
        Mode::Help => help::render(f, app),
        Mode::Confirm => {
            if let Some(confirm) = &app.pending_confirm {
                dialogs::render_confirm(f, &confirm.message);
            }
        }
        Mode::Input => {
            let prompt = format!("New branch from '{}':", app.branch_parent);
            dialogs::render_input(f, &prompt, &app.branch_input);
        }
        _ => {}
    }
}

fn render_header(f: &mut Frame, app: &App, area: Rect) {
    let init_status = if app.state.initialized {
        Span::styled("initialized", Style::default().fg(Color::Green))
    } else {
        Span::styled("not initialized", Style::default().fg(Color::Red))
    };

    let ago = app
        .state
        .last_refresh
        .elapsed()
        .map(|d| format!("{}s ago", d.as_secs()))
        .unwrap_or_else(|_| "now".to_string());

    match app.view {
        View::Panels => {
            // Show panel tabs
            let titles: Vec<Line> = Panel::all()
                .iter()
                .map(|p| {
                    let style = if *p == app.panel {
                        Style::default()
                            .fg(Color::Cyan)
                            .add_modifier(Modifier::BOLD)
                    } else {
                        Style::default().fg(Color::DarkGray)
                    };
                    Line::from(Span::styled(format!(" {} ", p.label()), style))
                })
                .collect();

            let tabs = Tabs::new(titles)
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title(vec![
                            Span::styled(
                                " neon-tui ",
                                Style::default().add_modifier(Modifier::BOLD),
                            ),
                            Span::raw("| "),
                            init_status,
                            Span::raw(" "),
                        ])
                        .title_bottom(vec![Span::styled(
                            format!(" refreshed {ago}  h/l panels  Enter detail  ? help "),
                            Style::default().fg(Color::DarkGray),
                        )]),
                )
                .select(app.panel.index())
                .highlight_style(
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                );

            f.render_widget(tabs, area);
        }
        View::Logs => {
            let source_name = app.log_source_name();
            let panel_label = app.log_panel.label();

            let titles: Vec<Line> = match app.log_panel {
                Panel::Components => app
                    .state
                    .components
                    .iter()
                    .enumerate()
                    .map(|(i, c)| {
                        let style = if i == app.log_source {
                            Style::default()
                                .fg(Color::Cyan)
                                .add_modifier(Modifier::BOLD)
                        } else {
                            Style::default().fg(Color::DarkGray)
                        };
                        Line::from(Span::styled(format!(" {} ", c.name), style))
                    })
                    .collect(),
                Panel::Branches => app
                    .state
                    .branches
                    .iter()
                    .enumerate()
                    .map(|(i, b)| {
                        let style = if i == app.log_source {
                            Style::default()
                                .fg(Color::Cyan)
                                .add_modifier(Modifier::BOLD)
                        } else {
                            Style::default().fg(Color::DarkGray)
                        };
                        Line::from(Span::styled(format!(" {} ", b.name), style))
                    })
                    .collect(),
                Panel::Tenants => vec![],
            };

            let tabs = Tabs::new(titles)
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title(vec![
                            Span::styled(
                                " neon-tui ",
                                Style::default().add_modifier(Modifier::BOLD),
                            ),
                            Span::raw("› "),
                            Span::styled(
                                format!("{panel_label} › {source_name} logs"),
                                Style::default()
                                    .fg(Color::Cyan)
                                    .add_modifier(Modifier::BOLD),
                            ),
                            Span::raw(" | "),
                            init_status,
                            Span::raw(" "),
                        ])
                        .title_bottom(vec![Span::styled(
                            format!(" refreshed {ago}  Esc back  h/l source  f follow  ? help "),
                            Style::default().fg(Color::DarkGray),
                        )]),
                )
                .select(app.log_source)
                .highlight_style(
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                );

            f.render_widget(tabs, area);
        }
    }
}

fn render_status_bar(f: &mut Frame, app: &App, area: Rect) {
    let mode_span = match app.mode {
        Mode::Normal => Span::styled(
            " NORMAL ",
            Style::default()
                .bg(Color::Blue)
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
        Mode::Command => Span::styled(
            " COMMAND ",
            Style::default()
                .bg(Color::Yellow)
                .fg(Color::Black)
                .add_modifier(Modifier::BOLD),
        ),
        Mode::Input => Span::styled(
            " INPUT ",
            Style::default()
                .bg(Color::Green)
                .fg(Color::Black)
                .add_modifier(Modifier::BOLD),
        ),
        Mode::Confirm => Span::styled(
            " CONFIRM ",
            Style::default()
                .bg(Color::Red)
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
        Mode::Help => Span::styled(
            " HELP ",
            Style::default()
                .bg(Color::Magenta)
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
    };

    // Show view breadcrumb
    let view_span = match app.view {
        View::Panels => Span::styled(
            format!(" {} ", app.panel.label()),
            Style::default().fg(Color::DarkGray),
        ),
        View::Logs => {
            let name = app.log_source_name();
            let follow = if app.log_follow { " FOLLOW" } else { "" };
            Span::styled(
                format!(" {name}{follow} "),
                Style::default().fg(Color::DarkGray),
            )
        }
    };

    let middle = if app.mode == Mode::Command {
        Span::raw(format!(":{}", app.command_input))
    } else if app.is_busy() {
        let spinner = ['|', '/', '-', '\\'];
        let idx = (app
            .state
            .last_refresh
            .elapsed()
            .unwrap_or_default()
            .as_millis()
            / 200) as usize
            % spinner.len();
        let status = app.status_text().unwrap_or("Working...");
        Span::styled(
            format!(" {} {status}", spinner[idx]),
            Style::default().fg(Color::Yellow),
        )
    } else if let Some(status) = app.status_text() {
        Span::styled(format!(" {status}"), Style::default().fg(Color::Yellow))
    } else {
        Span::raw("")
    };

    let help_hint = Span::styled(" ? help  q quit ", Style::default().fg(Color::DarkGray));

    let line = Line::from(vec![mode_span, view_span, Span::raw(" "), middle]);
    f.render_widget(line, area);

    // Right-aligned help hint
    let hint_width = 16;
    if area.width > hint_width {
        let right_area = Rect::new(area.x + area.width - hint_width, area.y, hint_width, 1);
        f.render_widget(Line::from(help_hint), right_area);
    }
}
