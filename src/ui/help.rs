use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph};
use ratatui::Frame;

use crate::app::App;

pub fn render(f: &mut Frame, _app: &App) {
    let area = centered_rect(70, 80, f.area());
    f.render_widget(Clear, area);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan))
        .title(Span::styled(
            " Keybindings ",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ))
        .title_bottom(" Press ? or Esc to close ");

    let inner = block.inner(area);
    f.render_widget(block, area);

    let sections = vec![
        section(
            "Global",
            &[
                ("q / Ctrl-c", "Quit"),
                ("?", "Toggle help"),
                ("r", "Refresh state"),
                (":", "Command mode"),
            ],
        ),
        section(
            "Navigation",
            &[
                ("h / Left", "Go left (prev panel / prev log source)"),
                ("l / Right", "Go right (next panel / next log source)"),
                ("j / Down", "Move down / scroll down"),
                ("k / Up", "Move up / scroll up"),
                ("gg", "Jump to first / scroll to top"),
                ("G", "Jump to last / scroll to bottom"),
                ("Ctrl-d", "Half page down"),
                ("Ctrl-u", "Half page up"),
                ("Enter", "View logs (components/branches)"),
                ("Esc", "Go back one layer"),
            ],
        ),
        section(
            "Components Panel",
            &[
                ("I", "Init Neon"),
                ("S", "Start all"),
                ("X", "Stop all"),
                ("s", "Start selected"),
                ("x", "Stop selected"),
                ("Enter", "View component logs"),
            ],
        ),
        section(
            "Branches Panel",
            &[
                ("n", "New branch"),
                ("d", "Delete branch"),
                ("s", "Start endpoint"),
                ("x", "Stop endpoint"),
                ("c", "Copy connection URL"),
                ("p", "Open psql"),
                ("Enter", "View endpoint logs"),
            ],
        ),
        section("Tenants Panel", &[("(read-only)", "View tenant info")]),
        section(
            "Logs View",
            &[
                ("h / l", "Switch log source"),
                ("f", "Toggle follow mode"),
                ("j / k", "Scroll up / down"),
                ("gg / G", "Top / bottom (follow)"),
                ("Esc", "Back to panel"),
            ],
        ),
        section(
            "Command Mode (:)",
            &[
                (":init", "Initialize Neon"),
                (":start / :stop", "Start / stop all"),
                (":destroy", "Destroy all data"),
                (":branch <name>", "Create branch"),
                (":branch <n> --from <p>", "Branch from parent"),
                (":delete <name>", "Delete branch"),
                (":switch <name>", "Start endpoint"),
                (":url [branch]", "Show connection URL"),
                (":q / :quit", "Quit"),
            ],
        ),
    ];

    let mut lines: Vec<Line> = Vec::new();
    for (title, entries) in sections {
        lines.push(Line::raw(""));
        lines.push(Line::from(Span::styled(
            format!("  {title}"),
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )));
        for (key, desc) in entries {
            lines.push(Line::from(vec![
                Span::raw("    "),
                Span::styled(
                    format!("{key:<26}"),
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw(desc),
            ]));
        }
    }

    let paragraph = Paragraph::new(lines).scroll((0, 0));
    f.render_widget(paragraph, inner);
}

fn section<'a>(
    title: &'a str,
    entries: &[(&'a str, &'a str)],
) -> (&'a str, Vec<(&'a str, &'a str)>) {
    (title, entries.to_vec())
}

fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}
