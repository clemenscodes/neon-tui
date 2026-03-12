mod app;
mod config;
mod neon;
mod ui;

use std::io;
use std::time::Duration;

use clap::{Parser, Subcommand};
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use crossterm::ExecutableCommand;
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;

use app::{Action, App, Mode, View};
use config::{CliOverrides, Config};

#[derive(Parser)]
#[command(
    name = "neon-tui",
    about = "Terminal UI and CLI for local Neon (serverless Postgres)"
)]
#[command(version)]
struct Cli {
    /// Path to config file
    #[arg(short, long, global = true)]
    config: Option<std::path::PathBuf>,

    /// Neon data directory
    #[arg(short, long, global = true)]
    dir: Option<std::path::PathBuf>,

    /// Neon binary directory
    #[arg(short, long, global = true)]
    bin_dir: Option<std::path::PathBuf>,

    /// Default compute port
    #[arg(long, global = true)]
    port: Option<u16>,

    /// Default compute host
    #[arg(long, global = true)]
    host: Option<String>,

    /// Enable Docker Compose detection mode (e.g. when Neon runs in docker compose)
    #[arg(long, global = true)]
    docker: bool,

    /// Docker Compose project name (default: auto-detected from cwd)
    #[arg(long, global = true)]
    docker_project: Option<String>,

    /// Pageserver HTTP port (default: 9898)
    #[arg(long, global = true)]
    pageserver_port: Option<u16>,

    /// Safekeeper PG port (default: 5454)
    #[arg(long, global = true)]
    safekeeper_port: Option<u16>,

    /// Storage broker port (default: 50051)
    #[arg(long, global = true)]
    broker_port: Option<u16>,

    /// Compute database user (default: test)
    #[arg(long, global = true)]
    user: Option<String>,

    /// Compute database password (embedded in connection URLs; also read from COMPUTE_PASSWORD)
    #[arg(long, global = true)]
    password: Option<String>,

    /// Compute database name (default: neondb)
    #[arg(long, global = true)]
    database: Option<String>,

    /// Default branch / endpoint name (default: main)
    #[arg(long, global = true)]
    branch: Option<String>,

    /// PostgreSQL version (default: 17)
    #[arg(long, global = true)]
    pg_version: Option<u16>,

    /// TUI refresh interval in seconds (default: 2)
    #[arg(long, global = true)]
    refresh: Option<u64>,

    /// Start directly in log view
    #[arg(long, global = true)]
    show_logs: bool,

    #[command(subcommand)]
    command: Option<SubCmd>,
}

#[derive(Subcommand)]
enum SubCmd {
    /// Initialize Neon repository (.neon/)
    Init,
    /// Start all services + default endpoint
    Start,
    /// Stop all services
    Stop,
    /// Show timelines and endpoints
    Status,
    /// Create a database branch
    Branch {
        /// Branch name
        name: String,
        /// Parent branch to fork from
        #[arg(long, default_value = "main")]
        from: String,
    },
    /// Start/switch to a branch endpoint
    Switch {
        /// Branch name
        name: String,
    },
    /// Stop and remove a branch endpoint
    Delete {
        /// Branch name
        name: String,
    },
    /// Connect with psql
    Psql {
        /// Branch name (defaults to main)
        branch: Option<String>,
    },
    /// Print DATABASE_URL for a branch
    Url {
        /// Branch name (defaults to main)
        branch: Option<String>,
    },
    /// Wipe all data (full reset)
    Destroy {
        /// Skip confirmation prompt
        #[arg(short = 'y', long)]
        yes: bool,
    },
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let cli = Cli::parse();

    let overrides = CliOverrides {
        config_path: cli.config,
        repo_dir: cli.dir,
        bin_dir: cli.bin_dir,
        host: cli.host,
        port: cli.port,
        docker_mode: if cli.docker { Some(true) } else { None },
        docker_project: cli.docker_project,
        pageserver_port: cli.pageserver_port,
        safekeeper_port: cli.safekeeper_port,
        broker_port: cli.broker_port,
        user: cli.user,
        password: cli.password,
        database: cli.database,
        default_branch: cli.branch,
        pg_version: cli.pg_version,
        refresh_interval: cli.refresh,
        show_logs: if cli.show_logs { Some(true) } else { None },
    };

    let config = Config::load(&overrides);

    match cli.command {
        Some(cmd) => run_cli(cmd, config).await,
        None => run_tui(config).await,
    }
}

// ── CLI mode ─────────────────────────────────────────────────────────────────

async fn run_cli(cmd: SubCmd, config: Config) -> io::Result<()> {
    use neon::command;

    let result = match cmd {
        SubCmd::Init => command::init(&config).await,
        SubCmd::Start => command::start(&config).await,
        SubCmd::Stop => command::stop(&config).await,
        SubCmd::Status => command::status(&config).await,
        SubCmd::Branch { name, from } => command::create_branch(&config, &name, &from).await,
        SubCmd::Switch { name } => command::start_endpoint(&config, &name).await,
        SubCmd::Delete { name } => command::delete_branch(&config, &name).await,
        SubCmd::Psql { branch } => {
            let branch = branch.as_deref().unwrap_or(&config.compute.default_branch);
            let url = command::connection_url(&config, branch);
            let status = std::process::Command::new("psql").arg(&url).status()?;
            if status.success() {
                return Ok(());
            } else {
                return Err(io::Error::other("psql exited with an error"));
            }
        }
        SubCmd::Url { branch } => {
            let branch = branch.as_deref().unwrap_or(&config.compute.default_branch);
            let url = command::connection_url(&config, branch);
            println!("{url}");
            return Ok(());
        }
        SubCmd::Destroy { yes } => {
            if !yes {
                eprint!(
                    "This will permanently delete all local Neon data at {}. Are you sure? [y/N] ",
                    config.neon.repo_dir.display()
                );
                let mut input = String::new();
                io::stdin().read_line(&mut input)?;
                if !input.trim().eq_ignore_ascii_case("y") {
                    eprintln!("Aborted.");
                    return Ok(());
                }
            }
            command::destroy(&config).await
        }
    };

    if result.success {
        if !result.stdout.is_empty() {
            println!("{}", result.stdout.trim_end());
        }
        Ok(())
    } else {
        eprintln!("Error: {}", result.stderr.trim_end());
        std::process::exit(1);
    }
}

// ── TUI mode ─────────────────────────────────────────────────────────────────

async fn run_tui(config: Config) -> io::Result<()> {
    let mut app = App::new(config);

    enable_raw_mode()?;
    io::stdout().execute(EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(io::stdout());
    let mut terminal = Terminal::new(backend)?;

    let result = run_event_loop(&mut terminal, &mut app).await;

    disable_raw_mode()?;
    io::stdout().execute(LeaveAlternateScreen)?;

    result
}

async fn run_event_loop(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &mut App,
) -> io::Result<()> {
    let refresh_interval = Duration::from_secs(app.config.ui.refresh_interval_secs);
    let log_refresh_interval = Duration::from_millis(500);
    let mut last_refresh = std::time::Instant::now();
    let mut last_log_refresh = std::time::Instant::now();

    while app.running {
        app.poll_bg_task();
        terminal.draw(|f| ui::dashboard::render(f, app))?;

        let timeout = if app.is_busy() {
            // Poll frequently while a background command is running.
            Duration::from_millis(200)
        } else if app.in_logs() && app.log_follow {
            log_refresh_interval
                .checked_sub(last_log_refresh.elapsed())
                .unwrap_or(Duration::ZERO)
                .min(
                    refresh_interval
                        .checked_sub(last_refresh.elapsed())
                        .unwrap_or(Duration::ZERO),
                )
        } else {
            refresh_interval
                .checked_sub(last_refresh.elapsed())
                .unwrap_or(Duration::ZERO)
        };

        if event::poll(timeout)? {
            if let Event::Key(key) = event::read()? {
                let action = map_key_event(key, app);

                if matches!(action, Action::OpenPsql) {
                    if let Some(url) = app.selected_branch_psql_url() {
                        disable_raw_mode()?;
                        io::stdout().execute(LeaveAlternateScreen)?;

                        let _ = std::process::Command::new("psql").arg(&url).status();

                        enable_raw_mode()?;
                        io::stdout().execute(EnterAlternateScreen)?;
                        terminal.clear()?;
                        app.refresh();
                        continue;
                    }
                }

                app.handle_action(action);
            }
        } else if last_refresh.elapsed() >= refresh_interval {
            app.refresh();
            last_refresh = std::time::Instant::now();
            last_log_refresh = std::time::Instant::now();
        } else if app.in_logs() && last_log_refresh.elapsed() >= log_refresh_interval {
            app.refresh_logs();
            last_log_refresh = std::time::Instant::now();
        }
    }

    Ok(())
}

fn map_key_event(key: KeyEvent, app: &mut App) -> Action {
    match app.mode {
        Mode::Normal => map_normal_mode(key, app),
        Mode::Command => map_command_mode(key, app),
        Mode::Input => map_input_mode(key, app),
        Mode::Confirm => map_confirm_mode(key),
        Mode::Help => map_help_mode(key),
    }
}

fn map_normal_mode(key: KeyEvent, app: &mut App) -> Action {
    if app.g_pressed {
        app.g_pressed = false;
        if key.code == KeyCode::Char('g') {
            if app.in_logs() {
                app.log_follow = false;
                app.log_scroll = 0;
                return Action::None;
            }
            return Action::JumpTop;
        }
    }

    match app.view {
        View::Panels => map_panels_mode(key, app),
        View::Logs => map_logs_mode(key, app),
    }
}

fn map_panels_mode(key: KeyEvent, app: &mut App) -> Action {
    match key.code {
        KeyCode::Char('q') => Action::Quit,
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => Action::Quit,

        KeyCode::Char('?') => Action::ToggleHelp,

        KeyCode::Char('h') | KeyCode::Left => Action::NavLeft,
        KeyCode::Char('l') | KeyCode::Right => Action::NavRight,

        KeyCode::Char('j') | KeyCode::Down => Action::MoveDown,
        KeyCode::Char('k') | KeyCode::Up => Action::MoveUp,
        KeyCode::Char('g') => {
            app.g_pressed = true;
            Action::None
        }
        KeyCode::Char('G') => Action::JumpBottom,
        KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => Action::HalfPageDown,
        KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => Action::HalfPageUp,

        KeyCode::Enter => Action::Enter,

        KeyCode::Char('r') => Action::Refresh,
        KeyCode::Char(':') => Action::EnterCommandMode,

        KeyCode::Char('I') => Action::InitNeon,
        KeyCode::Char('D') => Action::DestroyNeon,
        KeyCode::Char('S') => Action::StartAll,
        KeyCode::Char('X') => Action::StopAll,
        KeyCode::Char('s') => Action::StartSelected,
        KeyCode::Char('x') => Action::StopSelected,

        KeyCode::Char('n') if app.panel == app::Panel::Branches => Action::NewBranch,
        KeyCode::Char('d') if app.panel == app::Panel::Branches => Action::DeleteSelected,
        KeyCode::Char('c') if app.panel == app::Panel::Branches => Action::CopyUrl,
        KeyCode::Char('p') if app.panel == app::Panel::Branches => Action::OpenPsql,

        _ => Action::None,
    }
}

fn map_logs_mode(key: KeyEvent, app: &mut App) -> Action {
    match key.code {
        KeyCode::Char('q') => Action::Quit,
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => Action::Quit,

        KeyCode::Char('?') => Action::ToggleHelp,

        KeyCode::Esc => Action::Back,

        KeyCode::Char('h') | KeyCode::Left => Action::NavLeft,
        KeyCode::Char('l') | KeyCode::Right => Action::NavRight,

        KeyCode::Char('j') | KeyCode::Down => {
            if app.log_follow {
                // Already following — j is a no-op (we're at the bottom).
            } else {
                app.log_scroll = app.log_scroll.saturating_add(1);
            }
            Action::None
        }
        KeyCode::Char('k') | KeyCode::Up => {
            if app.log_follow {
                app.log_follow = false;
                // Start one line above the bottom so the first press visibly scrolls up.
                app.log_scroll = app.log_lines.len().saturating_sub(2);
            } else {
                app.log_scroll = app.log_scroll.saturating_sub(1);
            }
            Action::None
        }
        KeyCode::Char('g') => {
            app.g_pressed = true;
            Action::None
        }
        KeyCode::Char('G') => {
            app.log_follow = true;
            app.log_scroll = app.log_lines.len();
            Action::None
        }
        KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            if app.log_follow {
                app.log_follow = false;
                app.log_scroll = app.log_lines.len();
            } else {
                app.log_scroll = app.log_scroll.saturating_add(20);
            }
            Action::None
        }
        KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            if app.log_follow {
                app.log_follow = false;
                app.log_scroll = app.log_lines.len().saturating_sub(20);
            } else {
                app.log_scroll = app.log_scroll.saturating_sub(20);
            }
            Action::None
        }

        KeyCode::Char('f') => Action::ToggleLogFollow,
        KeyCode::Char('r') => Action::Refresh,
        KeyCode::Char(':') => Action::EnterCommandMode,

        _ => Action::None,
    }
}

fn map_command_mode(key: KeyEvent, app: &mut App) -> Action {
    match key.code {
        KeyCode::Esc => {
            app.mode = Mode::Normal;
            app.command_input.clear();
            Action::None
        }
        KeyCode::Enter => {
            let cmd = app.command_input.clone();
            app.command_input.clear();
            Action::ExecCommand(cmd)
        }
        KeyCode::Backspace => {
            app.command_input.pop();
            if app.command_input.is_empty() {
                app.mode = Mode::Normal;
            }
            Action::None
        }
        KeyCode::Char(c) => {
            app.command_input.push(c);
            Action::None
        }
        _ => Action::None,
    }
}

fn map_input_mode(key: KeyEvent, app: &mut App) -> Action {
    match key.code {
        KeyCode::Esc => {
            app.mode = Mode::Normal;
            app.branch_input.clear();
            Action::None
        }
        KeyCode::Enter => {
            let name = app.branch_input.clone();
            let parent = app.branch_parent.clone();
            app.branch_input.clear();
            app.mode = Mode::Normal;
            if name.is_empty() {
                app.set_status("Branch name cannot be empty.");
                Action::None
            } else {
                Action::ExecCommand(format!("branch {name} --from {parent}"))
            }
        }
        KeyCode::Backspace => {
            app.branch_input.pop();
            Action::None
        }
        KeyCode::Char(c) => {
            if c.is_alphanumeric() || c == '-' || c == '_' || c == '/' {
                app.branch_input.push(c);
            }
            Action::None
        }
        _ => Action::None,
    }
}

fn map_confirm_mode(key: KeyEvent) -> Action {
    match key.code {
        KeyCode::Char('y') | KeyCode::Char('Y') => Action::ConfirmYes,
        KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => Action::ConfirmNo,
        _ => Action::None,
    }
}

fn map_help_mode(key: KeyEvent) -> Action {
    match key.code {
        KeyCode::Char('?') | KeyCode::Esc | KeyCode::Char('q') => Action::ToggleHelp,
        _ => Action::None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_parses_no_args() {
        let cli = Cli::try_parse_from(["neon-tui"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn cli_parses_all_args() {
        let cli = Cli::try_parse_from([
            "neon-tui",
            "-d",
            "/tmp/.neon",
            "--port",
            "55432",
            "-b",
            "/usr/bin",
        ]);
        let cli = cli.unwrap();
        assert_eq!(cli.dir.unwrap().to_str().unwrap(), "/tmp/.neon");
        assert_eq!(cli.port.unwrap(), 55432);
        assert_eq!(cli.bin_dir.unwrap().to_str().unwrap(), "/usr/bin");
    }

    #[test]
    fn cli_parses_init_subcommand() {
        let cli = Cli::try_parse_from(["neon-tui", "init"]);
        assert!(cli.is_ok());
        assert!(matches!(cli.unwrap().command, Some(SubCmd::Init)));
    }

    #[test]
    fn cli_parses_branch_subcommand() {
        let cli = Cli::try_parse_from(["neon-tui", "branch", "feat-auth", "--from", "main"]);
        let cli = cli.unwrap();
        match cli.command {
            Some(SubCmd::Branch { name, from }) => {
                assert_eq!(name, "feat-auth");
                assert_eq!(from, "main");
            }
            _ => panic!("expected Branch subcommand"),
        }
    }

    #[test]
    fn cli_parses_destroy_with_yes() {
        let cli = Cli::try_parse_from(["neon-tui", "destroy", "-y"]);
        let cli = cli.unwrap();
        assert!(matches!(cli.command, Some(SubCmd::Destroy { yes: true })));
    }

    #[test]
    fn cli_subcommand_with_global_args() {
        let cli = Cli::try_parse_from(["neon-tui", "-d", "/tmp/.neon", "start"]);
        let cli = cli.unwrap();
        assert_eq!(cli.dir.unwrap().to_str().unwrap(), "/tmp/.neon");
        assert!(matches!(cli.command, Some(SubCmd::Start)));
    }

    #[test]
    fn cli_parses_docker_flag() {
        let cli = Cli::try_parse_from([
            "neon-tui",
            "--docker",
            "--docker-project",
            "eliteonlineshop",
        ]);
        let cli = cli.unwrap();
        assert!(cli.docker);
        assert_eq!(cli.docker_project.unwrap(), "eliteonlineshop");
    }

    #[test]
    fn cli_parses_all_flags() {
        let cli = Cli::try_parse_from([
            "neon-tui",
            "--docker",
            "--docker-project",
            "myproject",
            "--pageserver-port",
            "9999",
            "--safekeeper-port",
            "5555",
            "--broker-port",
            "50052",
            "--user",
            "alice",
            "--database",
            "mydb",
            "--branch",
            "dev",
            "--pg-version",
            "16",
            "--refresh",
            "5",
            "--show-logs",
        ]);
        let cli = cli.unwrap();
        assert!(cli.docker);
        assert_eq!(cli.docker_project.unwrap(), "myproject");
        assert_eq!(cli.pageserver_port.unwrap(), 9999);
        assert_eq!(cli.safekeeper_port.unwrap(), 5555);
        assert_eq!(cli.broker_port.unwrap(), 50052);
        assert_eq!(cli.user.unwrap(), "alice");
        assert_eq!(cli.database.unwrap(), "mydb");
        assert_eq!(cli.branch.unwrap(), "dev");
        assert_eq!(cli.pg_version.unwrap(), 16);
        assert_eq!(cli.refresh.unwrap(), 5);
        assert!(cli.show_logs);
    }
}
