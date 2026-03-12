use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::config::Config;
use crate::neon::{command, docker};
use crate::neon::state::{self, NeonState};

/// The main panels at the top level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Panel {
    Components,
    Branches,
    Tenants,
}

impl Panel {
    pub fn all() -> &'static [Panel] {
        &[Panel::Components, Panel::Branches, Panel::Tenants]
    }

    pub fn label(&self) -> &'static str {
        match self {
            Panel::Components => "Components",
            Panel::Branches => "Branches",
            Panel::Tenants => "Tenants",
        }
    }

    pub fn index(&self) -> usize {
        match self {
            Panel::Components => 0,
            Panel::Branches => 1,
            Panel::Tenants => 2,
        }
    }

    pub fn next(&self) -> Panel {
        match self {
            Panel::Components => Panel::Branches,
            Panel::Branches => Panel::Tenants,
            Panel::Tenants => Panel::Tenants,
        }
    }

    pub fn prev(&self) -> Panel {
        match self {
            Panel::Components => Panel::Components,
            Panel::Branches => Panel::Components,
            Panel::Tenants => Panel::Branches,
        }
    }
}

/// What the user is currently viewing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum View {
    /// Top-level panel list (Components or Branches).
    Panels,
    /// Viewing logs of a component (entered via Enter from Components).
    Logs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Normal,
    Command,
    Input,
    Confirm,
    Help,
}

#[derive(Debug, Clone)]
pub enum Action {
    Quit,
    Refresh,
    NavLeft,
    NavRight,
    MoveDown,
    MoveUp,
    JumpTop,
    JumpBottom,
    HalfPageDown,
    HalfPageUp,
    Enter,
    Back,
    StartSelected,
    StopSelected,
    DeleteSelected,
    CopyUrl,
    OpenPsql,
    NewBranch,
    StartAll,
    StopAll,
    InitNeon,
    DestroyNeon,
    ToggleHelp,
    EnterCommandMode,
    ExecCommand(String),
    ConfirmYes,
    ConfirmNo,
    ToggleLogFollow,
    None,
}

#[derive(Debug, Clone)]
pub struct PendingConfirm {
    pub message: String,
    pub action: Box<ConfirmAction>,
}

#[derive(Debug, Clone)]
pub enum ConfirmAction {
    DeleteBranch(String),
    StopAll,
    DestroyNeon,
    DeleteTimeline(String, String),
}

pub struct App {
    pub config: Config,
    pub state: NeonState,
    pub panel: Panel,
    pub view: View,
    pub mode: Mode,
    pub running: bool,
    pub selected_index: usize,
    pub command_input: String,
    pub branch_input: String,
    pub branch_parent: String,
    pub pending_confirm: Option<PendingConfirm>,
    pub status_message: Option<(String, Instant)>,
    /// Which panel we entered logs from (Components or Branches).
    pub log_panel: Panel,
    pub log_source: usize,
    pub log_follow: bool,
    pub log_scroll: usize,
    pub log_lines: Vec<String>,
    pub g_pressed: bool,
    /// Receiver for background command results (keeps UI responsive).
    pub bg_result_rx: Option<tokio::sync::oneshot::Receiver<BgResult>>,
    /// Cached timeline hierarchy (branch_name → parent_name).
    /// Updated from a background thread to avoid blocking the UI.
    cached_hierarchy: HashMap<String, String>,
    /// Handle to the background thread that refreshes the timeline hierarchy.
    hierarchy_thread: Option<std::thread::JoinHandle<HashMap<String, String>>>,
}

/// Result from a background command: (ok_message, error_prefix, command_result).
pub type BgResult = (String, String, command::CommandResult);

impl App {
    pub fn new(config: Config) -> Self {
        let state = state::read_state(&config);
        let (panel, view) = if config.ui.show_logs {
            (Panel::Components, View::Logs)
        } else {
            (Panel::Components, View::Panels)
        };
        Self {
            config,
            state,
            panel,
            view,
            mode: Mode::Normal,
            running: true,
            selected_index: 0,
            command_input: String::new(),
            branch_input: String::new(),
            branch_parent: String::new(),
            pending_confirm: None,
            status_message: None,
            log_panel: Panel::Components,
            log_source: 0,
            log_follow: true,
            log_scroll: 0,
            log_lines: Vec::new(),
            g_pressed: false,
            bg_result_rx: None,
            cached_hierarchy: HashMap::new(),
            hierarchy_thread: None,
        }
    }

    pub fn refresh(&mut self) {
        self.state = state::read_state(&self.config);

        // Check if the background hierarchy thread has finished.
        if let Some(handle) = &self.hierarchy_thread {
            if handle.is_finished() {
                let handle = self.hierarchy_thread.take().unwrap();
                if let Ok(hierarchy) = handle.join() {
                    self.cached_hierarchy = hierarchy;
                }
            }
        }

        // Apply cached hierarchy to branches.
        self.apply_cached_hierarchy();

        // Spawn a new hierarchy refresh if none is running and neon is initialized.
        if self.hierarchy_thread.is_none() && self.state.initialized {
            let config = self.config.clone();
            self.hierarchy_thread = Some(std::thread::spawn(move || {
                command::parse_timeline_hierarchy(&config)
            }));
        }

        self.refresh_logs();
    }

    /// Apply the cached timeline hierarchy to branches and sort them.
    fn apply_cached_hierarchy(&mut self) {
        if self.cached_hierarchy.is_empty() {
            return;
        }
        for branch in &mut self.state.branches {
            branch.parent = self.cached_hierarchy.get(&branch.name).cloned();
        }
        if !self.state.branches.is_empty() {
            state::sort_branches_by_tree(&mut self.state.branches);
        }
    }

    pub fn list_len(&self) -> usize {
        match self.panel {
            Panel::Components => self.state.components.len(),
            Panel::Branches => self.state.branches.len(),
            Panel::Tenants => self.state.tenants.iter().map(|t| 1 + t.timelines.len()).sum(),
        }
    }

    pub fn set_status(&mut self, msg: impl Into<String>) {
        self.status_message = Some((msg.into(), Instant::now()));
    }

    fn set_command_status(
        &mut self,
        result: &command::CommandResult,
        ok_msg: &str,
        err_prefix: &str,
    ) {
        if result.success {
            let detail = result.stdout.lines().next().unwrap_or("").trim();
            if detail.is_empty() {
                self.set_status(ok_msg.to_string());
            } else {
                self.set_status(format!("{ok_msg} ({detail})"));
            }
        } else {
            self.set_status(format!(
                "{err_prefix}: {}",
                result.stderr.lines().next().unwrap_or("unknown error")
            ));
        }
    }

    pub fn status_text(&self) -> Option<&str> {
        self.status_message.as_ref().and_then(|(msg, when)| {
            if when.elapsed() < Duration::from_secs(5) {
                Some(msg.as_str())
            } else {
                Option::None
            }
        })
    }

    /// Handle a vim-style action.
    pub fn handle_action(&mut self, action: Action) {
        match action {
            Action::Quit => self.running = false,
            Action::Refresh => {
                self.refresh();
                self.set_status("Refreshed.");
            }
            Action::NavLeft => match self.view {
                View::Panels => {
                    let prev = self.panel.prev();
                    if prev != self.panel {
                        self.panel = prev;
                        self.selected_index = 0;
                    }
                }
                View::Logs => {
                    let len = self.log_source_count();
                    if len > 0 {
                        self.log_source = if self.log_source == 0 {
                            len - 1
                        } else {
                            self.log_source - 1
                        };
                        self.refresh_logs();
                    }
                }
            },
            Action::NavRight => match self.view {
                View::Panels => {
                    let next = self.panel.next();
                    if next != self.panel {
                        self.panel = next;
                        self.selected_index = 0;
                    }
                }
                View::Logs => {
                    let len = self.log_source_count();
                    if len > 0 {
                        self.log_source = (self.log_source + 1) % len;
                        self.refresh_logs();
                    }
                }
            },
            Action::MoveDown => {
                let len = self.list_len();
                if len > 0 && self.selected_index < len - 1 {
                    self.selected_index += 1;
                }
            }
            Action::MoveUp => {
                if self.selected_index > 0 {
                    self.selected_index -= 1;
                }
            }
            Action::JumpTop => {
                self.selected_index = 0;
            }
            Action::JumpBottom => {
                let len = self.list_len();
                if len > 0 {
                    self.selected_index = len - 1;
                }
            }
            Action::HalfPageDown => {
                let len = self.list_len();
                if len > 0 {
                    self.selected_index = (self.selected_index + 10).min(len - 1);
                }
            }
            Action::HalfPageUp => {
                self.selected_index = self.selected_index.saturating_sub(10);
            }
            Action::Enter => match self.view {
                View::Panels
                    if self.panel == Panel::Components || self.panel == Panel::Branches =>
                {
                    self.log_panel = self.panel;
                    self.log_source = self.selected_index;
                    self.view = View::Logs;
                    self.log_follow = true;
                    self.refresh_logs();
                }
                _ => {}
            },
            Action::Back => match self.view {
                View::Logs => {
                    self.view = View::Panels;
                    self.panel = self.log_panel;
                    let max = match self.log_panel {
                        Panel::Components => self.state.components.len(),
                        Panel::Branches => self.state.branches.len(),
                        Panel::Tenants => self.state.tenants.len(),
                    };
                    self.selected_index = self.log_source.min(max.saturating_sub(1));
                }
                View::Panels => {
                    // At top level, Esc does nothing in Normal mode
                }
            },
            Action::ToggleHelp => {
                self.mode = if self.mode == Mode::Help {
                    Mode::Normal
                } else {
                    Mode::Help
                };
            }
            Action::EnterCommandMode => {
                self.mode = Mode::Command;
                self.command_input.clear();
            }
            Action::ExecCommand(cmd) => {
                self.mode = Mode::Normal;
                self.exec_command(&cmd);
            }
            Action::NewBranch => {
                self.mode = Mode::Input;
                self.branch_input.clear();
                self.branch_parent = self
                    .state
                    .branches
                    .get(self.selected_index)
                    .map(|b| b.name.clone())
                    .unwrap_or_else(|| self.config.compute.default_branch.clone());
            }
            Action::StartAll => {
                let config = self.config.clone();
                self.spawn_bg(
                    "Starting all components...",
                    "All components started".into(),
                    "Start failed".into(),
                    async move { command::start(&config).await },
                );
            }
            Action::StopAll => {
                self.pending_confirm = Some(PendingConfirm {
                    message: "Stop all Neon components?".to_string(),
                    action: Box::new(ConfirmAction::StopAll),
                });
                self.mode = Mode::Confirm;
            }
            Action::InitNeon => {
                let config = self.config.clone();
                self.spawn_bg(
                    "Initializing Neon...",
                    "Neon initialized".into(),
                    "Init failed".into(),
                    async move { command::init(&config).await },
                );
            }
            Action::DestroyNeon => {
                self.pending_confirm = Some(PendingConfirm {
                    message: "DESTROY all Neon data? This cannot be undone!".to_string(),
                    action: Box::new(ConfirmAction::DestroyNeon),
                });
                self.mode = Mode::Confirm;
            }
            Action::StartSelected => {
                self.start_selected();
            }
            Action::StopSelected => {
                self.stop_selected();
            }
            Action::DeleteSelected => {
                self.delete_selected();
            }
            Action::CopyUrl => {
                self.copy_selected_url();
            }
            Action::OpenPsql => {
                // Will be handled specially in the event loop (needs terminal restore)
            }
            Action::ConfirmYes => {
                if let Some(confirm) = self.pending_confirm.take() {
                    self.mode = Mode::Normal;
                    match *confirm.action {
                        ConfirmAction::DeleteBranch(name) => {
                            let config = self.config.clone();
                            self.spawn_bg(
                                &format!("Deleting branch '{name}'..."),
                                format!("Branch '{name}' deleted"),
                                "Delete failed".into(),
                                async move { command::delete_branch(&config, &name).await },
                            );
                        }
                        ConfirmAction::StopAll => {
                            let config = self.config.clone();
                            self.spawn_bg(
                                "Stopping all components...",
                                "All components stopped".into(),
                                "Stop failed".into(),
                                async move { command::stop(&config).await },
                            );
                        }
                        ConfirmAction::DestroyNeon => {
                            let config = self.config.clone();
                            self.spawn_bg(
                                "Destroying all Neon data...",
                                "Neon destroyed".into(),
                                "Destroy failed".into(),
                                async move { command::destroy(&config).await },
                            );
                        }
                        ConfirmAction::DeleteTimeline(tenant_id, timeline_id) => {
                            let config = self.config.clone();
                            let short_id = timeline_id.chars().take(12).collect::<String>();
                            self.spawn_bg(
                                &format!("Deleting timeline {short_id}..."),
                                format!("Timeline {short_id} deleted"),
                                "Delete timeline failed".into(),
                                async move {
                                    command::delete_timeline(&config, &tenant_id, &timeline_id)
                                        .await
                                },
                            );
                        }
                    }
                }
            }
            Action::ConfirmNo => {
                self.pending_confirm = None;
                self.mode = Mode::Normal;
                self.set_status("Cancelled.");
            }
            Action::ToggleLogFollow => {
                self.log_follow = !self.log_follow;
            }
            Action::None => {}
        }
    }

    fn start_selected(&mut self) {
        match self.panel {
            Panel::Branches => {
                if let Some(branch) = self.state.branches.get(self.selected_index) {
                    let name = branch.name.clone();
                    let config = self.config.clone();
                    self.spawn_bg(
                        &format!("Starting endpoint '{name}'..."),
                        format!("Endpoint '{name}' started"),
                        "Failed".into(),
                        async move { command::start_endpoint(&config, &name).await },
                    );
                }
            }
            Panel::Components | Panel::Tenants => {
                let config = self.config.clone();
                self.spawn_bg(
                    "Starting all components...",
                    "All components started".into(),
                    "Start failed".into(),
                    async move { command::start(&config).await },
                );
            }
        }
    }

    fn stop_selected(&mut self) {
        match self.panel {
            Panel::Branches => {
                if let Some(branch) = self.state.branches.get(self.selected_index) {
                    let name = branch.name.clone();
                    let config = self.config.clone();
                    self.spawn_bg(
                        &format!("Stopping endpoint '{name}'..."),
                        format!("Endpoint '{name}' stopped"),
                        "Failed".into(),
                        async move { command::stop_endpoint(&config, &name).await },
                    );
                }
            }
            Panel::Components | Panel::Tenants => {
                self.pending_confirm = Some(PendingConfirm {
                    message: "Stop all Neon components?".to_string(),
                    action: Box::new(ConfirmAction::StopAll),
                });
                self.mode = Mode::Confirm;
            }
        }
    }

    /// Returns `(tenant_id, timeline_id, is_root, branch_name)` for the currently selected
    /// timeline sub-row in the Tenants panel, or `None` if the selected row is a tenant header
    /// row or a different panel is active.
    pub fn selected_tenant_timeline(&self) -> Option<(String, String, bool, Option<String>)> {
        if self.panel != Panel::Tenants {
            return None;
        }
        let mut flat = 0usize;
        for tenant in &self.state.tenants {
            if flat == self.selected_index {
                return None; // tenant header row
            }
            flat += 1;
            for tl in &tenant.timelines {
                if flat == self.selected_index {
                    return Some((
                        tenant.id.clone(),
                        tl.id.clone(),
                        tl.is_root,
                        tl.branch_name.clone(),
                    ));
                }
                flat += 1;
            }
        }
        None
    }

    fn delete_selected(&mut self) {
        if self.panel == Panel::Branches {
            if let Some(branch) = self.state.branches.get(self.selected_index) {
                if branch.is_default {
                    self.set_status("Cannot delete the default branch.");
                    return;
                }
                let name = branch.name.clone();
                self.pending_confirm = Some(PendingConfirm {
                    message: format!("Delete branch '{name}'?"),
                    action: Box::new(ConfirmAction::DeleteBranch(name)),
                });
                self.mode = Mode::Confirm;
            }
        } else if self.panel == Panel::Tenants {
            match self.selected_tenant_timeline() {
                None => {
                    self.set_status("Select a timeline sub-row to delete.");
                }
                Some((_, _, true, _)) => {
                    self.set_status("Cannot delete the root timeline.");
                }
                Some((_, _, false, Some(branch_name))) => {
                    self.set_status(format!(
                        "Delete branch '{branch_name}' first before removing its timeline."
                    ));
                }
                Some((tenant_id, timeline_id, false, None)) => {
                    let short_id = timeline_id.chars().take(12).collect::<String>();
                    self.pending_confirm = Some(PendingConfirm {
                        message: format!(
                            "Delete dangling timeline {short_id}...? This removes pageserver data."
                        ),
                        action: Box::new(ConfirmAction::DeleteTimeline(tenant_id, timeline_id)),
                    });
                    self.mode = Mode::Confirm;
                }
            }
        }
    }

    fn copy_selected_url(&mut self) {
        if self.panel == Panel::Branches {
            if let Some(branch) = self.state.branches.get(self.selected_index) {
                let url = command::connection_url(&self.config, &branch.name);
                let copied = std::process::Command::new("wl-copy")
                    .arg(&url)
                    .status()
                    .ok()
                    .is_some_and(|s| s.success())
                    || std::process::Command::new("xclip")
                        .args(["-selection", "clipboard"])
                        .stdin(std::process::Stdio::piped())
                        .spawn()
                        .ok()
                        .and_then(|mut child| {
                            use std::io::Write;
                            child
                                .stdin
                                .as_mut()
                                .and_then(|stdin| stdin.write_all(url.as_bytes()).ok());
                            child.wait().ok()
                        })
                        .is_some_and(|s| s.success());

                if copied {
                    self.set_status(format!("Copied: {url}"));
                } else {
                    self.set_status(format!("URL: {url}  (clipboard unavailable)"));
                }
            }
        }
    }

    fn exec_command(&mut self, cmd: &str) {
        let parts: Vec<&str> = cmd.split_whitespace().collect();
        match parts.first().copied() {
            Some("q" | "quit") => self.running = false,
            Some("init") => {
                let config = self.config.clone();
                self.spawn_bg(
                    "Initializing Neon...",
                    "Neon initialized".into(),
                    "Init failed".into(),
                    async move { command::init(&config).await },
                );
            }
            Some("start") => {
                let config = self.config.clone();
                self.spawn_bg(
                    "Starting all components...",
                    "All components started".into(),
                    "Start failed".into(),
                    async move { command::start(&config).await },
                );
            }
            Some("stop") => {
                self.pending_confirm = Some(PendingConfirm {
                    message: "Stop all Neon components?".to_string(),
                    action: Box::new(ConfirmAction::StopAll),
                });
                self.mode = Mode::Confirm;
            }
            Some("destroy") => {
                self.pending_confirm = Some(PendingConfirm {
                    message: "DESTROY all Neon data? This cannot be undone!".to_string(),
                    action: Box::new(ConfirmAction::DestroyNeon),
                });
                self.mode = Mode::Confirm;
            }
            Some("branch") => {
                if let Some(name) = parts.get(1) {
                    let name = name.to_string();
                    let default = self.config.compute.default_branch.clone();
                    let parent = if parts.get(2).copied() == Some("--from") {
                        parts
                            .get(3)
                            .map(|s| s.to_string())
                            .unwrap_or_else(|| default.clone())
                    } else {
                        default
                    };
                    let config = self.config.clone();
                    let ok_msg = format!("Branch '{name}' created");
                    self.spawn_bg(
                        &format!("Creating branch '{name}'..."),
                        ok_msg,
                        "Failed".into(),
                        async move { command::create_branch(&config, &name, &parent).await },
                    );
                } else {
                    self.set_status("Usage: :branch <name> [--from <parent>]");
                }
            }
            Some("delete") => {
                if let Some(name) = parts.get(1) {
                    self.pending_confirm = Some(PendingConfirm {
                        message: format!("Delete branch '{name}'?"),
                        action: Box::new(ConfirmAction::DeleteBranch(name.to_string())),
                    });
                    self.mode = Mode::Confirm;
                } else {
                    self.set_status("Usage: :delete <branch-name>");
                }
            }
            Some("switch") => {
                if let Some(name) = parts.get(1) {
                    let name = name.to_string();
                    let config = self.config.clone();
                    let ok_msg = format!("Endpoint '{name}' started");
                    self.spawn_bg(
                        &format!("Starting endpoint '{name}'..."),
                        ok_msg,
                        "Failed".into(),
                        async move { command::start_endpoint(&config, &name).await },
                    );
                } else {
                    self.set_status("Usage: :switch <branch-name>");
                }
            }
            Some("url") => {
                let branch = parts
                    .get(1)
                    .copied()
                    .unwrap_or(&self.config.compute.default_branch);
                let url = command::connection_url(&self.config, branch);
                self.set_status(url);
            }
            _ => {
                self.set_status(format!("Unknown command: {cmd}"));
            }
        }
    }

    pub fn selected_branch_psql_url(&self) -> Option<String> {
        if self.panel != Panel::Branches {
            return None;
        }
        self.state
            .branches
            .get(self.selected_index)
            .map(|b| command::connection_url(&self.config, &b.name))
    }

    pub fn refresh_logs(&mut self) {
        // Each log source provides either a local file path or a docker container name.
        let (log_file, docker_container) = match self.log_panel {
            Panel::Components => match self.log_source {
                idx if idx < self.state.components.len() => {
                    let c = &self.state.components[idx];
                    (c.log_file.clone(), c.docker_container.clone())
                }
                _ => (None, None),
            },
            Panel::Branches => match self.log_source {
                idx if idx < self.state.branches.len() => {
                    let b = &self.state.branches[idx];
                    (b.log_file.clone(), b.docker_container.clone())
                }
                _ => (None, None),
            },
            Panel::Tenants => (None, None),
        };

        self.log_lines = if let Some(container) = docker_container {
            docker::container_logs(&container, 500)
        } else if let Some(path) = log_file {
            read_log_tail(&path, 500)
        } else {
            vec!["No log file available.".to_string()]
        };

        if self.log_follow && !self.log_lines.is_empty() {
            self.log_scroll = self.log_lines.len().saturating_sub(1);
        }
    }

    /// Number of items in the current log panel (for h/l navigation).
    pub fn log_source_count(&self) -> usize {
        match self.log_panel {
            Panel::Components => self.state.components.len(),
            Panel::Branches => self.state.branches.len(),
            Panel::Tenants => 0,
        }
    }

    /// Name of the currently viewed log source.
    pub fn log_source_name(&self) -> &str {
        match self.log_panel {
            Panel::Components => self
                .state
                .components
                .get(self.log_source)
                .map(|c| c.name.as_str())
                .unwrap_or("unknown"),
            Panel::Branches => self
                .state
                .branches
                .get(self.log_source)
                .map(|b| b.name.as_str())
                .unwrap_or("unknown"),
            Panel::Tenants => "unknown",
        }
    }

    /// Whether we're currently in the log detail view.
    pub fn in_logs(&self) -> bool {
        self.view == View::Logs
    }

    /// Whether a background command is currently running.
    pub fn is_busy(&self) -> bool {
        self.bg_result_rx.is_some()
    }

    /// Poll the background task; if finished, update status and refresh.
    pub fn poll_bg_task(&mut self) {
        let Some(rx) = &mut self.bg_result_rx else {
            return;
        };
        match rx.try_recv() {
            Ok((ok_msg, err_prefix, result)) => {
                self.bg_result_rx = None;
                self.set_command_status(&result, &ok_msg, &err_prefix);
                self.refresh();
            }
            Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {
                // Still running — do nothing.
            }
            Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                self.bg_result_rx = None;
                self.set_status("Background command failed.");
                self.refresh();
            }
        }
    }

    /// Spawn a long-running command in the background so the UI stays responsive.
    fn spawn_bg<F>(&mut self, status: &str, ok_msg: String, err_prefix: String, fut: F)
    where
        F: std::future::Future<Output = command::CommandResult> + Send + 'static,
    {
        if self.is_busy() {
            self.set_status("A command is already running.");
            return;
        }
        self.set_status(status);
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.bg_result_rx = Some(rx);
        tokio::spawn(async move {
            let result = fut.await;
            let _ = tx.send((ok_msg, err_prefix, result));
        });
    }
}

fn read_log_tail(path: &std::path::Path, max_lines: usize) -> Vec<String> {
    let Ok(contents) = std::fs::read_to_string(path) else {
        return vec![format!("Cannot read: {}", path.display())];
    };
    let lines: Vec<String> = contents.lines().map(String::from).collect();
    if lines.len() > max_lines {
        lines[lines.len() - max_lines..].to_vec()
    } else {
        lines
    }
}
