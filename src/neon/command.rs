use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use tokio::process::Command;

use crate::config::Config;
use crate::neon::{docker, process};

/// Result of running a neon_local command.
#[derive(Debug, Clone)]
pub struct CommandResult {
    pub success: bool,
    pub stdout: String,
    pub stderr: String,
}

impl CommandResult {
    fn ok(msg: impl Into<String>) -> Self {
        Self {
            success: true,
            stdout: msg.into(),
            stderr: String::new(),
        }
    }

    fn err(msg: impl Into<String>) -> Self {
        Self {
            success: false,
            stdout: String::new(),
            stderr: msg.into(),
        }
    }
}

// ── Low-level helpers ────────────────────────────────────────────────────────

/// Run `neon_local` with the given arguments.
async fn neon_local(config: &Config, args: &[&str]) -> CommandResult {
    let bin = config.neon_local_bin();
    run_command(&bin, args, &config.neon.repo_dir).await
}

async fn run_command(bin: &Path, args: &[&str], working_dir: &Path) -> CommandResult {
    let cwd = if working_dir.exists() {
        working_dir.to_path_buf()
    } else {
        std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
    };

    let result = Command::new(bin)
        .args(args)
        .current_dir(cwd)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await;

    match result {
        Ok(output) => CommandResult {
            success: output.status.success(),
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
        },
        Err(e) => CommandResult::err(format!("Failed to run {}: {e}", bin.display())),
    }
}

/// Simple HTTP GET against localhost, returns response body.
fn http_get(port: u16, path: &str) -> Option<String> {
    let addr = format!("127.0.0.1:{port}");
    let mut stream =
        TcpStream::connect_timeout(&addr.parse().ok()?, Duration::from_secs(2)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
    let request =
        format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).ok()?;
    let mut response = String::new();
    stream.read_to_string(&mut response).ok()?;
    // Split headers from body
    let body = response.split("\r\n\r\n").nth(1)?;
    Some(body.to_string())
}

/// Wait for a port to start listening (up to `timeout`).
async fn wait_for_port(port: u16, timeout: Duration) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if process::is_port_listening(port) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    false
}

/// Allocate the next free block of 3 ports for a branch endpoint.
/// Scans endpoint.json files and listening ports to find the max in use.
fn next_branch_ports(config: &Config) -> (u16, u16, u16) {
    let mut max_port = config.compute.port;

    // Scan endpoint.json files
    let endpoints_dir = config.neon.repo_dir.join("endpoints");
    if let Ok(entries) = std::fs::read_dir(&endpoints_dir) {
        for entry in entries.flatten() {
            let json_path = entry.path().join("endpoint.json");
            if let Ok(contents) = std::fs::read_to_string(&json_path) {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&contents) {
                    for key in &["pg_port", "external_http_port", "internal_http_port"] {
                        if let Some(p) = v.get(*key).and_then(|v| v.as_u64()) {
                            let p = p as u16;
                            if p > max_port {
                                max_port = p;
                            }
                        }
                    }
                }
            }
        }
    }

    // Scan listening ports in the compute range
    let port_min = config.compute.port;
    let port_max = port_min + 999;
    if let Ok(contents) = std::fs::read_to_string("/proc/net/tcp") {
        for line in contents.lines().skip(1) {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 4 && parts[3] == "0A" {
                if let Some(hex) = parts[1].split(':').nth(1) {
                    if let Ok(p) = u16::from_str_radix(hex, 16) {
                        if p >= port_min && p <= port_max && p > max_port {
                            max_port = p;
                        }
                    }
                }
            }
        }
    }

    let ext = max_port + 1;
    let int = max_port + 2;
    let pg = max_port + 3;
    (pg, ext, int)
}

/// Kill a process by PID.
fn kill_pid(pid: u32) {
    unsafe {
        libc::kill(pid as i32, libc::SIGKILL);
    }
}

/// Force-kill any processes listening on the given ports.
fn force_kill_ports(ports: &[u16]) {
    for &port in ports {
        if process::is_port_listening(port) {
            // Find PID from /proc/net/tcp
            if let Some(pid) = find_pid_for_port(port) {
                kill_pid(pid);
            }
        }
    }
}

/// Find the PID of a process listening on a given port.
fn find_pid_for_port(port: u16) -> Option<u32> {
    let hex_port = format!("{port:04X}");

    // Read /proc/net/tcp to find the inode
    let tcp = std::fs::read_to_string("/proc/net/tcp").ok()?;
    let mut inode = None;
    for line in tcp.lines().skip(1) {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 10
            && parts[3] == "0A"
            && parts[1].split(':').nth(1).is_some_and(|p| p == hex_port)
        {
            inode = Some(parts[9].to_string());
            break;
        }
    }
    let inode = inode?;

    // Scan /proc/*/fd/ to find the process with this socket inode
    let proc_dir = std::fs::read_dir("/proc").ok()?;
    for entry in proc_dir.flatten() {
        let name = entry.file_name();
        let pid_str = name.to_string_lossy().to_string();
        let Ok(pid) = pid_str.parse::<u32>() else {
            continue;
        };
        let fd_dir = format!("/proc/{pid}/fd");
        let Ok(fds) = std::fs::read_dir(&fd_dir) else {
            continue;
        };
        for fd in fds.flatten() {
            if let Ok(link) = std::fs::read_link(fd.path()) {
                let link_str = link.to_string_lossy();
                if link_str.contains(&format!("socket:[{inode}]")) {
                    return Some(pid);
                }
            }
        }
    }
    None
}

// ── Init helpers: pg_distrib mirror + storage controller DB ─────────────────

/// Read `pg_distrib_dir` from the neon config file.
fn parse_pg_distrib_dir(config_path: &Path) -> Option<PathBuf> {
    let contents = std::fs::read_to_string(config_path).ok()?;
    for line in contents.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("pg_distrib_dir") {
            // pg_distrib_dir = "/nix/store/.../share"
            let value = trimmed.split('=').nth(1)?.trim().trim_matches('"');
            return Some(PathBuf::from(value));
        }
    }
    None
}

/// Create a local symlink mirror of `pg_distrib_dir` where `v16` points to `v<pg_version>`.
/// This makes neon_local's hardcoded pg16 storage controller DB use the desired version.
fn create_pg_distrib_mirror(
    original: &Path,
    mirror: &Path,
    pg_version: u16,
) -> std::io::Result<()> {
    std::fs::create_dir_all(mirror)?;
    let target_version = format!("v{pg_version}");

    for entry in std::fs::read_dir(original)? {
        let entry = entry?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        let link_path = mirror.join(&name);

        // v16 → v<pg_version>; all others → their real paths
        let target = if name_str == "v16" && pg_version != 16 {
            original.join(&target_version)
        } else {
            entry.path()
        };

        if !link_path.exists() {
            std::os::unix::fs::symlink(&target, &link_path)?;
        }
    }
    Ok(())
}

/// Pre-initialize the storage controller database during `init` so that
/// `neon_local start` finds the database AND full schema already in place.
/// This fixes a race condition in neon where the storage controller queries
/// the `controllers` table before its own diesel migrations complete.
async fn pre_init_storage_controller_db(config: &Config) -> Result<(), String> {
    let repo = &config.neon.repo_dir;
    let db_dir = repo.join("storage_controller_db");
    let port = config.ports.storage_controller_db.to_string();

    // Determine the pg binary directory (use the real v<pg_version> path)
    let pg_bin = if let Some(original) = parse_pg_distrib_dir(&repo.join("config")) {
        // The config might already point to the mirror; resolve symlinks
        let version_dir = original.join(format!("v{}", config.compute.pg_version));
        let resolved = std::fs::canonicalize(&version_dir).unwrap_or(version_dir);
        resolved.join("bin")
    } else {
        return Err("Cannot determine pg_distrib_dir from config".to_string());
    };

    // 1. initdb
    let initdb = pg_bin.join("initdb");
    let result = Command::new(&initdb)
        .args(["-D", &db_dir.display().to_string(), "--no-instructions"])
        .current_dir(repo)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .map_err(|e| format!("initdb failed to start: {e}"))?;
    if !result.status.success() {
        return Err(format!(
            "initdb failed: {}",
            String::from_utf8_lossy(&result.stderr)
        ));
    }

    // 2. Set the port in postgresql.conf
    let pg_conf = db_dir.join("postgresql.conf");
    if let Ok(mut conf) = std::fs::read_to_string(&pg_conf) {
        conf.push_str(&format!("\nport = {port}\n"));
        let _ = std::fs::write(&pg_conf, conf);
    }

    // 3. Start the database
    let pg_ctl = pg_bin.join("pg_ctl");
    let log_file = db_dir.join("postgres.log");
    let result = Command::new(&pg_ctl)
        .args([
            "-D",
            &db_dir.display().to_string(),
            "-w",
            "-l",
            &log_file.display().to_string(),
            "start",
        ])
        .current_dir(repo)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .map_err(|e| format!("pg_ctl start failed: {e}"))?;
    if !result.status.success() {
        return Err(format!(
            "pg_ctl start failed: {}",
            String::from_utf8_lossy(&result.stderr)
        ));
    }

    // 4. Wait for the database to accept connections
    if !wait_for_port(config.ports.storage_controller_db, Duration::from_secs(10)).await {
        let _ = stop_pg_ctl(&pg_ctl, &db_dir).await;
        return Err("Storage controller DB did not start in time".to_string());
    }

    // 5. Create the storage_controller database
    let createdb = pg_bin.join("createdb");
    let result = Command::new(&createdb)
        .args(["-h", "localhost", "-p", &port, "storage_controller"])
        .current_dir(repo)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .map_err(|e| format!("createdb failed: {e}"))?;
    if !result.status.success() {
        let stderr = String::from_utf8_lossy(&result.stderr);
        if !stderr.contains("already exists") {
            let _ = stop_pg_ctl(&pg_ctl, &db_dir).await;
            return Err(format!("createdb failed: {stderr}"));
        }
    }

    // 6. Stop the database (neon_local start will restart it and run migrations)
    stop_pg_ctl(&pg_ctl, &db_dir).await?;

    Ok(())
}

async fn stop_pg_ctl(pg_ctl: &Path, db_dir: &Path) -> Result<(), String> {
    let result = Command::new(pg_ctl)
        .args(["-D", &db_dir.display().to_string(), "-w", "stop"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .map_err(|e| format!("pg_ctl stop failed: {e}"))?;
    if !result.status.success() {
        return Err(format!(
            "pg_ctl stop failed: {}",
            String::from_utf8_lossy(&result.stderr)
        ));
    }
    Ok(())
}

// ── Docker Compose helpers ────────────────────────────────────────────────────

/// Run `docker compose --project-name <project> <args>`.
async fn docker_compose(project: &str, args: &[&str]) -> CommandResult {
    let mut full_args = vec!["compose", "--project-name", project];
    full_args.extend_from_slice(args);
    let output = tokio::process::Command::new("docker")
        .args(&full_args)
        .output()
        .await;
    match output {
        Ok(out) => CommandResult {
            success: out.status.success(),
            stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
        },
        Err(e) => CommandResult::err(format!("docker compose failed: {e}")),
    }
}

fn docker_unsupported(op: &str) -> CommandResult {
    CommandResult::err(format!(
        "{op} is not supported in Docker mode. Manage containers with docker compose directly."
    ))
}

/// Stop a single Docker container by name (`docker stop`).
async fn run_docker_stop(container: &str) -> CommandResult {
    let out = tokio::process::Command::new("docker")
        .args(["stop", container])
        .output()
        .await;
    match out {
        Ok(o) if o.status.success() => CommandResult::ok(format!("Stopped {container}.")),
        Ok(o) => CommandResult::err(format!(
            "Failed to stop {container}: {}",
            String::from_utf8_lossy(&o.stderr).trim()
        )),
        Err(e) => CommandResult::err(format!("docker stop failed: {e}")),
    }
}

/// Stop all branch containers tracked in the Docker state file.
///
/// Branch containers are started via `docker run` outside of Compose, so
/// `docker compose stop/down` does not touch them.  This function ensures
/// they are cleaned up before the Compose stack is stopped.
async fn stop_branch_containers() {
    let state = docker::read_docker_branch_state();
    for (_name, entry) in &state.branches {
        if let Some(container) = &entry.container {
            let _ = tokio::process::Command::new("docker")
                .args(["stop", container])
                .output()
                .await;
        }
    }
}

// ── Public API: Commands ─────────────────────────────────────────────────────

/// Initialize Neon repository.
pub async fn init(config: &Config) -> CommandResult {
    if config.docker.mode {
        return docker_unsupported("init");
    }
    let repo = &config.neon.repo_dir;
    if repo.is_dir() {
        return CommandResult::err(format!(
            "Neon directory already exists at {}. Use 'destroy' first for a full reset.",
            repo.display()
        ));
    }

    let result = neon_local(config, &["init"]).await;
    if !result.success {
        return result;
    }

    let config_path = repo.join("config");

    // Disable safekeeper-based timeline creation (incompatible with local dev)
    if let Ok(contents) = std::fs::read_to_string(&config_path) {
        let patched = contents.replace(
            "timelines_onto_safekeepers = true",
            "timelines_onto_safekeepers = false",
        );
        let _ = std::fs::write(&config_path, patched);
    }

    // Create a local pg_distrib mirror so the storage controller DB uses the
    // configured pg_version (neon_local hardcodes pg16 for the storage controller DB;
    // we symlink v16 → v<configured> to override).
    if let Some(original_distrib) = parse_pg_distrib_dir(&config_path) {
        let pg_ver = config.compute.pg_version;
        let mirror = repo.join("pg_distrib");
        if let Err(e) = create_pg_distrib_mirror(&original_distrib, &mirror, pg_ver) {
            return CommandResult::err(format!("Failed to create pg_distrib mirror: {e}"));
        }
        // Rewrite pg_distrib_dir in the config to point to our mirror
        if let Ok(contents) = std::fs::read_to_string(&config_path) {
            let abs_mirror = std::fs::canonicalize(&mirror)
                .unwrap_or_else(|_| mirror.clone())
                .display()
                .to_string();
            let patched = contents.replace(&original_distrib.display().to_string(), &abs_mirror);
            let _ = std::fs::write(&config_path, patched);
        }
    }

    // Pre-initialize the storage controller DB so that `neon_local start` finds
    // it ready, avoiding FATAL "database does not exist" log messages.
    if let Err(e) = pre_init_storage_controller_db(config).await {
        return CommandResult::err(format!(
            "Failed to pre-initialize storage controller DB: {e}"
        ));
    }

    CommandResult::ok("Neon initialized successfully.")
}

/// Start all Neon services + default endpoint.
pub async fn start(config: &Config) -> CommandResult {
    if config.docker.mode {
        return docker_compose(&config.docker.compose_project, &["up", "-d"]).await;
    }
    let repo = &config.neon.repo_dir;
    if !repo.is_dir() || !repo.join("config").exists() {
        return CommandResult::err("Neon not initialized. Run 'init' first.");
    }

    // Start core services if not already running
    if !process::is_port_listening(config.ports.storage_broker) {
        let result = neon_local(config, &["start"]).await;
        if !result.success {
            return result;
        }
        // Wait for pageserver to be ready
        if !wait_for_port(config.ports.pageserver_http, Duration::from_secs(15)).await {
            return CommandResult::err("Pageserver did not start in time.");
        }
    }

    // Ensure default tenant exists
    let tenant_body = http_get(config.ports.pageserver_http, "/v1/tenant").unwrap_or_default();
    if tenant_body == "[]" || tenant_body.is_empty() {
        let tenant_id = generate_hex_id();
        let result = neon_local(
            config,
            &[
                "tenant",
                "create",
                "--tenant-id",
                &tenant_id,
                "--set-default",
            ],
        )
        .await;
        if !result.success {
            return CommandResult::err(format!("Failed to create tenant: {}", result.stderr));
        }
    }

    // Ensure default timeline exists
    let default_branch = &config.compute.default_branch;
    let timeline_result = neon_local(config, &["timeline", "list"]).await;
    if !timeline_result.stdout.contains(default_branch) {
        let result = neon_local(
            config,
            &["timeline", "create", "--branch-name", default_branch],
        )
        .await;
        if !result.success {
            return CommandResult::err(format!("Failed to create timeline: {}", result.stderr));
        }
    }

    // Start or create the default endpoint
    let pg_version = config.compute.pg_version.to_string();
    let port_str = config.compute.port.to_string();
    let endpoint_result = neon_local(config, &["endpoint", "list"]).await;
    if endpoint_result.stdout.contains(default_branch) {
        let result = neon_local(
            config,
            &[
                "endpoint",
                "start",
                default_branch,
                "--create-test-user",
                "--dev",
            ],
        )
        .await;
        if !result.success {
            // Ignore errors if already running
            if !process::is_port_listening(config.compute.port) {
                return CommandResult::err(format!("Failed to start endpoint: {}", result.stderr));
            }
        }
    } else {
        let result = neon_local(
            config,
            &[
                "endpoint",
                "create",
                default_branch,
                "--branch-name",
                default_branch,
                "--pg-port",
                &port_str,
                "--pg-version",
                &pg_version,
                "--update-catalog",
            ],
        )
        .await;
        if !result.success {
            return CommandResult::err(format!("Failed to create endpoint: {}", result.stderr));
        }
        let result = neon_local(
            config,
            &[
                "endpoint",
                "start",
                default_branch,
                "--create-test-user",
                "--dev",
            ],
        )
        .await;
        if !result.success {
            return CommandResult::err(format!("Failed to start endpoint: {}", result.stderr));
        }
    }

    let url = connection_url(config, default_branch);
    CommandResult::ok(format!("Neon is running.\nConnection URL: {url}"))
}

/// Stop all Neon services.
pub async fn stop(config: &Config) -> CommandResult {
    if config.docker.mode {
        // Stop branch containers first (they're outside of Compose).
        stop_branch_containers().await;
        return docker_compose(&config.docker.compose_project, &["stop"]).await;
    }
    let repo = &config.neon.repo_dir;
    if !repo.is_dir() {
        return CommandResult::err("Neon not initialized.");
    }

    // Stop all endpoints first
    let endpoint_result = neon_local(config, &["endpoint", "list"]).await;
    for line in endpoint_result.stdout.lines().skip(1) {
        let endpoint_id = line.split_whitespace().next().unwrap_or("");
        if !endpoint_id.is_empty() && endpoint_id != "ENDPOINT" {
            let _ = neon_local(config, &["endpoint", "stop", endpoint_id]).await;
        }
    }

    // Stop core services
    let _ = neon_local(config, &["stop"]).await;

    // Force-kill any remaining processes on neon ports
    let neon_ports = [
        config.ports.storage_broker,
        config.ports.pageserver_http,
        64000, // pageserver pg port
        config.ports.safekeeper_pg,
        7676, // safekeeper http
        config.ports.storage_controller,
        config.ports.storage_controller_db,
        config.ports.endpoint_storage,
    ];

    // Brief pause for graceful shutdown
    tokio::time::sleep(Duration::from_millis(500)).await;

    let any_remaining = neon_ports.iter().any(|&p| process::is_port_listening(p));
    if any_remaining {
        force_kill_ports(&neon_ports);
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    CommandResult::ok("Neon stopped.")
}

/// Show status (timelines and endpoints).
pub async fn status(config: &Config) -> CommandResult {
    let repo = &config.neon.repo_dir;
    if !repo.is_dir() {
        return CommandResult::err("Neon not initialized.");
    }

    let timelines = neon_local(config, &["timeline", "list"]).await;
    let endpoints = neon_local(config, &["endpoint", "list"]).await;

    let mut output = String::new();
    output.push_str("Timelines (branches):\n");
    output.push_str(&timelines.stdout);
    output.push_str("\nEndpoints (compute):\n");
    output.push_str(&endpoints.stdout);

    CommandResult::ok(output)
}

/// Create a new database branch.
pub async fn create_branch(config: &Config, name: &str, parent: &str) -> CommandResult {
    if config.docker.mode {
        return create_docker_branch(config, name, parent).await;
    }
    let repo = &config.neon.repo_dir;
    if !repo.is_dir() {
        return CommandResult::err("Neon not initialized.");
    }
    if name == config.compute.default_branch {
        return CommandResult::err(format!("Cannot branch from '{name}' to itself."));
    }

    // Check if timeline already exists in .neon/config branch_name_mappings
    let config_path = repo.join("config");
    let timeline_exists = std::fs::read_to_string(&config_path)
        .map(|c| c.contains(&format!("{name} = ")))
        .unwrap_or(false);

    if !timeline_exists {
        // Create the timeline (this also auto-starts a compute we don't want)
        let result = neon_local(
            config,
            &[
                "timeline",
                "branch",
                "--branch-name",
                name,
                "--ancestor-branch-name",
                parent,
            ],
        )
        .await;
        if !result.success {
            return CommandResult::err(format!("Failed to create timeline: {}", result.stderr));
        }

        // Kill the auto-started compute_ctl (it has wrong ports)
        if let Some(pid) = process::find_process_by_arg(name, "compute_ctl") {
            kill_pid(pid);
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    }

    // Remove any auto-created endpoint directory
    let endpoint_dir = repo.join("endpoints").join(name);
    let _ = std::fs::remove_dir_all(&endpoint_dir);

    // Allocate safe ports
    let (pg_port, ext_port, int_port) = next_branch_ports(config);
    let pg_version = config.compute.pg_version.to_string();
    let pg_port_str = pg_port.to_string();
    let ext_port_str = ext_port.to_string();
    let int_port_str = int_port.to_string();

    let result = neon_local(
        config,
        &[
            "endpoint",
            "create",
            name,
            "--branch-name",
            name,
            "--pg-port",
            &pg_port_str,
            "--external-http-port",
            &ext_port_str,
            "--internal-http-port",
            &int_port_str,
            "--pg-version",
            &pg_version,
            "--update-catalog",
        ],
    )
    .await;
    if !result.success {
        return CommandResult::err(format!("Failed to create endpoint: {}", result.stderr));
    }

    let result = neon_local(
        config,
        &["endpoint", "start", name, "--create-test-user", "--dev"],
    )
    .await;
    if !result.success {
        return CommandResult::err(format!("Failed to start endpoint: {}", result.stderr));
    }

    let url = connection_url(config, name);
    CommandResult::ok(format!("Branch '{name}' is ready.\nConnection URL: {url}"))
}

/// Delete a branch endpoint.
pub async fn delete_branch(config: &Config, name: &str) -> CommandResult {
    if config.docker.mode {
        return delete_docker_branch(config, name).await;
    }
    if name == config.compute.default_branch {
        return CommandResult::err(format!("Cannot delete the default branch '{name}'."));
    }

    // Stop and destroy the endpoint via neon_local
    let result = neon_local(config, &["endpoint", "stop", name, "--destroy"]).await;
    if !result.success {
        // If the endpoint wasn't running, neon_local may error — that's OK,
        // we still want to clean up the directory below.
        if !result.stderr.contains("not found") && !result.stderr.contains("does not exist") {
            // Log but don't bail — we still want to clean up
        }
    }

    // Kill any remaining compute processes for this branch
    if let Some(pid) = process::find_process_by_arg(name, "compute_ctl") {
        kill_pid(pid);
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    // Force-kill processes on the branch's ports
    let endpoint_json = config
        .neon
        .repo_dir
        .join("endpoints")
        .join(name)
        .join("endpoint.json");
    if let Ok(contents) = std::fs::read_to_string(&endpoint_json) {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&contents) {
            let mut ports_to_kill = Vec::new();
            for key in &["pg_port", "external_http_port", "internal_http_port"] {
                if let Some(p) = v.get(*key).and_then(|val| val.as_u64()) {
                    ports_to_kill.push(p as u16);
                }
            }
            if !ports_to_kill.is_empty() {
                force_kill_ports(&ports_to_kill);
            }
        }
    }

    // Remove the endpoint directory so the branch disappears from the UI
    let endpoint_dir = config.neon.repo_dir.join("endpoints").join(name);
    if endpoint_dir.exists() {
        if let Err(e) = std::fs::remove_dir_all(&endpoint_dir) {
            return CommandResult::err(format!(
                "Endpoint stopped but failed to remove directory: {e}"
            ));
        }
    }

    CommandResult::ok(format!(
        "Branch '{name}' deleted. Timeline data is preserved in the pageserver."
    ))
}

/// Start (or create+start) a branch endpoint.
pub async fn start_endpoint(config: &Config, name: &str) -> CommandResult {
    if config.docker.mode {
        return docker_compose(
            &config.docker.compose_project,
            &["start", name],
        )
        .await;
    }
    let endpoint_result = neon_local(config, &["endpoint", "list"]).await;

    if endpoint_result.stdout.contains(name) {
        let result = neon_local(
            config,
            &["endpoint", "start", name, "--create-test-user", "--dev"],
        )
        .await;
        if !result.success {
            // Check if already running
            let port = config.branch_port(name);
            if !process::is_port_listening(port) {
                return CommandResult::err(format!("Failed to start endpoint: {}", result.stderr));
            }
        }
    } else {
        // Create the endpoint with safe ports
        let (pg_port, ext_port, int_port) = next_branch_ports(config);
        let pg_version = config.compute.pg_version.to_string();
        let pg_port_str = pg_port.to_string();
        let ext_port_str = ext_port.to_string();
        let int_port_str = int_port.to_string();

        let result = neon_local(
            config,
            &[
                "endpoint",
                "create",
                name,
                "--branch-name",
                name,
                "--pg-port",
                &pg_port_str,
                "--external-http-port",
                &ext_port_str,
                "--internal-http-port",
                &int_port_str,
                "--pg-version",
                &pg_version,
                "--update-catalog",
            ],
        )
        .await;
        if !result.success {
            return CommandResult::err(format!("Failed to create endpoint: {}", result.stderr));
        }

        let result = neon_local(
            config,
            &["endpoint", "start", name, "--create-test-user", "--dev"],
        )
        .await;
        if !result.success {
            return CommandResult::err(format!("Failed to start endpoint: {}", result.stderr));
        }
    }

    let url = connection_url(config, name);
    CommandResult::ok(format!("Endpoint '{name}' started.\nConnection URL: {url}"))
}

/// Stop a branch endpoint.
pub async fn stop_endpoint(config: &Config, name: &str) -> CommandResult {
    if config.docker.mode {
        if name == config.compute.default_branch {
            // Default branch is a Compose service.
            return docker_compose(&config.docker.compose_project, &["stop", "compute"]).await;
        }
        // Other branches are standalone containers tracked in the state file.
        let state = docker::read_docker_branch_state();
        if let Some(entry) = state.branches.get(name) {
            if let Some(container) = &entry.container {
                return run_docker_stop(container).await;
            }
        }
        return CommandResult::err(format!("Branch '{name}' not found in Docker state."));
    }
    neon_local(config, &["endpoint", "stop", name]).await
}

/// Destroy all Neon data.
pub async fn destroy(config: &Config) -> CommandResult {
    if config.docker.mode {
        // Stop and remove all branch containers (started outside Compose with --rm).
        stop_branch_containers().await;
        // Tear down the Compose stack and remove volumes.
        let result = docker_compose(
            &config.docker.compose_project,
            &["down", "--volumes", "--remove-orphans"],
        )
        .await;
        // Clear the local branch state file.
        let _ = std::fs::remove_file(docker::STATE_FILE);
        return result;
    }
    let repo = &config.neon.repo_dir;
    if !repo.is_dir() {
        return CommandResult::ok("Nothing to destroy.");
    }

    // Try graceful stop first
    let _ = stop(config).await;

    // Remove the data directory
    if let Err(e) = std::fs::remove_dir_all(repo) {
        return CommandResult::err(format!("Failed to remove {}: {e}", repo.display()));
    }

    CommandResult::ok("All Neon data destroyed. Run 'init' to start fresh.")
}

/// Get the connection URL for a branch.
pub fn connection_url(config: &Config, branch: &str) -> String {
    let port = if config.docker.mode {
        // In Docker mode, non-default branches store their port in the state file.
        docker::docker_branch_port(branch).unwrap_or(config.compute.port)
    } else {
        config.branch_port(branch)
    };
    match &config.compute.password {
        Some(pw) => format!(
            "postgresql://{}:{}@{}:{}/{}",
            config.compute.user, pw, config.compute.host, port, config.compute.database,
        ),
        None => format!(
            "postgresql://{}@{}:{}/{}",
            config.compute.user, config.compute.host, port, config.compute.database,
        ),
    }
}

/// Parse the output of `neon_local timeline list` to extract parent-child relationships.
/// Returns a map of branch_name → parent_branch_name.
pub fn parse_timeline_hierarchy(config: &Config) -> std::collections::HashMap<String, String> {
    let bin = config.neon_local_bin();
    let cwd = if config.neon.repo_dir.exists() {
        config.neon.repo_dir.clone()
    } else {
        return std::collections::HashMap::new();
    };

    let Ok(output) = std::process::Command::new(&bin)
        .args(["timeline", "list"])
        .current_dir(cwd)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
    else {
        return std::collections::HashMap::new();
    };

    if !output.status.success() {
        return std::collections::HashMap::new();
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    parse_timeline_output(&stdout)
}

/// Parse `neon_local timeline list` output into a map of child → parent branch names.
fn parse_timeline_output(output: &str) -> std::collections::HashMap<String, String> {
    let mut parents = std::collections::HashMap::new();
    // Output format (note: first-level children have ┗━ at column 0, NOT indented):
    //   main [timeline_id]
    //   ┗━ @0/1234: develop [timeline_id]
    //      ┗━ @0/5678: feature [timeline_id]
    //
    // We track a stack of (depth, branch_name) to determine parent.
    // Depth is computed from the position of the ┗ tree connector, not whitespace.
    let mut stack: Vec<(usize, String)> = Vec::new();

    for line in output.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let branch_name = if let Some(name) = extract_branch_name(trimmed) {
            name
        } else {
            continue;
        };

        // Compute depth from the byte position of '┗'.
        // Root lines (no connector) are depth 0.
        // Child lines: ┗ at byte 0 → depth 1, at byte 3 → depth 2, etc.
        let depth = if let Some(pos) = line.find('┗') {
            1 + pos / 3
        } else {
            0
        };

        // Pop stack entries at same or deeper depth
        while let Some((level, _)) = stack.last() {
            if *level >= depth {
                stack.pop();
            } else {
                break;
            }
        }

        // If there's a parent on the stack, record the relationship
        if let Some((_, parent_name)) = stack.last() {
            parents.insert(branch_name.clone(), parent_name.clone());
        }

        stack.push((depth, branch_name));
    }

    parents
}

/// Extract branch name from a timeline list line.
/// "main [abc123]" → "main"
/// "┗━ @0/1234: develop [abc123]" → "develop"
fn extract_branch_name(line: &str) -> Option<String> {
    // Find the "[" that starts the timeline ID
    let before_bracket = if let Some(pos) = line.rfind(" [") {
        &line[..pos]
    } else {
        line
    };

    // The branch name is the last whitespace-delimited token
    let name = before_bracket.split_whitespace().next_back()?;
    // Strip any trailing colon (from "┗━ @LSN: name" format — the name comes after colon)
    // Actually in "┗━ @0/1234: develop", "develop" is the last token, which is correct
    Some(name.to_string())
}

// ── Docker-mode branch management ────────────────────────────────────────────

/// Create a branch in Docker mode by:
///   1. Creating a timeline fork in the pageserver via its HTTP API.
///   2. Starting a new compute container pointing at the new timeline.
///   3. Persisting branch metadata to `.neon-tui-docker-branches.json`.
async fn create_docker_branch(config: &Config, name: &str, parent: &str) -> CommandResult {
    #[derive(serde::Deserialize)]
    struct TenantEntry {
        id: String,
    }
    #[derive(serde::Deserialize)]
    struct TimelineEntry {
        timeline_id: String,
        ancestor_timeline_id: Option<String>,
    }

    let ps_port = config.ports.pageserver_http;
    let base = format!("http://127.0.0.1:{ps_port}/v1");

    // 1. Get default tenant ID.
    let tenants: Vec<TenantEntry> = match ureq::get(&format!("{base}/tenant"))
        .call()
        .ok()
        .and_then(|r| r.into_string().ok())
        .and_then(|s| serde_json::from_str(&s).ok())
    {
        Some(t) => t,
        None => return CommandResult::err("Cannot reach pageserver. Is it running?"),
    };
    let tenant_id = match tenants.into_iter().next() {
        Some(t) => t.id,
        None => return CommandResult::err("No tenants found in pageserver."),
    };

    // 2. Fetch timelines to resolve the parent branch's timeline ID.
    let timelines: Vec<TimelineEntry> =
        match ureq::get(&format!("{base}/tenant/{tenant_id}/timeline"))
            .call()
            .ok()
            .and_then(|r| r.into_string().ok())
            .and_then(|s| serde_json::from_str(&s).ok())
        {
            Some(t) => t,
            None => return CommandResult::err("Cannot fetch timelines from pageserver."),
        };

    let state = docker::read_docker_branch_state();
    let parent_timeline_id = if let Some(entry) = state.branches.get(parent) {
        entry.timeline_id.clone()
    } else if parent == config.compute.default_branch {
        // Root timeline = timeline with no ancestor.
        match timelines.iter().find(|t| t.ancestor_timeline_id.is_none()) {
            Some(t) => t.timeline_id.clone(),
            None => return CommandResult::err("Cannot find root timeline in pageserver."),
        }
    } else {
        return CommandResult::err(format!("Parent branch '{parent}' not found."));
    };

    // 3. Create the new timeline.
    let new_timeline_id = generate_hex_id();
    let pg_version = config.compute.pg_version;
    let body = format!(
        r#"{{"new_timeline_id":"{new_timeline_id}","ancestor_timeline_id":"{parent_timeline_id}","pg_version":{pg_version}}}"#
    );
    let resp = ureq::post(&format!("{base}/tenant/{tenant_id}/timeline"))
        .set("Content-Type", "application/json")
        .send_string(&body);
    if let Err(e) = resp {
        return CommandResult::err(format!("Pageserver rejected timeline creation: {e}"));
    }

    // 4. Get the compute image from the existing compose compute container.
    let existing_compute = format!("{}-compute-1", config.docker.compose_project);
    let image_out = tokio::process::Command::new("docker")
        .args(["inspect", "--format", "{{.Config.Image}}", &existing_compute])
        .output()
        .await;
    let compute_image = match image_out {
        Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout).trim().to_string(),
        _ => return CommandResult::err(format!(
            "Cannot inspect compute container '{existing_compute}'. Is it running?"
        )),
    };

    // 5. Pick a free host port (scan upward from default compute port).
    let used_ports: std::collections::HashSet<u16> = state
        .branches
        .values()
        .map(|e| e.port)
        .chain(std::iter::once(config.compute.port))
        .collect();
    let new_port = ((config.compute.port + 1)..=65535)
        .find(|p| !used_ports.contains(p) && !process::is_port_listening(*p))
        .unwrap_or(config.compute.port + 1);

    // 6. Start a new compute container for this branch.
    let container_name = format!("{}-compute-{name}", config.docker.compose_project);
    let network = format!("{}_default", config.docker.compose_project);
    let port_mapping = format!("{new_port}:55432");
    let tenant_env = format!("TENANT_ID={tenant_id}");
    let timeline_env = format!("TIMELINE_ID={new_timeline_id}");
    let pg_version_env = format!("PG_VERSION={pg_version}");

    let run_out = tokio::process::Command::new("docker")
        .args([
            "run", "--rm", "-d",
            "--name", &container_name,
            "--volumes-from", &existing_compute,
            "--network", &network,
            "-p", &port_mapping,
            "-e", &tenant_env,
            "-e", &timeline_env,
            "-e", &pg_version_env,
            "--entrypoint", "/shell/compute.sh",
            &compute_image,
        ])
        .output()
        .await;

    match run_out {
        Ok(o) if o.status.success() => {}
        Ok(o) => return CommandResult::err(format!(
            "Failed to start compute container: {}",
            String::from_utf8_lossy(&o.stderr).trim()
        )),
        Err(e) => return CommandResult::err(format!("docker run failed: {e}")),
    }

    // 7. Wait for the compute to accept connections.
    if !wait_for_port(new_port, Duration::from_secs(30)).await {
        return CommandResult::err(format!(
            "Compute for branch '{name}' did not start within 30 s."
        ));
    }

    // 8. Persist branch metadata.
    let mut state = docker::read_docker_branch_state();
    state.branches.insert(
        name.to_string(),
        docker::DockerBranchEntry {
            timeline_id: new_timeline_id,
            container: Some(container_name),
            port: new_port,
            parent: Some(parent.to_string()),
        },
    );
    docker::save_docker_branch_state(&state);

    let url = connection_url(config, name);
    CommandResult::ok(format!("Branch '{name}' is ready.\nConnection URL: {url}"))
}

/// Delete a branch in Docker mode: stop its compute container and remove state.
async fn delete_docker_branch(config: &Config, name: &str) -> CommandResult {
    let mut state = docker::read_docker_branch_state();
    let entry = match state.branches.get(name) {
        Some(e) => e.clone(),
        None => return CommandResult::err(format!("Branch '{name}' not found in state.")),
    };

    // Stop and remove the compute container if present.
    if let Some(container) = &entry.container {
        let _ = tokio::process::Command::new("docker")
            .args(["stop", container])
            .output()
            .await;
        let _ = tokio::process::Command::new("docker")
            .args(["rm", "-f", container])
            .output()
            .await;
    }

    let timeline_id = entry.timeline_id.clone();
    state.branches.remove(name);
    docker::save_docker_branch_state(&state);

    // Also delete the timeline from the pageserver to avoid dangling timelines.
    let mut warning = String::new();
    if let Some(tenant_id) = get_default_tenant_id(config).await {
        let result = delete_timeline(config, &tenant_id, &timeline_id).await;
        if !result.success {
            warning = format!(" (warning: pageserver timeline delete failed: {})", result.stderr);
        }
    }

    CommandResult::ok(format!(
        "Branch '{name}' and its timeline deleted.{warning}"
    ))
}

/// Get the ID of the first (default) tenant from the pageserver HTTP API.
pub async fn get_default_tenant_id(config: &Config) -> Option<String> {
    #[derive(serde::Deserialize)]
    struct TenantEntry {
        id: String,
    }
    let port = config.ports.pageserver_http;
    let list: Vec<TenantEntry> = tokio::task::spawn_blocking(move || {
        ureq::get(&format!("http://127.0.0.1:{port}/v1/tenant"))
            .call()
            .ok()
            .and_then(|r| r.into_string().ok())
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default()
    })
    .await
    .unwrap_or_default();
    list.into_iter().next().map(|t| t.id)
}

/// Delete a timeline from the pageserver HTTP API.
pub async fn delete_timeline(config: &Config, tenant_id: &str, timeline_id: &str) -> CommandResult {
    let port = config.ports.pageserver_http;
    let tenant_id = tenant_id.to_string();
    let timeline_id = timeline_id.to_string();
    tokio::task::spawn_blocking(move || {
        let url = format!(
            "http://127.0.0.1:{port}/v1/tenant/{tenant_id}/timeline/{timeline_id}"
        );
        match ureq::delete(&url).call() {
            Ok(_) => CommandResult::ok(format!("Timeline {timeline_id} deleted.")),
            Err(ureq::Error::Status(status, response)) => {
                let body = response.into_string().unwrap_or_default();
                CommandResult::err(format!(
                    "Pageserver returned HTTP {status} deleting timeline {timeline_id}: {body}"
                ))
            }
            Err(e) => CommandResult::err(format!(
                "Failed to delete timeline {timeline_id}: {e}"
            )),
        }
    })
    .await
    .unwrap_or_else(|e| CommandResult::err(format!("spawn_blocking failed: {e}")))
}

/// Generate a random 32-char hex string for tenant/timeline IDs.
fn generate_hex_id() -> String {
    let mut bytes = [0u8; 16];
    let f = std::fs::File::open("/dev/urandom");
    if let Ok(mut f) = f {
        let _ = std::io::Read::read_exact(&mut f, &mut bytes);
    }
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_timeline_single_branch() {
        let output = "main [abc123def456]\n";
        let parents = parse_timeline_output(output);
        assert!(parents.is_empty());
    }

    #[test]
    fn parse_timeline_parent_child() {
        // Real neon_local output: first-level children have ┗━ at column 0
        let output = "\
main [abc123def456]
┗━ @0/20CE8A8: develop [def789abc012]
";
        let parents = parse_timeline_output(output);
        assert_eq!(parents.get("develop").map(String::as_str), Some("main"));
        assert!(parents.get("main").is_none());
    }

    #[test]
    fn parse_timeline_deep_hierarchy() {
        let output = "\
main [aaa]
┗━ @0/1234: develop [bbb]
   ┗━ @0/5678: feature [ccc]
";
        let parents = parse_timeline_output(output);
        assert_eq!(parents.get("develop").map(String::as_str), Some("main"));
        assert_eq!(parents.get("feature").map(String::as_str), Some("develop"));
    }

    #[test]
    fn parse_timeline_siblings() {
        let output = "\
main [aaa]
┗━ @0/1234: branch-a [bbb]
┗━ @0/5678: branch-b [ccc]
";
        let parents = parse_timeline_output(output);
        assert_eq!(parents.get("branch-a").map(String::as_str), Some("main"));
        assert_eq!(parents.get("branch-b").map(String::as_str), Some("main"));
    }

    #[test]
    fn parse_timeline_real_output() {
        // Exact output from neon_local timeline list
        let output = "\
main [818c978ff8aa558ebcf634c1c72fc586]
┗━ @0/20CE8A8: develop [8a8d29eaf31c7929bc9f4903714dd276]
   ┗━ @0/20CF8C0: test [fbdd054a70d2d743de52d9b5552a0c3b]
";
        let parents = parse_timeline_output(output);
        assert_eq!(parents.get("develop").map(String::as_str), Some("main"));
        assert_eq!(parents.get("test").map(String::as_str), Some("develop"));
        assert!(parents.get("main").is_none());
    }

    #[test]
    fn extract_branch_name_root() {
        assert_eq!(
            extract_branch_name("main [abc123]"),
            Some("main".to_string())
        );
    }

    #[test]
    fn extract_branch_name_child() {
        assert_eq!(
            extract_branch_name("┗━ @0/20CE8A8: develop [def456]"),
            Some("develop".to_string())
        );
    }
}
