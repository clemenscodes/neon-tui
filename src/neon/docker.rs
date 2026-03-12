use std::collections::HashMap;
use std::process::Stdio;

use serde::Deserialize;

// ── Docker label-based branch state ──────────────────────────────────────────
//
// Branch metadata is stored as Docker labels on the compute containers.
// Docker is the source of truth — no local JSON state file is needed.

/// Metadata for a branch compute container, read from Docker labels.
#[derive(Debug, Clone)]
pub struct BranchContainer {
    pub branch: String,
    pub timeline_id: String,
    pub parent: Option<String>,
    pub container_name: String,
    pub host_port: u16,
    pub running: bool,
}

// ── Serde helpers for `docker inspect` output ────────────────────────────────

#[derive(Deserialize)]
struct InspectEntry {
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "State")]
    state: InspectState,
    #[serde(rename = "Config")]
    config: InspectConfig,
    #[serde(rename = "NetworkSettings")]
    network: InspectNetwork,
}

#[derive(Deserialize)]
struct InspectState {
    #[serde(rename = "Running")]
    running: bool,
    #[allow(dead_code)]
    #[serde(rename = "Pid")]
    pid: u32,
    #[allow(dead_code)]
    #[serde(rename = "StartedAt")]
    started_at: String,
}

#[derive(Deserialize)]
struct InspectConfig {
    #[serde(rename = "Labels")]
    labels: HashMap<String, String>,
}

#[derive(Deserialize)]
struct InspectNetwork {
    #[serde(rename = "Ports")]
    ports: HashMap<String, Option<Vec<PortBinding>>>,
}

#[derive(Deserialize)]
struct PortBinding {
    #[serde(rename = "HostPort")]
    host_port: String,
}

/// List all branch compute containers for the given project by reading Docker labels.
///
/// Uses `docker ps -a --filter label=neon.project={project}` to find containers,
/// then `docker inspect` to read their metadata labels and port bindings.
pub fn list_branch_containers(project: &str) -> Vec<BranchContainer> {
    // Step 1: get names of all containers matching the project label.
    let filter = format!("label=neon.project={project}");
    let output = std::process::Command::new("docker")
        .args([
            "ps", "-a",
            "--filter", &filter,
            "--format", "{{.Names}}",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output();

    let Ok(out) = output else {
        return vec![];
    };
    if !out.status.success() {
        return vec![];
    }

    let names: Vec<String> = String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| l.trim().to_string())
        .collect();

    if names.is_empty() {
        return vec![];
    }

    // Step 2: inspect all containers at once.
    let mut inspect_cmd = std::process::Command::new("docker");
    inspect_cmd.arg("inspect");
    for name in &names {
        inspect_cmd.arg(name);
    }
    let inspect_out = inspect_cmd
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output();

    let Ok(inspect_result) = inspect_out else {
        return vec![];
    };
    if !inspect_result.status.success() {
        return vec![];
    }

    let json_text = String::from_utf8_lossy(&inspect_result.stdout);
    let entries: Vec<InspectEntry> = serde_json::from_str(&json_text).unwrap_or_default();

    // Step 3: build BranchContainer from each entry.
    entries
        .into_iter()
        .filter_map(|entry| {
            let labels = &entry.config.labels;
            let branch = labels.get("neon.branch")?.clone();
            let timeline_id = labels.get("neon.timeline").cloned().unwrap_or_default();
            let parent = labels.get("neon.parent").cloned();

            let host_port = entry
                .network
                .ports
                .get("55432/tcp")
                .and_then(|opt| opt.as_ref())
                .and_then(|bindings| bindings.first())
                .and_then(|b| b.host_port.parse::<u16>().ok())
                .unwrap_or(0);

            let container_name = entry.name.trim_start_matches('/').to_string();

            Some(BranchContainer {
                branch,
                timeline_id,
                parent,
                container_name,
                host_port,
                running: entry.state.running,
            })
        })
        .collect()
}

/// Convenience wrapper: look up a single branch container by branch name.
pub fn inspect_branch_container(project: &str, branch: &str) -> Option<BranchContainer> {
    list_branch_containers(project)
        .into_iter()
        .find(|bc| bc.branch == branch)
}

/// Canonical container name for a branch compute container.
pub fn branch_container_name(project: &str, branch: &str) -> String {
    format!("{project}-compute-{branch}")
}

/// Parsed entry from `docker compose ps --format json`.
#[derive(Debug, Deserialize)]
pub struct DockerPs {
    #[serde(rename = "Service")]
    pub service: String,
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "State")]
    pub state: String,
    /// Container health status ("healthy", "unhealthy", "", …).
    #[serde(rename = "Health")]
    pub health: String,
    /// Port mappings string, e.g. "0.0.0.0:9000->9000/tcp".
    #[serde(rename = "Ports")]
    #[allow(dead_code)]
    pub ports: String,
}

impl DockerPs {
    /// Returns true if the container is running (and healthy when a healthcheck exists).
    pub fn is_running(&self) -> bool {
        if self.state != "running" {
            return false;
        }
        // If there is a healthcheck, only consider it up when healthy.
        match self.health.as_str() {
            "" | "healthy" => true,
            _ => false,
        }
    }
}

/// List all containers for the given Compose project.
///
/// Returns an empty Vec if Docker is not available or the project has no containers.
pub fn list_containers(project: &str) -> Vec<DockerPs> {
    let output = std::process::Command::new("docker")
        .args([
            "compose",
            "--project-name",
            project,
            "ps",
            "--format",
            "json",
            "--all",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output();

    let Ok(out) = output else {
        return vec![];
    };

    if !out.status.success() {
        return vec![];
    }

    let text = String::from_utf8_lossy(&out.stdout);
    // `docker compose ps --format json` outputs either a JSON array (newer Docker)
    // or one JSON object per line (older Docker / Docker Desktop).
    if text.trim_start().starts_with('[') {
        serde_json::from_str::<Vec<DockerPs>>(&text).unwrap_or_default()
    } else {
        text.lines()
            .filter(|l| !l.trim().is_empty())
            .filter_map(|l| serde_json::from_str::<DockerPs>(l).ok())
            .collect()
    }
}

/// Get the host-level PID of the main process in a Docker container.
pub fn container_pid(container_name: &str) -> Option<u32> {
    let output = std::process::Command::new("docker")
        .args(["inspect", "--format", "{{.State.Pid}}", container_name])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let s = String::from_utf8_lossy(&output.stdout);
    let pid: u32 = s.trim().parse().ok()?;
    // Docker returns 0 if the container is not running.
    if pid == 0 { None } else { Some(pid) }
}

/// Get the start time of a Docker container by running `docker inspect`.
pub fn container_started_at(container_name: &str) -> Option<std::time::SystemTime> {
    let output = std::process::Command::new("docker")
        .args(["inspect", "--format", "{{.State.StartedAt}}", container_name])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let s = String::from_utf8_lossy(&output.stdout);
    let s = s.trim();
    // Parse RFC3339 format like "2026-03-12T10:15:30.123456789Z"
    parse_rfc3339_to_system_time(s)
}

fn parse_rfc3339_to_system_time(s: &str) -> Option<std::time::SystemTime> {
    // Format: "2026-03-12T10:15:30.123456789Z"
    let (date_part, time_part) = s.split_once('T')?;
    let date_parts: Vec<&str> = date_part.split('-').collect();
    if date_parts.len() < 3 {
        return None;
    }
    let year: u64 = date_parts[0].parse().ok()?;
    let month: u64 = date_parts[1].parse().ok()?;
    let day: u64 = date_parts[2].parse().ok()?;

    // Strip nanoseconds and timezone suffix
    let time_clean = time_part.split('.').next().unwrap_or(time_part);
    let time_clean = time_clean.trim_end_matches('Z');
    let time_parts: Vec<&str> = time_clean.split(':').collect();
    if time_parts.len() < 3 {
        return None;
    }
    let hour: u64 = time_parts[0].parse().ok()?;
    let min: u64 = time_parts[1].parse().ok()?;
    let sec: u64 = time_parts[2].parse().ok()?;

    // Days since Unix epoch (1970-01-01)
    let days = days_since_epoch(year, month, day)?;
    let secs = days * 86400 + hour * 3600 + min * 60 + sec;
    Some(std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(secs))
}

fn days_since_epoch(year: u64, month: u64, day: u64) -> Option<u64> {
    // Simplified: count days from 1970-01-01
    let mut days: u64 = 0;
    for y in 1970..year {
        days += if is_leap(y) { 366 } else { 365 };
    }
    let months = [31u64, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    for m in 1..month {
        let idx = (m - 1) as usize;
        days += months[idx];
        if m == 2 && is_leap(year) {
            days += 1;
        }
    }
    days += day - 1;
    Some(days)
}

fn is_leap(year: u64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

/// Fetch the last `tail` log lines for a container (stdout + stderr combined).
///
/// Docker writes application logs to stdout/stderr; `docker logs` merges them.
pub fn container_logs(container_name: &str, tail: usize) -> Vec<String> {
    let output = std::process::Command::new("docker")
        .args([
            "logs",
            "--tail",
            &tail.to_string(),
            "--timestamps",
            container_name,
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output();

    match output {
        Ok(out) => {
            // Docker sends application logs to stderr; combine both streams.
            let mut lines: Vec<String> = Vec::new();
            for src in [&out.stdout, &out.stderr] {
                for l in String::from_utf8_lossy(src).lines() {
                    lines.push(l.to_string());
                }
            }
            // Sort by the RFC3339 timestamp prefix so the output is chronological.
            lines.sort();
            if lines.is_empty() {
                vec!["(no log output)".to_string()]
            } else {
                lines
            }
        }
        Err(e) => vec![format!("docker logs failed: {e}")],
    }
}
