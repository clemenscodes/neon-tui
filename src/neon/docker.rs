use std::collections::HashMap;
use std::process::Stdio;

use serde::{Deserialize, Serialize};

// ── Docker branch state ───────────────────────────────────────────────────────
//
// Persisted to .neon-tui-docker-branches.json in the working directory.
// Tracks branches created via the TUI in Docker mode (timeline ID, compute
// container name, exposed host port, parent branch name).

/// Per-branch metadata stored in the local state file.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DockerBranchEntry {
    pub timeline_id: String,
    pub container: Option<String>,
    pub port: u16,
    pub parent: Option<String>,
}

/// Root of the persisted state file.
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct DockerBranchState {
    pub branches: HashMap<String, DockerBranchEntry>,
}

const STATE_FILE: &str = ".neon-tui-docker-branches.json";

pub fn read_docker_branch_state() -> DockerBranchState {
    let Ok(contents) = std::fs::read_to_string(STATE_FILE) else {
        return DockerBranchState::default();
    };
    serde_json::from_str(&contents).unwrap_or_default()
}

pub fn save_docker_branch_state(state: &DockerBranchState) {
    if let Ok(json) = serde_json::to_string_pretty(state) {
        let _ = std::fs::write(STATE_FILE, json);
    }
}

/// Look up the host port for a Docker-mode branch.
/// Returns None for the default branch (which uses config.compute.port directly).
pub fn docker_branch_port(branch: &str) -> Option<u16> {
    let state = read_docker_branch_state();
    state.branches.get(branch).map(|e| e.port)
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
