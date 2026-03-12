use std::path::{Path, PathBuf};
use std::time::SystemTime;

use serde::Deserialize;

use crate::config::Config;
use crate::neon::process;

/// Complete snapshot of local Neon state.
#[derive(Debug, Clone)]
pub struct NeonState {
    pub initialized: bool,
    pub components: Vec<ComponentInfo>,
    pub branches: Vec<BranchInfo>,
    pub tenants: Vec<TenantInfo>,
    pub last_refresh: SystemTime,
}

#[derive(Debug, Clone)]
pub struct ComponentInfo {
    pub name: String,
    pub status: Status,
    pub pid: Option<u32>,
    pub port: u16,
    pub log_file: Option<PathBuf>,
    pub start_time: Option<SystemTime>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Up,
    Down,
}

impl Status {
    pub fn symbol(&self) -> &'static str {
        match self {
            Self::Up => "●",
            Self::Down => "○",
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::Up => "UP",
            Self::Down => "DOWN",
        }
    }
}

#[derive(Debug, Clone)]
pub struct BranchInfo {
    pub name: String,
    pub status: Status,
    pub pg_port: u16,
    pub pid: Option<u32>,
    pub is_default: bool,
    pub parent: Option<String>,
    pub log_file: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct TenantInfo {
    pub id: String,
    pub is_default: bool,
    pub timelines: usize,
}

#[derive(Debug, Deserialize)]
struct EndpointJson {
    pg_port: Option<u16>,
}

/// Read the full Neon state from the filesystem and running processes.
pub fn read_state(config: &Config) -> NeonState {
    let repo_dir = &config.neon.repo_dir;
    let initialized = repo_dir.is_dir() && repo_dir.join("config").exists();

    let components = if initialized {
        read_components(config)
    } else {
        vec![]
    };

    let branches = if initialized {
        read_branches(config)
    } else {
        vec![]
    };

    // NOTE: timeline hierarchy (parent info) is populated separately
    // by App::apply_cached_hierarchy() to avoid blocking the UI thread.
    // parse_timeline_hierarchy() runs `neon_local timeline list` which
    // can hang when neon is starting up.

    let tenants = if initialized {
        read_tenants(config)
    } else {
        vec![]
    };

    NeonState {
        initialized,
        components,
        branches,
        tenants,
        last_refresh: SystemTime::now(),
    }
}

fn read_components(config: &Config) -> Vec<ComponentInfo> {
    let repo = &config.neon.repo_dir;
    let ports = &config.ports;
    let ps_id = pageserver_id();
    let sc_id = storage_controller_id();

    vec![
        read_component(
            "storage_broker",
            ports.storage_broker,
            &repo.join("storage_broker.pid"),
            Some(repo.join("storage_broker").join("storage_broker.log")),
        ),
        read_component(
            "storage_controller_db",
            ports.storage_controller_db,
            &repo.join("storage_controller_db").join("postmaster.pid"),
            Some(repo.join("storage_controller_db").join("postgres.log")),
        ),
        read_component(
            "storage_controller",
            ports.storage_controller,
            &repo
                .join(format!("storage_controller_{sc_id}"))
                .join("storage_controller.pid"),
            Some(
                repo.join(format!("storage_controller_{sc_id}"))
                    .join("storage_controller.log"),
            ),
        ),
        read_component(
            "pageserver",
            ports.pageserver_http,
            &repo
                .join(format!("pageserver_{ps_id}"))
                .join("pageserver.pid"),
            Some(
                repo.join(format!("pageserver_{ps_id}"))
                    .join("pageserver.log"),
            ),
        ),
        read_component(
            "safekeeper",
            ports.safekeeper_pg,
            &safekeeper_pid_path(repo),
            Some(safekeeper_log_path(repo)),
        ),
        read_component(
            "endpoint_storage",
            ports.endpoint_storage,
            &repo.join("endpoint_storage").join("endpoint_storage.pid"),
            Some(repo.join("endpoint_storage").join("endpoint_storage.log")),
        ),
    ]
}

fn read_component(
    name: &str,
    port: u16,
    pid_file: &Path,
    log_file: Option<PathBuf>,
) -> ComponentInfo {
    let pid = read_pid_file(pid_file);
    let alive = pid.is_some_and(process::is_pid_alive);
    let port_open = port > 0 && process::is_port_listening(port);
    let status = if alive || port_open {
        Status::Up
    } else {
        Status::Down
    };
    let start_time = if status == Status::Up {
        pid.and_then(process::process_start_time)
    } else {
        None
    };

    ComponentInfo {
        name: name.to_string(),
        status,
        pid,
        port,
        log_file,
        start_time,
    }
}

fn read_branches(config: &Config) -> Vec<BranchInfo> {
    let endpoints_dir = config.neon.repo_dir.join("endpoints");

    let mut branches = Vec::new();

    // Always include the default branch
    let default_branch = &config.compute.default_branch;
    let default_endpoint = endpoints_dir.join(default_branch).join("endpoint.json");
    let default_info = if default_endpoint.exists() {
        read_branch_endpoint(config, default_branch, &default_endpoint)
    } else {
        BranchInfo {
            name: default_branch.clone(),
            status: if process::is_port_listening(config.compute.port) {
                Status::Up
            } else {
                Status::Down
            },
            pg_port: config.compute.port,
            pid: None,
            is_default: true,
            parent: None,
            log_file: compute_log_path(&endpoints_dir, default_branch),
        }
    };
    branches.push(default_info);

    // Read other endpoints
    if let Ok(entries) = std::fs::read_dir(&endpoints_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if name == *default_branch {
                continue;
            }
            let json_path = entry.path().join("endpoint.json");
            if json_path.exists() {
                branches.push(read_branch_endpoint(config, &name, &json_path));
            }
        }
    }

    branches
}

fn read_branch_endpoint(config: &Config, name: &str, json_path: &Path) -> BranchInfo {
    let is_default = name == config.compute.default_branch;
    let mut pg_port = if is_default { config.compute.port } else { 0 };

    if let Ok(contents) = std::fs::read_to_string(json_path) {
        if let Ok(ep) = serde_json::from_str::<EndpointJson>(&contents) {
            if let Some(p) = ep.pg_port {
                pg_port = p;
            }
        }
    }

    let pid = find_compute_pid(name);
    let alive = pid.is_some_and(process::is_pid_alive);
    let port_open = pg_port > 0 && process::is_port_listening(pg_port);
    let endpoints_dir = config.neon.repo_dir.join("endpoints");

    BranchInfo {
        name: name.to_string(),
        status: if alive || port_open {
            Status::Up
        } else {
            Status::Down
        },
        pg_port,
        pid,
        is_default,
        parent: None, // populated later from timeline hierarchy
        log_file: compute_log_path(&endpoints_dir, name),
    }
}

fn compute_log_path(endpoints_dir: &Path, name: &str) -> Option<PathBuf> {
    let path = endpoints_dir.join(name).join("compute.log");
    if path.exists() {
        Some(path)
    } else {
        None
    }
}

/// Find compute_ctl PID for a given endpoint name.
fn find_compute_pid(endpoint_name: &str) -> Option<u32> {
    process::find_process_by_arg(endpoint_name, "compute_ctl")
}

fn read_pid_file(path: &Path) -> Option<u32> {
    let contents = std::fs::read_to_string(path).ok()?;
    // PostgreSQL postmaster.pid files are multi-line; PID is always the first line
    contents.lines().next()?.trim().parse().ok()
}

fn pageserver_id() -> &'static str {
    static ID: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    ID.get_or_init(|| std::env::var("PAGESERVER_ID").unwrap_or_else(|_| "1".to_string()))
}

fn storage_controller_id() -> &'static str {
    static ID: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    ID.get_or_init(|| std::env::var("STORAGE_CONTROLLER_ID").unwrap_or_else(|_| "1".to_string()))
}

fn safekeeper_pid_path(repo: &Path) -> PathBuf {
    let id = std::env::var("SAFEKEEPER_ID").unwrap_or_else(|_| "1".to_string());
    repo.join("safekeepers")
        .join(format!("sk{id}"))
        .join("safekeeper.pid")
}

/// Sort branches into tree order: parent before children, depth-first.
pub fn sort_branches_by_tree(branches: &mut Vec<BranchInfo>) {
    let mut result: Vec<BranchInfo> = Vec::with_capacity(branches.len());
    let mut remaining: Vec<BranchInfo> = std::mem::take(branches);

    // First pass: add root branches (no parent)
    fn add_children(
        parent_name: &str,
        remaining: &mut Vec<BranchInfo>,
        result: &mut Vec<BranchInfo>,
    ) {
        let mut i = 0;
        while i < remaining.len() {
            if remaining[i]
                .parent
                .as_deref()
                .is_some_and(|p| p == parent_name)
            {
                let child = remaining.remove(i);
                let child_name = child.name.clone();
                result.push(child);
                add_children(&child_name, remaining, result);
            } else {
                i += 1;
            }
        }
    }

    // Start with root nodes (no parent)
    let mut i = 0;
    while i < remaining.len() {
        if remaining[i].parent.is_none() {
            let root = remaining.remove(i);
            let root_name = root.name.clone();
            result.push(root);
            add_children(&root_name, &mut remaining, &mut result);
        } else {
            i += 1;
        }
    }

    // Any orphans (parent not found) go at the end
    result.extend(remaining);
    *branches = result;
}

fn read_tenants(config: &Config) -> Vec<TenantInfo> {
    let config_path = config.neon.repo_dir.join("config");
    let Ok(contents) = std::fs::read_to_string(&config_path) else {
        return vec![];
    };

    // Parse default_tenant_id from neon config
    let default_tenant = contents
        .lines()
        .find(|l| l.trim().starts_with("default_tenant_id"))
        .and_then(|l| l.split('=').nth(1))
        .map(|v| v.trim().trim_matches('"').to_string());

    // Parse branch_name_mappings to count timelines per tenant
    // Format: branch_name = [["tenant_id", "timeline_id"]]
    let mut tenant_timelines: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    let mut in_mappings = false;
    for line in contents.lines() {
        let trimmed = line.trim();
        if trimmed == "[branch_name_mappings]" {
            in_mappings = true;
            continue;
        }
        if trimmed.starts_with('[') && in_mappings {
            break;
        }
        if in_mappings && trimmed.contains('=') {
            // Extract tenant_id from [["tenant_id", "timeline_id"]]
            if let Some(start) = trimmed.find("[[\"") {
                let rest = &trimmed[start + 3..];
                if let Some(end) = rest.find('"') {
                    let tenant_id = &rest[..end];
                    *tenant_timelines.entry(tenant_id.to_string()).or_insert(0) += 1;
                }
            }
        }
    }

    // If we found a default tenant but no mappings, still show it
    if let Some(ref dt) = default_tenant {
        tenant_timelines.entry(dt.clone()).or_insert(0);
    }

    let mut tenants: Vec<TenantInfo> = tenant_timelines
        .into_iter()
        .map(|(id, timelines)| TenantInfo {
            is_default: default_tenant.as_deref() == Some(&id),
            id,
            timelines,
        })
        .collect();

    // Sort: default first
    tenants.sort_by(|a, b| b.is_default.cmp(&a.is_default).then(a.id.cmp(&b.id)));
    tenants
}

fn safekeeper_log_path(repo: &Path) -> PathBuf {
    let id = std::env::var("SAFEKEEPER_ID").unwrap_or_else(|_| "1".to_string());
    repo.join("safekeepers")
        .join(format!("sk{id}"))
        .join(format!("safekeeper-{id}.log"))
}
