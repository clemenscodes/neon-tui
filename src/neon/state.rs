use std::path::{Path, PathBuf};
use std::time::SystemTime;

use serde::Deserialize;

use crate::config::Config;
use crate::neon::{docker, process};

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
    /// Docker container name, populated when running in docker mode.
    pub docker_container: Option<String>,
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
    /// Docker container name, populated when running in docker mode.
    pub docker_container: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TimelineInfo {
    pub id: String,
    pub branch_name: Option<String>,
    pub is_root: bool,
}

#[derive(Debug, Clone)]
pub struct TenantInfo {
    pub id: String,
    pub is_default: bool,
    pub timelines: Vec<TimelineInfo>,
}

#[derive(Debug, Deserialize)]
struct EndpointJson {
    pg_port: Option<u16>,
}

/// Read the full Neon state — dispatches to Docker or local mode.
pub fn read_state(config: &Config) -> NeonState {
    if config.docker.mode {
        return read_docker_state(config);
    }
    read_local_state(config)
}

/// Read state from local filesystem / processes (original behaviour).
fn read_local_state(config: &Config) -> NeonState {
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

// ── Docker mode ──────────────────────────────────────────────────────────────

/// Read Neon component state from Docker Compose container status.
///
/// Enabled when `config.docker.mode == true` (e.g. set `NEON_DOCKER_MODE=1`).
///
/// In this mode the TUI shows the six Compose services as components and the
/// compute container as the single "main" branch.  Log lines are fetched via
/// `docker logs` rather than read from local files.
fn read_docker_state(config: &Config) -> NeonState {
    let project = &config.docker.compose_project;
    let containers = docker::list_containers(project);

    // The stack is "initialized" if at least one Neon storage service exists.
    let neon_services = ["storage-broker", "pageserver", "safekeeper", "compute"];
    let initialized = containers
        .iter()
        .any(|c| neon_services.contains(&c.service.as_str()));

    let components = build_docker_components(&containers, config);
    let branches = build_docker_branches(&containers, config);

    let tenants = build_docker_tenants(config);

    NeonState {
        initialized,
        components,
        branches,
        tenants,
        last_refresh: SystemTime::now(),
    }
}

/// Fetch tenants from the pageserver HTTP API in Docker mode.
///
/// Queries `GET /v1/tenant` and then `GET /v1/tenant/{id}/timeline` for each tenant
/// to obtain the timeline list with branch names.  Returns an empty Vec on any network / parse error.
fn build_docker_tenants(config: &Config) -> Vec<TenantInfo> {
    #[derive(Deserialize)]
    struct TenantEntry {
        id: String,
    }

    #[derive(Deserialize)]
    struct TimelineEntry {
        timeline_id: String,
        ancestor_timeline_id: Option<String>,
    }

    let base = format!("http://127.0.0.1:{}/v1", config.ports.pageserver_http);

    // Fetch tenant list.
    let tenant_list: Vec<TenantEntry> = match ureq::get(&format!("{base}/tenant"))
        .call()
        .ok()
        .and_then(|r| r.into_string().ok())
        .and_then(|s| serde_json::from_str(&s).ok())
    {
        Some(list) => list,
        None => return vec![],
    };

    // There is typically one default tenant; treat the first as default.
    let first_id = tenant_list.first().map(|t| t.id.clone());

    // Build timeline_id → branch_name map from Docker labels.
    let project = &config.docker.compose_project;
    let branch_containers = docker::list_branch_containers(project);
    let timeline_to_branch: std::collections::HashMap<String, String> = branch_containers
        .iter()
        .map(|bc| (bc.timeline_id.clone(), bc.branch.clone()))
        .collect();

    tenant_list
        .into_iter()
        .map(|t| {
            let timeline_entries: Vec<TimelineEntry> =
                ureq::get(&format!("{base}/tenant/{}/timeline", t.id))
                    .call()
                    .ok()
                    .and_then(|r| r.into_string().ok())
                    .and_then(|s| serde_json::from_str(&s).ok())
                    .unwrap_or_default();

            let timelines = timeline_entries
                .into_iter()
                .map(|tl| {
                    // The root timeline (no ancestor) maps to the default branch name.
                    let branch_name = if tl.ancestor_timeline_id.is_none() {
                        Some(config.compute.default_branch.clone())
                    } else {
                        timeline_to_branch.get(&tl.timeline_id).cloned()
                    };
                    TimelineInfo {
                        id: tl.timeline_id,
                        branch_name,
                        is_root: tl.ancestor_timeline_id.is_none(),
                    }
                })
                .collect();

            TenantInfo {
                is_default: first_id.as_deref() == Some(&t.id),
                id: t.id,
                timelines,
            }
        })
        .collect()
}

/// Map Docker Compose services to [`ComponentInfo`] entries shown in the Components panel.
fn build_docker_components(
    containers: &[docker::DockerPs],
    config: &Config,
) -> Vec<ComponentInfo> {
    // Core Neon stack — app/app-dev are application concerns, not Neon infrastructure.
    let services: &[(&str, &str, u16)] = &[
        ("minio",          "minio",          9000),
        ("storage-broker", "storage_broker", config.ports.storage_broker),
        ("pageserver",     "pageserver",     config.ports.pageserver_http),
        ("safekeeper",     "safekeeper",     config.ports.safekeeper_pg),
        ("compute",        "compute",        config.compute.port),
    ];

    services
        .iter()
        .map(|(svc, display, port)| {
            let container = containers.iter().find(|c| c.service == *svc);
            let status = match container {
                Some(c) if c.is_running() => Status::Up,
                _ => Status::Down,
            };
            let docker_container = container.map(|c| c.name.clone());
            let (pid, start_time) = if status == Status::Up {
                let p = docker_container.as_deref().and_then(docker::container_pid);
                let t = docker_container
                    .as_deref()
                    .and_then(docker::container_started_at);
                (p, t)
            } else {
                (None, None)
            };
            ComponentInfo {
                name: display.to_string(),
                status,
                pid,
                port: *port,
                log_file: None,
                start_time,
                docker_container,
            }
        })
        .collect()
}

/// Build the Branches panel in Docker mode.
///
/// Shows the default branch (from `docker compose ps`) plus any extra branches
/// created via the TUI (persisted in `.neon-tui-docker-branches.json`).
fn build_docker_branches(containers: &[docker::DockerPs], config: &Config) -> Vec<BranchInfo> {
    let default_branch = &config.compute.default_branch;
    let compute = containers.iter().find(|c| c.service == "compute");
    let default_status = match compute {
        Some(c) if c.is_running() => Status::Up,
        _ => Status::Down,
    };

    let default_container = compute.map(|c| c.name.clone());
    let default_pid = if default_status == Status::Up {
        default_container.as_deref().and_then(docker::container_pid)
    } else {
        None
    };
    let mut branches = vec![BranchInfo {
        name: default_branch.clone(),
        status: default_status,
        pg_port: config.compute.port,
        pid: default_pid,
        is_default: true,
        parent: None,
        log_file: None,
        docker_container: default_container,
    }];

    // Add branches created via the TUI (discovered via Docker labels).
    let project = &config.docker.compose_project;
    let branch_containers = docker::list_branch_containers(project);
    for bc in &branch_containers {
        // Skip the default branch — it is already shown as a Compose service above.
        if bc.branch == *default_branch {
            continue;
        }
        let branch_status = if bc.running { Status::Up } else { Status::Down };
        let branch_pid = if branch_status == Status::Up {
            docker::container_pid(&bc.container_name)
        } else {
            None
        };
        branches.push(BranchInfo {
            name: bc.branch.clone(),
            status: branch_status,
            pg_port: bc.host_port,
            pid: branch_pid,
            is_default: false,
            parent: bc.parent.clone(),
            log_file: None,
            docker_container: Some(bc.container_name.clone()),
        });
    }

    branches
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
        docker_container: None,
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
            docker_container: None,
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
        docker_container: None,
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

    // Parse branch_name_mappings to extract timelines per tenant with branch names.
    // Format: branch_name = [["tenant_id", "timeline_id"]]
    let mut tenant_timelines: std::collections::HashMap<String, Vec<TimelineInfo>> =
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
            // Parse branch_name from left side of '='
            let branch_name = trimmed
                .split('=')
                .next()
                .map(|s| s.trim().to_string())
                .unwrap_or_default();
            // Extract tenant_id and timeline_id from [["tenant_id", "timeline_id"]]
            if let Some(start) = trimmed.find("[[\"") {
                let rest = &trimmed[start + 3..];
                // First string: tenant_id
                if let Some(end) = rest.find('"') {
                    let tenant_id = rest[..end].to_string();
                    // Second string: after the first closing quote, skip separator, find next quoted string
                    let after_tenant = &rest[end + 1..];
                    let timeline_id = after_tenant
                        .find('"')
                        .and_then(|s| {
                            let inner = &after_tenant[s + 1..];
                            inner.find('"').map(|e| inner[..e].to_string())
                        })
                        .unwrap_or_default();

                    if !timeline_id.is_empty() {
                        tenant_timelines
                            .entry(tenant_id)
                            .or_default()
                            .push(TimelineInfo {
                                id: timeline_id,
                                branch_name: Some(branch_name),
                                is_root: false,
                            });
                    } else {
                        // timeline_id not parseable; still record tenant
                        tenant_timelines.entry(tenant_id).or_default();
                    }
                }
            }
        }
    }

    // If we found a default tenant but no mappings, still show it
    if let Some(ref dt) = default_tenant {
        tenant_timelines.entry(dt.clone()).or_default();
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
