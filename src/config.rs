use std::env;
use std::path::{Path, PathBuf};

use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct Config {
    pub neon: NeonConfig,
    pub compute: ComputeConfig,
    pub ports: PortsConfig,
    pub ui: UiConfig,
    pub docker: DockerConfig,
}

#[derive(Debug, Clone)]
pub struct DockerConfig {
    /// When true, detect component status via `docker compose ps` instead of PID files.
    pub mode: bool,
    /// Docker Compose project name (defaults to the directory name).
    pub compose_project: String,
}

#[derive(Debug, Clone)]
pub struct NeonConfig {
    pub repo_dir: PathBuf,
    pub bin_dir: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct ComputeConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub database: String,
    pub default_branch: String,
    pub pg_version: u16,
}

#[derive(Debug, Clone)]
pub struct PortsConfig {
    pub pageserver_http: u16,
    pub safekeeper_pg: u16,
    pub storage_broker: u16,
    pub storage_controller: u16,
    pub storage_controller_db: u16,
    pub endpoint_storage: u16,
}

#[derive(Debug, Clone)]
pub struct UiConfig {
    pub refresh_interval_secs: u64,
    pub show_logs: bool,
}

#[derive(Debug, Default, Deserialize)]
struct FileConfig {
    neon: Option<FileNeon>,
    compute: Option<FileCompute>,
    ports: Option<FilePorts>,
    ui: Option<FileUi>,
    docker: Option<FileDocker>,
}

#[derive(Debug, Default, Deserialize)]
struct FileNeon {
    repo_dir: Option<String>,
    bin_dir: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct FileCompute {
    host: Option<String>,
    port: Option<u16>,
    user: Option<String>,
    database: Option<String>,
    default_branch: Option<String>,
    pg_version: Option<u16>,
}

#[derive(Debug, Default, Deserialize)]
struct FilePorts {
    pageserver_http: Option<u16>,
    safekeeper_pg: Option<u16>,
    storage_broker: Option<u16>,
    storage_controller: Option<u16>,
    storage_controller_db: Option<u16>,
    endpoint_storage: Option<u16>,
}

#[derive(Debug, Default, Deserialize)]
struct FileUi {
    refresh_interval: Option<u64>,
    show_logs: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
struct FileDocker {
    mode: Option<bool>,
    compose_project: Option<String>,
}

/// CLI overrides passed from clap
#[derive(Debug, Default)]
pub struct CliOverrides {
    pub config_path: Option<PathBuf>,
    pub repo_dir: Option<PathBuf>,
    pub bin_dir: Option<PathBuf>,
    pub host: Option<String>,
    pub port: Option<u16>,
}

impl Config {
    /// Load configuration with priority: CLI > env > config file > defaults
    pub fn load(cli: &CliOverrides) -> Self {
        let file_cfg = load_config_file(cli.config_path.as_deref());

        let file_neon = file_cfg.neon.unwrap_or_default();
        let file_compute = file_cfg.compute.unwrap_or_default();
        let file_ports = file_cfg.ports.unwrap_or_default();
        let file_ui = file_cfg.ui.unwrap_or_default();
        let file_docker = file_cfg.docker.unwrap_or_default();

        Config {
            neon: NeonConfig {
                repo_dir: cli
                    .repo_dir
                    .clone()
                    .or_else(|| env::var("NEON_REPO_DIR").ok().map(PathBuf::from))
                    .or_else(|| file_neon.repo_dir.map(PathBuf::from))
                    .unwrap_or_else(|| PathBuf::from(".neon")),
                bin_dir: cli
                    .bin_dir
                    .clone()
                    .or_else(|| env::var("NEON_BIN_DIR").ok().map(PathBuf::from))
                    .or_else(|| file_neon.bin_dir.map(PathBuf::from)),
            },
            compute: ComputeConfig {
                host: cli
                    .host
                    .clone()
                    .or_else(|| env::var("COMPUTE_HOST").ok())
                    .or(file_compute.host)
                    .unwrap_or_else(|| "127.0.0.1".to_string()),
                port: cli
                    .port
                    .or_else(|| env_u16("COMPUTE_PORT"))
                    .or(file_compute.port)
                    .unwrap_or(55432),
                user: env::var("COMPUTE_USER")
                    .ok()
                    .or(file_compute.user)
                    .unwrap_or_else(|| "test".to_string()),
                database: env::var("COMPUTE_DB")
                    .ok()
                    .or(file_compute.database)
                    .unwrap_or_else(|| "neondb".to_string()),
                default_branch: env::var("DEFAULT_BRANCH")
                    .ok()
                    .or(file_compute.default_branch)
                    .unwrap_or_else(|| "main".to_string()),
                pg_version: env_u16("PG_VERSION")
                    .or(file_compute.pg_version)
                    .unwrap_or(17),
            },
            ports: PortsConfig {
                pageserver_http: env_u16("PAGESERVER_HTTP_PORT")
                    .or(file_ports.pageserver_http)
                    .unwrap_or(9898),
                safekeeper_pg: env_u16("SAFEKEEPER_PG_PORT")
                    .or(file_ports.safekeeper_pg)
                    .unwrap_or(5454),
                storage_broker: env_u16("STORAGE_BROKER_PORT")
                    .or(file_ports.storage_broker)
                    .unwrap_or(50051),
                storage_controller: env_u16("STORAGE_CONTROLLER_PORT")
                    .or(file_ports.storage_controller)
                    .unwrap_or(1234),
                storage_controller_db: env_u16("STORAGE_CONTROLLER_DB_PORT")
                    .or(file_ports.storage_controller_db)
                    .unwrap_or(1235),
                endpoint_storage: env_u16("ENDPOINT_STORAGE_PORT")
                    .or(file_ports.endpoint_storage)
                    .unwrap_or(9993),
            },
            ui: UiConfig {
                refresh_interval_secs: file_ui.refresh_interval.unwrap_or(2),
                show_logs: file_ui.show_logs.unwrap_or(false),
            },
            docker: DockerConfig {
                mode: env::var("NEON_DOCKER_MODE")
                    .ok()
                    .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                    .or(file_docker.mode)
                    .unwrap_or(false),
                compose_project: env::var("NEON_DOCKER_PROJECT")
                    .ok()
                    .or(file_docker.compose_project)
                    .unwrap_or_else(detect_compose_project),
            },
        }
    }

    pub fn neon_local_bin(&self) -> PathBuf {
        match &self.neon.bin_dir {
            Some(dir) => dir.join("neon_local"),
            None => PathBuf::from("neon_local"),
        }
    }

    pub fn branch_port(&self, branch: &str) -> u16 {
        if branch == self.compute.default_branch {
            return self.compute.port;
        }
        let endpoint_json = self
            .neon
            .repo_dir
            .join("endpoints")
            .join(branch)
            .join("endpoint.json");
        if let Ok(contents) = std::fs::read_to_string(&endpoint_json) {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&contents) {
                if let Some(port) = v.get("pg_port").and_then(|p| p.as_u64()) {
                    return port as u16;
                }
            }
        }
        self.compute.port
    }
}

fn env_u16(key: &str) -> Option<u16> {
    env::var(key).ok().and_then(|v| v.parse().ok())
}

/// Infer the Docker Compose project name from the current working directory.
fn detect_compose_project() -> String {
    std::env::current_dir()
        .ok()
        .and_then(|p| {
            p.file_name()
                .map(|n| n.to_string_lossy().to_lowercase().replace('-', "").replace('_', ""))
        })
        .unwrap_or_else(|| "neon".to_string())
}

fn load_config_file(explicit_path: Option<&Path>) -> FileConfig {
    let candidates: Vec<PathBuf> = if let Some(p) = explicit_path {
        vec![p.to_path_buf()]
    } else if let Ok(p) = env::var("NEON_TUI_CONFIG") {
        vec![PathBuf::from(p)]
    } else {
        let mut paths = vec![PathBuf::from("neon-tui.toml")];
        if let Some(config_dir) = dirs::config_dir() {
            paths.push(config_dir.join("neon-tui").join("config.toml"));
        }
        paths
    };

    for path in candidates {
        if let Ok(contents) = std::fs::read_to_string(&path) {
            match toml::from_str(&contents) {
                Ok(cfg) => return cfg,
                Err(e) => {
                    eprintln!("Warning: failed to parse {}: {e}", path.display());
                }
            }
        }
    }

    FileConfig::default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_sane() {
        let cfg = Config::load(&CliOverrides::default());
        assert_eq!(cfg.compute.port, 55432);
        assert_eq!(cfg.compute.host, "127.0.0.1");
        assert_eq!(cfg.compute.user, "test");
        assert_eq!(cfg.compute.database, "neondb");
        assert_eq!(cfg.compute.default_branch, "main");
        assert_eq!(cfg.ports.pageserver_http, 9898);
    }

    #[test]
    fn cli_overrides_take_priority() {
        let cli = CliOverrides {
            port: Some(12345),
            host: Some("0.0.0.0".to_string()),
            ..Default::default()
        };
        let cfg = Config::load(&cli);
        assert_eq!(cfg.compute.port, 12345);
        assert_eq!(cfg.compute.host, "0.0.0.0");
    }

    #[test]
    fn branch_port_default() {
        let cfg = Config::load(&CliOverrides::default());
        assert_eq!(cfg.branch_port("main"), 55432);
    }

    #[test]
    fn neon_local_bin_with_bin_dir() {
        let cli = CliOverrides {
            bin_dir: Some(PathBuf::from("/nix/store/abc/bin")),
            ..Default::default()
        };
        let cfg = Config::load(&cli);
        assert_eq!(
            cfg.neon_local_bin(),
            PathBuf::from("/nix/store/abc/bin/neon_local")
        );
    }

    #[test]
    fn neon_local_bin_without_bin_dir() {
        // When NEON_BIN_DIR is not set, falls back to plain binary name
        unsafe { std::env::remove_var("NEON_BIN_DIR") };
        let cfg = Config::load(&CliOverrides::default());
        assert_eq!(cfg.neon_local_bin(), PathBuf::from("neon_local"));
    }

    #[test]
    fn show_logs_defaults_to_false() {
        let cfg = Config::load(&CliOverrides::default());
        assert!(!cfg.ui.show_logs);
    }

    #[test]
    fn docker_config_fields_accessible() {
        // Verify DockerConfig fields compile and have the expected types.
        let cfg = Config::load(&CliOverrides::default());
        let _mode: bool = cfg.docker.mode;
        let _project: &str = &cfg.docker.compose_project;
    }
}
