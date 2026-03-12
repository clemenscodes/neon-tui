# neon-tui

A terminal UI and CLI control plane for [Neon](https://neon.tech) local development (serverless Postgres).

## Features

- Full TUI dashboard for managing local Neon instances
- CLI subcommands for scripting
- Branch management (create, switch, delete)
- Log viewer with live tailing
- Service lifecycle management (init, start, stop, destroy)
- psql integration

## Usage

### TUI mode (no subcommand)
```bash
neon-tui
neon-tui --dir /path/to/.neon
```

### CLI mode
```bash
neon-tui init            # Initialize .neon repository
neon-tui start           # Start all services
neon-tui stop            # Stop all services
neon-tui status          # Show timelines and endpoints
neon-tui branch feat-x   # Create a branch
neon-tui switch feat-x   # Switch to a branch endpoint
neon-tui delete feat-x   # Delete a branch
neon-tui psql            # Connect with psql
neon-tui url             # Print DATABASE_URL
neon-tui destroy -y      # Wipe all data
```

## TUI Keybindings

| Key | Action |
|-----|--------|
| `h`/`l` or `←`/`→` | Navigate panels |
| `j`/`k` or `↓`/`↑` | Move selection |
| `S` | Start selected service |
| `X` | Stop selected service |
| `n` | New branch (in Branches panel) |
| `d` | Delete branch |
| `p` | Open psql for branch |
| `r` | Refresh |
| `?` | Help |
| `q` | Quit |

## Installation

### Nix (recommended)

Add as a flake input:

```nix
inputs.neon-tui.url = "github:clemenscodes/neon-tui";
```

Then use the package:

```nix
inputs.neon-tui.packages.${system}.neon-tui
```

Or run directly:

```bash
nix run github:clemenscodes/neon-tui
```

### Cargo

```bash
cargo install --git https://github.com/clemenscodes/neon-tui
```

## Requirements

- Local [Neon](https://github.com/neondatabase/neon) binaries (configure via `--bin-dir` or config file)
