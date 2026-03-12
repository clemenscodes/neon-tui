use std::time::SystemTime;

/// Check if a PID is alive by reading /proc/{pid}/status.
pub fn is_pid_alive(pid: u32) -> bool {
    std::fs::metadata(format!("/proc/{pid}")).is_ok()
}

/// Check if a TCP port is listening by reading /proc/net/tcp.
pub fn is_port_listening(port: u16) -> bool {
    let hex_port = format!("{port:04X}");
    let Ok(contents) = std::fs::read_to_string("/proc/net/tcp") else {
        return false;
    };
    for line in contents.lines().skip(1) {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 4
            && parts[3] == "0A"
            && parts[1].split(':').nth(1).is_some_and(|p| p == hex_port)
        {
            return true;
        }
    }
    // Also check /proc/net/tcp6
    if let Ok(contents6) = std::fs::read_to_string("/proc/net/tcp6") {
        for line in contents6.lines().skip(1) {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 4
                && parts[3] == "0A"
                && parts[1].split(':').nth(1).is_some_and(|p| p == hex_port)
            {
                return true;
            }
        }
    }
    false
}

/// Get the start time of a process from /proc/{pid}/stat.
pub fn process_start_time(pid: u32) -> Option<SystemTime> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // Field 22 (0-indexed: 21) is starttime in clock ticks since boot
    // We need boot time to convert this to absolute time
    let fields: Vec<&str> = stat.rsplit(')').next()?.split_whitespace().collect();
    // After closing paren, field index 19 (0-based) is starttime
    let starttime_ticks: u64 = fields.get(19)?.parse().ok()?;
    let ticks_per_sec = unsafe { libc::sysconf(libc::_SC_CLK_TCK) } as u64;

    let uptime_str = std::fs::read_to_string("/proc/uptime").ok()?;
    let uptime_secs: f64 = uptime_str.split_whitespace().next()?.parse().ok()?;

    let process_start_secs = starttime_ticks / ticks_per_sec;
    let secs_since_start = uptime_secs as u64 - process_start_secs;

    SystemTime::now().checked_sub(std::time::Duration::from_secs(secs_since_start))
}

/// Find a process PID by searching /proc for matching command line arguments.
pub fn find_process_by_arg(endpoint_name: &str, binary_name: &str) -> Option<u32> {
    let proc = std::fs::read_dir("/proc").ok()?;
    for entry in proc.flatten() {
        let name = entry.file_name();
        let pid_str = name.to_string_lossy();
        let Ok(pid) = pid_str.parse::<u32>() else {
            continue;
        };

        let cmdline_path = format!("/proc/{pid}/cmdline");
        let Ok(cmdline) = std::fs::read_to_string(&cmdline_path) else {
            continue;
        };

        // cmdline is null-separated
        if cmdline.contains(binary_name) && cmdline.contains(endpoint_name) {
            return Some(pid);
        }
    }
    None
}
