//! Environment capture. A benchmark number without its environment is not a
//! result, so every published run prints this block.

use std::process::Command;

pub struct Environment {
    pub commit: String,
    pub dirty: bool,
    pub cpu: String,
    pub logical_cores: usize,
    pub os: String,
    pub kernel: String,
    pub arch: String,
    pub rustc: String,
    pub profile: &'static str,
}

impl Environment {
    pub fn capture() -> Environment {
        Environment {
            commit: run("git", &["rev-parse", "--short", "HEAD"]).unwrap_or_else(unknown),
            dirty: !run("git", &["status", "--porcelain"])
                .unwrap_or_default()
                .is_empty(),
            cpu: cpu_model(),
            logical_cores: std::thread::available_parallelism()
                .map(|value| value.get())
                .unwrap_or(0),
            os: std::env::consts::OS.to_string(),
            kernel: run("uname", &["-r"]).unwrap_or_else(unknown),
            arch: std::env::consts::ARCH.to_string(),
            rustc: run("rustc", &["--version"]).unwrap_or_else(unknown),
            profile: if cfg!(debug_assertions) {
                "debug"
            } else {
                "release"
            },
        }
    }

    pub fn print(&self) {
        println!("environment:");
        println!(
            "  commit         {}{}",
            self.commit,
            if self.dirty { " (dirty)" } else { "" }
        );
        println!("  cpu            {}", self.cpu);
        println!("  logical cores  {}", self.logical_cores);
        println!(
            "  os             {} {} ({})",
            self.os, self.kernel, self.arch
        );
        println!("  toolchain      {}", self.rustc);
        println!("  profile        {}", self.profile);
        if self.profile == "debug" {
            println!("  note           debug build; timings are not representative");
        }
    }
}

fn unknown() -> String {
    "unknown".to_string()
}

fn run(program: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(program).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn cpu_model() -> String {
    if cfg!(target_os = "macos") {
        return run("sysctl", &["-n", "machdep.cpu.brand_string"]).unwrap_or_else(unknown);
    }
    let Ok(info) = std::fs::read_to_string("/proc/cpuinfo") else {
        return unknown();
    };
    info.lines()
        .find(|line| line.starts_with("model name"))
        .and_then(|line| line.split(':').nth(1))
        .map(|value| value.trim().to_string())
        .unwrap_or_else(unknown)
}
