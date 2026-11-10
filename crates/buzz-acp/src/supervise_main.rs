#![deny(unsafe_code)]
#![cfg_attr(windows, windows_subsystem = "windows")]

fn main() -> anyhow::Result<()> {
    buzz_acp::supervise::run(std::env::args_os().skip(1))
}
