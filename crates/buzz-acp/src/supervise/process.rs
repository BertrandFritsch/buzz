use std::{path::PathBuf, process::Stdio, time::Duration};

use anyhow::{bail, ensure, Result};
use process_wrap::tokio::{CommandWrap, KillOnDrop};
use serde::Deserialize;
use tokio::io::{AsyncRead, AsyncReadExt};

#[derive(Clone, Debug, Deserialize)]
pub(super) struct CommandSpec {
    pub executable: PathBuf,
    #[serde(default)]
    pub args: Vec<String>,
    pub timeout_secs: u64,
}

impl CommandSpec {
    pub fn validate(&self, maximum_timeout: u64) -> Result<()> {
        ensure!(
            self.executable.is_absolute(),
            "helper_executable_not_absolute"
        );
        ensure!(self.executable.is_file(), "helper_executable_missing");
        ensure!(self.args.len() <= 64, "helper_arguments_over_limit");
        ensure!(
            self.args
                .iter()
                .all(|arg| arg.len() <= 8192 && !arg.contains('\0')),
            "helper_argument_invalid"
        );
        ensure!(
            (1..=maximum_timeout).contains(&self.timeout_secs),
            "helper_timeout_invalid"
        );
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub(super) struct Isolation {
    #[cfg(target_os = "linux")]
    pub cgroup: PathBuf,
}

impl Isolation {
    pub fn validate(unit: Option<&str>) -> Result<Self> {
        #[cfg(target_os = "linux")]
        {
            let unit = unit.ok_or_else(|| anyhow::anyhow!("dedicated_systemd_unit_required"))?;
            ensure!(
                unit.len() <= 128
                    && unit.ends_with(".service")
                    && unit
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || b"-_.@".contains(&byte)),
                "systemd_unit_invalid"
            );
            let membership = std::fs::read_to_string("/proc/self/cgroup")
                .map_err(|_| anyhow::anyhow!("systemd_cgroup_unavailable"))?;
            let relative = membership
                .lines()
                .find_map(|line| line.strip_prefix("0::"))
                .ok_or_else(|| anyhow::anyhow!("unified_systemd_cgroup_required"))?;
            ensure!(
                std::path::Path::new(relative)
                    .file_name()
                    .is_some_and(|name| name == unit),
                "dedicated_systemd_unit_mismatch"
            );
            let cgroup = PathBuf::from("/sys/fs/cgroup").join(relative.trim_start_matches('/'));
            ensure!(
                cgroup.join("cgroup.procs").is_file(),
                "systemd_cgroup_unavailable"
            );
            let isolation = Self { cgroup };
            isolation.check_empty()?;
            Ok(isolation)
        }
        #[cfg(windows)]
        {
            let _ = unit;
            Ok(Self {})
        }
        #[cfg(not(any(windows, target_os = "linux")))]
        {
            let _ = unit;
            bail!("native_supervision_platform_unsupported")
        }
    }

    pub fn check_empty(&self) -> Result<()> {
        #[cfg(target_os = "linux")]
        {
            use std::io::Read;
            let mut pending = vec![self.cgroup.clone()];
            let mut visited = 0;
            while let Some(group) = pending.pop() {
                visited += 1;
                ensure!(visited <= 256, "systemd_cgroup_tree_over_limit");
                let mut members = String::new();
                std::fs::File::open(group.join("cgroup.procs"))
                    .and_then(|file| file.take(65537).read_to_string(&mut members))
                    .map_err(|_| anyhow::anyhow!("systemd_cgroup_unavailable"))?;
                ensure!(members.len() <= 65536, "systemd_cgroup_members_over_limit");
                for member in members.split_whitespace() {
                    ensure!(
                        member.parse::<u32>().ok() == Some(std::process::id()),
                        "helper_descendant_survived"
                    );
                }
                let mut entries = 0;
                for entry in std::fs::read_dir(&group)
                    .map_err(|_| anyhow::anyhow!("systemd_cgroup_unavailable"))?
                {
                    entries += 1;
                    ensure!(entries <= 512, "systemd_cgroup_tree_over_limit");
                    let entry = entry.map_err(|_| anyhow::anyhow!("systemd_cgroup_unavailable"))?;
                    if entry
                        .file_type()
                        .map_err(|_| anyhow::anyhow!("systemd_cgroup_unavailable"))?
                        .is_dir()
                    {
                        pending.push(entry.path());
                        ensure!(pending.len() <= 256, "systemd_cgroup_tree_over_limit");
                    }
                }
            }
        }
        Ok(())
    }
}

async fn read_bounded<R: AsyncRead + Unpin>(stream: R, limit: usize) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    stream
        .take(limit as u64 + 1)
        .read_to_end(&mut bytes)
        .await
        .map_err(|_| anyhow::anyhow!("helper_pipe_failed"))?;
    ensure!(bytes.len() <= limit, "helper_output_over_limit");
    Ok(bytes)
}

pub(super) async fn execute(
    spec: &CommandSpec,
    output_limit: usize,
    isolation: Option<&Isolation>,
) -> Result<Vec<u8>> {
    let mut command = CommandWrap::with_new(&spec.executable, |command| {
        command
            .args(&spec.args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
    });
    command.wrap(KillOnDrop);
    #[cfg(windows)]
    {
        command.wrap(super::windows_job::WindowsJob);
    }
    #[cfg(unix)]
    command.wrap(process_wrap::tokio::ProcessGroup::leader());
    let mut child = command
        .spawn()
        .map_err(|_| anyhow::anyhow!("helper_containment_or_spawn_failed"))?;
    let stdout = child
        .stdout()
        .take()
        .ok_or_else(|| anyhow::anyhow!("helper_pipe_unavailable"))?;
    let stderr = child
        .stderr()
        .take()
        .ok_or_else(|| anyhow::anyhow!("helper_pipe_unavailable"))?;
    let collect = async {
        let (output, _, status) = tokio::try_join!(
            read_bounded(stdout, output_limit),
            read_bounded(stderr, output_limit),
            async {
                child
                    .wait()
                    .await
                    .map_err(|_| anyhow::anyhow!("helper_wait_failed"))
            }
        )?;
        Ok::<_, anyhow::Error>((output, status))
    };
    let result = tokio::time::timeout(Duration::from_secs(spec.timeout_secs), collect).await;
    let (output, status) = match result {
        Ok(Ok(value)) => value,
        failure => {
            child
                .start_kill()
                .map_err(|_| anyhow::anyhow!("helper_tree_termination_failed"))?;
            tokio::time::timeout(Duration::from_secs(10), child.wait())
                .await
                .map_err(|_| anyhow::anyhow!("helper_tree_termination_timeout"))?
                .map_err(|_| anyhow::anyhow!("helper_tree_termination_failed"))?;
            if let Some(isolation) = isolation {
                isolation.check_empty()?;
            }
            return match failure {
                Ok(Err(error)) => Err(error),
                _ => Err(anyhow::anyhow!("helper_timeout")),
            };
        }
    };
    if let Some(isolation) = isolation {
        isolation.check_empty()?;
    }
    if !status.success() {
        if status.code() == Some(75) {
            bail!("helper_temporary_failure");
        }
        bail!("helper_failed");
    }
    Ok(output)
}

#[cfg(all(test, windows))]
mod tests {
    use super::*;

    fn shell(script: String, timeout_secs: u64) -> Result<CommandSpec> {
        Ok(CommandSpec {
            executable: PathBuf::from(
                std::env::var_os("ComSpec")
                    .ok_or_else(|| anyhow::anyhow!("test_command_missing"))?,
            ),
            args: vec!["/d".into(), "/c".into(), script],
            timeout_secs,
        })
    }

    #[tokio::test]
    async fn actual_output_capture_is_bounded_and_failure_propagates() -> Result<()> {
        let command = shell(
            "for /l %i in (1,1,2000) do @echo output_capture_test".into(),
            5,
        )?;
        let error = execute(&command, 1024, Some(&Isolation::validate(None)?))
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected_output_limit_error"))?;
        assert_eq!(error.to_string(), "helper_output_over_limit");
        Ok(())
    }

    #[tokio::test]
    async fn actual_timeout_kills_nested_windows_job_child() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let marker = directory.path().join("escaped.txt");
        let script = directory.path().join("parent.cmd");
        std::fs::write(&script, format!("@echo off\r\nstart \"\" /b cmd /d /c \"ping -n 4 127.0.0.1 >nul & echo leaked> {}\"\r\nping -n 20 127.0.0.1 >nul\r\n", marker.display()))?;
        let command = shell(script.to_string_lossy().into_owned(), 1)?;
        let error = execute(&command, 1024, Some(&Isolation::validate(None)?))
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected_timeout_error"))?;
        assert_eq!(error.to_string(), "helper_timeout");
        tokio::time::sleep(Duration::from_secs(4)).await;
        assert!(
            !marker.exists(),
            "nested child survived the supervisor timeout"
        );
        Ok(())
    }

    #[tokio::test]
    #[ignore = "internal subprocess fixture invoked by abrupt-death regression"]
    async fn abrupt_death_child_fixture() -> Result<()> {
        let directory = PathBuf::from(
            std::env::var_os("BUZZ_SUPERVISE_CRASH_FIXTURE")
                .ok_or_else(|| anyhow::anyhow!("test_fixture_directory_missing"))?,
        );
        let script = directory.join("crash-parent.cmd");
        std::fs::write(&script, format!("@echo off\r\nstart \"\" /b cmd /d /c \"echo ready> {} & ping -n 4 127.0.0.1 >nul & echo leaked> {}\"\r\nping -n 20 127.0.0.1 >nul\r\n",
            directory.join("ready.txt").display(), directory.join("leaked.txt").display()))?;
        execute(
            &shell(script.to_string_lossy().into_owned(), 30)?,
            4096,
            Some(&Isolation::validate(None)?),
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn abrupt_supervisor_death_closes_kernel_job_and_kills_descendants() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let mut fixture = tokio::process::Command::new(std::env::current_exe()?)
            .creation_flags(windows::Win32::System::Threading::CREATE_NO_WINDOW.0)
            .args([
                "--ignored",
                "--exact",
                "supervise::process::tests::abrupt_death_child_fixture",
            ])
            .env("BUZZ_SUPERVISE_CRASH_FIXTURE", directory.path())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()?;
        let ready = directory.path().join("ready.txt");
        tokio::time::timeout(Duration::from_secs(5), async {
            while !ready.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        fixture.start_kill()?; // Abrupt death: no Rust destructor can close the job.
        fixture.wait().await?;
        tokio::time::sleep(Duration::from_secs(4)).await;
        assert!(
            !directory.path().join("leaked.txt").exists(),
            "kernel job did not contain descendants after parent death"
        );
        Ok(())
    }
}
