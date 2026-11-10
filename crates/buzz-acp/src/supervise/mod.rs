//! Mission-aware host scheduling without a model pool or recurring idle helpers.
//!
//! A trusted projection command supplies deadlines from its existing authority;
//! this module only watches that authority and invokes a trusted bounded due command.

mod process;
mod watch;
#[cfg(windows)]
mod windows_job;

use std::{
    ffi::OsString,
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::atomic::Ordering,
    time::Duration,
};

use anyhow::{ensure, Result};
use chrono::Utc;
use clap::Parser;
use fs2::FileExt;
use serde::{Deserialize, Serialize};

use process::{CommandSpec, Isolation};
use watch::{normalized, Filter, Subscription};

const CONFIG_LIMIT: usize = 1024 * 1024;

#[derive(Parser)]
#[command(
    name = "buzz-supervise",
    about = "Watch authoritative mission deadlines without idle model turns"
)]
struct Args {
    #[arg(long)]
    config: PathBuf,
}

#[derive(Clone, Debug, Deserialize)]
struct Config {
    version: u32,
    state_dir: PathBuf,
    watch_roots: Vec<PathBuf>,
    #[serde(default)]
    watch_files: Vec<PathBuf>,
    #[serde(default)]
    watch_subtrees: Vec<PathBuf>,
    projection: CommandSpec,
    tick: CommandSpec,
    #[serde(default = "default_output_limit")]
    helper_output_limit: usize,
    #[serde(default)]
    linux_service_unit: Option<String>,
}

fn default_output_limit() -> usize {
    CONFIG_LIMIT
}

impl Config {
    fn validate(&mut self) -> Result<()> {
        ensure!(self.version == 1, "supervisor_config_version_invalid");
        ensure!(
            self.state_dir.is_absolute(),
            "supervisor_state_not_absolute"
        );
        ensure!(
            (1..=32).contains(&self.watch_roots.len()),
            "watch_roots_count_invalid"
        );
        ensure!(
            self.watch_files.len() <= 4096 && self.watch_subtrees.len() <= 128,
            "watch_inputs_over_limit"
        );
        ensure!(
            (1024..=CONFIG_LIMIT).contains(&self.helper_output_limit),
            "helper_output_limit_invalid"
        );
        self.projection.validate(60)?;
        self.tick.validate(200)?;
        std::fs::create_dir_all(&self.state_dir)
            .map_err(|_| anyhow::anyhow!("supervisor_state_unavailable"))?;
        self.state_dir = normalized(&self.state_dir)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&self.state_dir, std::fs::Permissions::from_mode(0o700))
                .map_err(|_| anyhow::anyhow!("supervisor_state_permissions_failed"))?;
        }
        for root in &mut self.watch_roots {
            *root = normalized(root)?;
            ensure!(root.is_dir(), "watch_root_unavailable");
        }
        for path in self
            .watch_files
            .iter_mut()
            .chain(self.watch_subtrees.iter_mut())
        {
            *path = normalized(path)?;
            ensure!(
                self.watch_roots.iter().any(|root| path.starts_with(root)),
                "watch_input_outside_roots"
            );
            ensure!(
                !path.starts_with(&self.state_dir),
                "watch_input_is_supervisor_state"
            );
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Status {
    Idle,
    Armed,
    Waiting,
    Gap,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Projection {
    version: u32,
    status: Status,
    generation: String,
    #[serde(default)]
    next_due_at: Option<i64>,
    #[serde(default)]
    obligation_key: Option<String>,
    #[serde(default)]
    watch_inputs: Vec<PathBuf>,
    #[serde(default)]
    gap_codes: Vec<String>,
    #[serde(default)]
    waiting_count: u32,
}

fn hex_key(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn projection_gap(error: &anyhow::Error) -> bool {
    matches!(
        error.to_string().as_str(),
        "projection_json_invalid"
            | "projection_identity_invalid"
            | "projection_obligation_invalid"
            | "armed_projection_invalid"
            | "inactive_projection_has_deadline"
            | "projection_diagnostics_invalid"
            | "inactive_projection_has_obligations"
            | "projection_watch_inputs_over_limit"
            | "helper_failed"
    )
}

impl Projection {
    fn parse(bytes: &[u8]) -> Result<Self> {
        let projection: Self = serde_json::from_slice(bytes)
            .map_err(|_| anyhow::anyhow!("projection_json_invalid"))?;
        ensure!(
            projection.version == 1 && hex_key(&projection.generation),
            "projection_identity_invalid"
        );
        ensure!(
            projection.obligation_key.as_deref().is_none_or(hex_key),
            "projection_obligation_invalid"
        );
        ensure!(
            projection.watch_inputs.len() <= 4096,
            "projection_watch_inputs_over_limit"
        );
        ensure!(
            projection.waiting_count <= 4096
                && projection.gap_codes.len() <= 128
                && projection.gap_codes.iter().all(|code| !code.is_empty()
                    && code.len() <= 96
                    && code.bytes().all(|byte| byte.is_ascii_lowercase()
                        || byte.is_ascii_digit()
                        || byte == b'_'
                        || byte == b'-')),
            "projection_diagnostics_invalid"
        );
        match projection.status {
            Status::Armed => ensure!(
                projection.obligation_key.is_some()
                    && projection
                        .next_due_at
                        .is_some_and(|due| (0..=253_402_300_799_000).contains(&due)),
                "armed_projection_invalid"
            ),
            Status::Idle => ensure!(
                projection.next_due_at.is_none() && projection.obligation_key.is_none(),
                "inactive_projection_has_deadline"
            ),
            _ => ensure!(
                projection.next_due_at.is_none(),
                "inactive_projection_has_deadline"
            ),
        }
        ensure!(
            projection.status != Status::Idle
                || (projection.waiting_count == 0 && projection.gap_codes.is_empty()),
            "inactive_projection_has_obligations"
        );
        Ok(projection)
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct Receipt {
    #[serde(default)]
    obligation_key: Option<String>,
    #[serde(default)]
    observation_attempts: u32,
    #[serde(default)]
    retry_at: Option<i64>,
    #[serde(default)]
    projection_launches: u64,
    #[serde(default)]
    tick_launches: u64,
}

impl Receipt {
    fn deadline(&self, projection: &Projection) -> Option<i64> {
        if projection.status != Status::Armed {
            return None;
        }
        let due = projection.next_due_at?;
        Some(if self.obligation_key == projection.obligation_key {
            due.max(self.retry_at.unwrap_or(due))
        } else {
            due
        })
    }

    fn record_attempt(&mut self, projection: &Projection, now: i64) {
        if self.obligation_key != projection.obligation_key {
            self.obligation_key.clone_from(&projection.obligation_key);
            self.observation_attempts = 0;
        }
        self.observation_attempts = self.observation_attempts.saturating_add(1).min(32);
        let exponent = self.observation_attempts.saturating_sub(1).min(5);
        let seconds = (10_u64 * (1_u64 << exponent)).min(300);
        self.retry_at = Some(now.saturating_add(seconds as i64 * 1000));
        self.tick_launches = self.tick_launches.saturating_add(1);
    }
}

fn read_bounded(path: &Path) -> Result<Vec<u8>> {
    ensure!(
        !std::fs::symlink_metadata(path)
            .map_err(|_| anyhow::anyhow!("bounded_file_unavailable"))?
            .file_type()
            .is_symlink(),
        "bounded_file_is_symlink"
    );
    let mut bytes = Vec::new();
    File::open(path)
        .map_err(|_| anyhow::anyhow!("bounded_file_unavailable"))?
        .take(CONFIG_LIMIT as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| anyhow::anyhow!("bounded_file_unavailable"))?;
    ensure!(bytes.len() <= CONFIG_LIMIT, "bounded_file_over_limit");
    Ok(bytes)
}

fn save_json(path: &Path, document: &impl Serialize) -> Result<()> {
    let bytes =
        serde_json::to_vec(document).map_err(|_| anyhow::anyhow!("state_encoding_failed"))?;
    ensure!(bytes.len() <= CONFIG_LIMIT, "supervisor_state_over_limit");
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("supervisor_state_unavailable"))?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)
        .map_err(|_| anyhow::anyhow!("supervisor_state_write_failed"))?;
    temporary
        .write_all(&bytes)
        .map_err(|_| anyhow::anyhow!("supervisor_state_write_failed"))?;
    temporary
        .as_file()
        .sync_all()
        .map_err(|_| anyhow::anyhow!("supervisor_state_write_failed"))?;
    temporary
        .persist(path)
        .map_err(|_| anyhow::anyhow!("supervisor_state_write_failed"))?;
    #[cfg(unix)]
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| anyhow::anyhow!("supervisor_state_write_failed"))?;
    Ok(())
}

fn lock_state(config: &Config) -> Result<File> {
    let mut options = OpenOptions::new();
    options.create(true).read(true).write(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let lock_path = config.state_dir.join("supervisor.lock");
    if lock_path.exists() {
        ensure!(
            !std::fs::symlink_metadata(&lock_path)?
                .file_type()
                .is_symlink(),
            "supervisor_lock_is_symlink"
        );
    }
    let lock = options
        .open(lock_path)
        .map_err(|_| anyhow::anyhow!("supervisor_lock_unavailable"))?;
    FileExt::try_lock_exclusive(&lock)
        .map_err(|_| anyhow::anyhow!("supervisor_already_running_or_lock_failed"))?;
    Ok(lock)
}

struct Supervisor {
    config: Config,
    receipt: Receipt,
    subscription: Subscription,
    isolation: Isolation,
    run_id: String,
    projection_runs: u64,
    tick_runs: u64,
}

impl Supervisor {
    fn health(&self, status: &str, projection: Option<&Projection>) -> Result<()> {
        self.health_with_code(status, projection, None)
    }

    fn health_with_code(
        &self,
        status: &str,
        projection: Option<&Projection>,
        code: Option<&str>,
    ) -> Result<()> {
        save_json(
            &self.config.state_dir.join("health.json"),
            &serde_json::json!({
                "version": 1, "status": status, "phase": status, "observed_at": Utc::now().to_rfc3339(),
                "run_id": self.run_id, "supervisor_pid": std::process::id(), "pid": std::process::id(),
                "projection_runs": self.projection_runs, "tick_runs": self.tick_runs, "error_code": code,
                "logical_key": projection.and_then(|value| value.obligation_key.as_ref()),
                "projection_launches": self.receipt.projection_launches,
                "tick_launches": self.receipt.tick_launches, "next_due_at": projection.and_then(|value| self.receipt.deadline(value)),
                "generation": projection.map(|value| &value.generation),
                "observation_attempts": self.receipt.observation_attempts,
                "gap_codes": projection.map(|value| &value.gap_codes),
                "waiting_count": projection.map(|value| value.waiting_count),
            }),
        )
    }

    fn save_receipt(&self) -> Result<()> {
        save_json(&self.config.state_dir.join("receipt.json"), &self.receipt)
    }

    async fn project(&mut self) -> Result<Projection> {
        let mut rescans = 0;
        loop {
            if self
                .subscription
                .signal
                .rescan
                .swap(false, Ordering::SeqCst)
            {
                rescans += 1;
                ensure!(rescans <= 3, "native_watch_rescan_exhausted");
                self.health("watch_rescan", None)?;
                self.subscription.rebuild()?;
            }
            self.subscription.clear_pending();
            self.receipt.projection_launches = self.receipt.projection_launches.saturating_add(1);
            self.projection_runs = self.projection_runs.saturating_add(1);
            self.save_receipt()?;
            let bytes = process::execute(
                &self.config.projection,
                self.config.helper_output_limit,
                Some(&self.isolation),
            )
            .await?;
            // Notifications arriving during the helper invalidate its result before parsing/arming.
            if self.subscription.signal.dirty.load(Ordering::SeqCst)
                || self.subscription.signal.rescan.load(Ordering::SeqCst)
            {
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
            let projection = Projection::parse(&bytes)?;
            self.subscription
                .filter
                .write()
                .map_err(|_| anyhow::anyhow!("watch_filter_unavailable"))?
                .set_dynamic(&projection.watch_inputs)?;
            return Ok(projection);
        }
    }

    async fn drive(&mut self) -> Result<()> {
        let mut projection: Option<Projection> = None;
        let mut dirty = true;
        loop {
            if dirty {
                match self.project().await {
                    Ok(value) => projection = Some(value),
                    Err(error) if projection_gap(&error) => {
                        projection = None;
                        self.health_with_code("gap", None, Some(&error.to_string()))?;
                    }
                    Err(error) => {
                        self.health_with_code(
                            "fatal_helper_or_watch_failure",
                            None,
                            Some(&error.to_string()),
                        )?;
                        return Err(error);
                    }
                }
                dirty = false;
                if let Some(value) = &projection {
                    let status = match value.status {
                        Status::Idle => "idle",
                        Status::Armed => "armed",
                        Status::Waiting => "waiting",
                        Status::Gap => "gap",
                    };
                    self.health(status, Some(value))?;
                }
            }
            let deadline = projection
                .as_ref()
                .and_then(|value| self.receipt.deadline(value));
            let now = Utc::now().timestamp_millis();
            if deadline.is_some_and(|due| due <= now) {
                // Alarm generation never grants authority. Re-read before every due invocation.
                let current = match self.project().await {
                    Ok(value) => value,
                    Err(error) if projection_gap(&error) => {
                        projection = None;
                        self.health_with_code("gap", None, Some(&error.to_string()))?;
                        continue;
                    }
                    Err(error) => {
                        self.health_with_code(
                            "fatal_projection_failure",
                            None,
                            Some(&error.to_string()),
                        )?;
                        return Err(error);
                    }
                };
                if self.subscription.signal.dirty.load(Ordering::SeqCst) {
                    projection = Some(current);
                    dirty = true;
                    continue;
                }
                if self
                    .receipt
                    .deadline(&current)
                    .is_some_and(|due| due <= Utc::now().timestamp_millis())
                {
                    self.receipt
                        .record_attempt(&current, Utc::now().timestamp_millis());
                    self.save_receipt()?; // Durable launch barrier before the due side effect.
                    self.tick_runs = self.tick_runs.saturating_add(1);
                    self.health("checking_due", Some(&current))?;
                    if let Err(error) = process::execute(
                        &self.config.tick,
                        self.config.helper_output_limit,
                        Some(&self.isolation),
                    )
                    .await
                    {
                        if error.to_string() != "helper_failed" {
                            self.health_with_code(
                                "fatal_due_helper_failure",
                                Some(&current),
                                Some(&error.to_string()),
                            )?;
                            return Err(error);
                        }
                        self.health("due_helper_failed", Some(&current))?;
                    }
                    dirty = true; // Reproject receipt/authority after the finite side effect.
                } else {
                    let status = match current.status {
                        Status::Idle => "idle",
                        Status::Armed => "armed",
                        Status::Waiting => "waiting",
                        Status::Gap => "gap",
                    };
                    self.health(status, Some(&current))?;
                }
                projection = Some(current);
                continue;
            }
            let sleep = async {
                match deadline {
                    Some(deadline) => {
                        // Internal clock reconciliation for unusually long armed deadlines does
                        // not start a helper; an empty obligation set has no sleep at all.
                        let milliseconds = deadline.saturating_sub(now).max(0) as u64;
                        tokio::time::sleep(Duration::from_millis(milliseconds.min(86_400_000)))
                            .await;
                    }
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::select! {
                event = self.subscription.rx.recv() => {
                    ensure!(event.is_some(), "native_watch_channel_closed");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    dirty = true;
                }
                _ = sleep => {}
                _ = shutdown_signal() => { self.health("stopped", projection.as_ref())?; return Ok(()); }
            }
        }
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! { _ = terminate.recv() => {}, _ = tokio::signal::ctrl_c() => {} }
            }
            Err(_) => std::future::pending::<()>().await,
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// Start a credential-free native deadline supervisor from its own CLI arguments.
///
/// Linux execution requires its configured dedicated systemd cgroup with
/// `KillMode=control-group`; use the GUI-subsystem binary for Windows tasks.
pub fn run(arguments: impl IntoIterator<Item = OsString>) -> Result<()> {
    let args = Args::parse_from(std::iter::once(OsString::from("buzz-supervise")).chain(arguments));
    ensure!(args.config.is_absolute(), "supervisor_config_not_absolute");
    let mut config: Config = serde_json::from_slice(&read_bounded(&args.config)?)
        .map_err(|_| anyhow::anyhow!("supervisor_config_invalid"))?;
    config.validate()?;
    let _lock = lock_state(&config)?;
    let run_id = uuid::Uuid::new_v4().to_string();
    let isolation = match Isolation::validate(config.linux_service_unit.as_deref()) {
        Ok(isolation) => isolation,
        Err(error) => {
            save_json(
                &config.state_dir.join("health.json"),
                &serde_json::json!({
                    "version": 1, "status": "fatal_startup_failure", "phase": "fatal_startup_failure",
                    "run_id": run_id, "supervisor_pid": std::process::id(),
                    "projection_runs": 0, "tick_runs": 0, "next_due_at": null, "logical_key": null,
                    "error_code": error.to_string(), "observed_at": Utc::now().to_rfc3339(),
                }),
            )?;
            return Err(error);
        }
    };
    let receipt_path = config.state_dir.join("receipt.json");
    let receipt: Receipt = if receipt_path.exists() {
        serde_json::from_slice(&read_bounded(&receipt_path)?)
            .map_err(|_| anyhow::anyhow!("supervisor_receipt_invalid"))?
    } else {
        Receipt::default()
    };
    ensure!(
        receipt.observation_attempts <= 32
            && receipt.obligation_key.as_deref().is_none_or(hex_key)
            && receipt
                .retry_at
                .is_none_or(|due| (0..=253_402_300_799_000).contains(&due)),
        "supervisor_receipt_invalid"
    );
    let subscription = Subscription::new(Filter {
        roots: config.watch_roots.clone(),
        files: config.watch_files.clone(),
        subtrees: config.watch_subtrees.clone(),
        excluded: config.state_dir.clone(),
    })?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| anyhow::anyhow!("supervisor_runtime_unavailable"))?;
    let mut supervisor = Supervisor {
        config,
        receipt,
        subscription,
        isolation,
        run_id,
        projection_runs: 0,
        tick_runs: 0,
    };
    runtime.block_on(supervisor.drive())
}

#[cfg(test)]
mod tests;
