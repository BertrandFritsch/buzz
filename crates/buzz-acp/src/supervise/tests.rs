use super::*;

fn projection(status: &str, due: &str) -> Result<Projection> {
    let deadline: Option<i64> = serde_json::from_str(due)?;
    Projection::parse(&serde_json::to_vec(&serde_json::json!({
        "version": 1, "status": status, "generation": "a".repeat(64),
        "obligation_key": (status == "armed").then(|| "b".repeat(64)), "next_due_at": deadline,
    }))?)
}

#[test]
fn terminal_and_waiting_disarm_even_with_persisted_observation_retry() -> Result<()> {
    let pending = projection("armed", "1000")?;
    let mut receipt = Receipt::default();
    receipt.record_attempt(&pending, 1000);
    assert_eq!(receipt.deadline(&pending), Some(11000));
    assert_eq!(receipt.deadline(&projection("waiting", "null")?), None);
    assert_eq!(receipt.deadline(&projection("idle", "null")?), None);
    assert_eq!(receipt.deadline(&projection("gap", "null")?), None);
    assert!(projection("idle", "1000").is_err());
    Ok(())
}

#[test]
fn malformed_projection_and_private_diagnostics_are_rejected() -> Result<()> {
    assert!(Projection::parse(b"{}").is_err());
    let mut pending = projection("armed", "1000")?;
    pending.gap_codes = vec!["ci-discovery-unavailable".into()];
    pending.waiting_count = 1;
    assert!(Projection::parse(&serde_json::to_vec(&pending)?).is_ok());
    pending.gap_codes = vec!["private payload or path".into()];
    assert!(Projection::parse(&serde_json::to_vec(&pending)?).is_err());
    let mut idle = projection("idle", "null")?;
    idle.gap_codes = vec!["ci-discovery-unavailable".into()];
    assert!(Projection::parse(&serde_json::to_vec(&idle)?).is_err());
    Ok(())
}

#[test]
fn retry_is_persisted_by_logical_key_and_bounded_across_generations() -> Result<()> {
    let mut pending = projection("armed", "1000")?;
    let mut receipt = Receipt::default();
    for _ in 0..40 {
        receipt.record_attempt(&pending, 1000);
    }
    assert_eq!(receipt.retry_at, Some(301000));
    assert_eq!(receipt.observation_attempts, 32);
    let saved = serde_json::to_vec(&receipt)?;
    let restored: Receipt = serde_json::from_slice(&saved)?;
    pending.generation = "c".repeat(64);
    assert_eq!(restored.deadline(&pending), Some(301000));
    pending.obligation_key = Some("d".repeat(64));
    assert_eq!(restored.deadline(&pending), Some(1000));
    Ok(())
}

#[test]
fn crash_lock_excludes_alias_and_releases_on_drop() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let root = normalized(directory.path())?;
    let dummy = std::env::current_exe()?;
    let config = Config {
        version: 1,
        state_dir: root.clone(),
        watch_roots: vec![root],
        watch_files: vec![],
        watch_subtrees: vec![],
        projection: CommandSpec {
            executable: dummy.clone(),
            args: vec![],
            timeout_secs: 1,
        },
        tick: CommandSpec {
            executable: dummy,
            args: vec![],
            timeout_secs: 1,
        },
        helper_output_limit: 1024,
        linux_service_unit: None,
    };
    let first = lock_state(&config)?;
    assert!(lock_state(&config).is_err());
    drop(first);
    assert!(lock_state(&config).is_ok());
    Ok(())
}

#[cfg(windows)]
fn native_fixture() -> Result<(tempfile::TempDir, Supervisor, PathBuf)> {
    let directory = tempfile::tempdir()?;
    let root = normalized(directory.path())?;
    let source = root.join("authority.json");
    save_json(&source, &projection("idle", "null")?)?;
    let command = PathBuf::from(
        std::env::var_os("ComSpec").ok_or_else(|| anyhow::anyhow!("test_command_missing"))?,
    );
    let mut config = Config {
        version: 1,
        state_dir: root.join("state"),
        watch_roots: vec![root.clone()],
        watch_files: vec![source.clone()],
        watch_subtrees: vec![],
        projection: CommandSpec {
            executable: command.clone(),
            args: vec![
                "/d".into(),
                "/c".into(),
                "type".into(),
                source.to_string_lossy().into_owned(),
            ],
            timeout_secs: 5,
        },
        tick: CommandSpec {
            executable: command,
            args: vec!["/d".into(), "/c".into(), "echo tick".into()],
            timeout_secs: 5,
        },
        helper_output_limit: 4096,
        linux_service_unit: None,
    };
    config.validate()?;
    let subscription = Subscription::new(Filter {
        roots: config.watch_roots.clone(),
        files: config.watch_files.clone(),
        subtrees: vec![],
        excluded: config.state_dir.clone(),
    })?;
    let supervisor = Supervisor {
        config,
        receipt: Receipt::default(),
        subscription,
        isolation: Isolation::validate(None)?,
        run_id: "fixture".into(),
        projection_runs: 0,
        tick_runs: 0,
    };
    Ok((directory, supervisor, source))
}

#[cfg(windows)]
async fn wait_health(path: &Path, phase: &str) -> Result<serde_json::Value> {
    for _ in 0..200 {
        if let Ok(bytes) = std::fs::read(path) {
            if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes) {
                if value["phase"] == phase {
                    return Ok(value);
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    anyhow::bail!("fixture_health_timeout")
}

#[cfg(windows)]
#[tokio::test]
async fn actual_settled_idle_loop_has_no_recurring_helpers() -> Result<()> {
    let (_directory, mut supervisor, _source) = native_fixture()?;
    let health = supervisor.config.state_dir.join("health.json");
    let task = tokio::spawn(async move { supervisor.drive().await });
    wait_health(&health, "idle").await?;
    // Native ancestor subscriptions can deliver the pre-start directory's
    // final metadata notification after the first snapshot. Settle that finite
    // startup reconciliation before measuring idle helper activity.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let initial: serde_json::Value = serde_json::from_slice(&std::fs::read(&health)?)?;
    tokio::time::sleep(Duration::from_millis(1000)).await;
    let final_health: serde_json::Value = serde_json::from_slice(&std::fs::read(&health)?)?;
    assert!(initial["projection_runs"]
        .as_u64()
        .is_some_and(|runs| runs >= 1));
    assert_eq!(final_health["projection_runs"], initial["projection_runs"]);
    assert_eq!(final_health["tick_runs"], 0);
    assert_eq!(final_health["run_id"], initial["run_id"]);
    task.abort();
    let _ = task.await;
    Ok(())
}

#[cfg(windows)]
#[tokio::test]
async fn actual_temporary_helper_exit_propagates_and_does_not_disarm_into_gap() -> Result<()> {
    let (_directory, mut supervisor, _source) = native_fixture()?;
    supervisor.config.projection.args = vec!["/d".into(), "/c".into(), "exit /b 75".into()];
    let health = supervisor.config.state_dir.join("health.json");
    let error = supervisor
        .drive()
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("temporary failure swallowed"))?;
    assert_eq!(error.to_string(), "helper_temporary_failure");
    assert!(!projection_gap(&error));
    let health: serde_json::Value = serde_json::from_slice(&std::fs::read(&health)?)?;
    assert_eq!(health["phase"], "fatal_helper_or_watch_failure");
    assert_eq!(health["error_code"], "helper_temporary_failure");
    assert_eq!(health["tick_runs"], 0);
    Ok(())
}

#[cfg(windows)]
#[tokio::test]
async fn actual_writer_cancels_future_deadline_without_a_due_helper() -> Result<()> {
    let (_directory, mut supervisor, source) = native_fixture()?;
    let due = Utc::now().timestamp_millis() + 1200;
    save_json(&source, &projection("armed", &due.to_string())?)?;
    let health = supervisor.config.state_dir.join("health.json");
    let task = tokio::spawn(async move { supervisor.drive().await });
    wait_health(&health, "armed").await?;
    save_json(&source, &projection("idle", "null")?)?;
    wait_health(&health, "idle").await?;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let final_health: serde_json::Value = serde_json::from_slice(&std::fs::read(&health)?)?;
    assert_eq!(final_health["phase"], "idle");
    assert_eq!(final_health["tick_runs"], 0);
    task.abort();
    let _ = task.await;
    Ok(())
}

#[cfg(windows)]
#[tokio::test]
async fn mutation_during_real_projection_discards_old_generation() -> Result<()> {
    let (_directory, mut supervisor, source) = native_fixture()?;
    save_json(&source, &projection("armed", "0")?)?;
    let script = source.with_extension("cmd");
    std::fs::write(
        &script,
        format!(
            "@echo off\r\ntype \"{}\"\r\nping -n 2 127.0.0.1 >nul\r\n",
            source.display()
        ),
    )?;
    supervisor.config.projection.args = vec![
        "/d".into(),
        "/c".into(),
        script.to_string_lossy().replace("\\\\?\\", ""),
    ];
    let task = tokio::spawn(async move {
        let projection = supervisor.project().await?;
        Ok::<_, anyhow::Error>((projection, supervisor.projection_runs))
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    save_json(&source, &projection("idle", "null")?)?;
    let (result, runs) = tokio::time::timeout(Duration::from_secs(8), task).await???;
    assert_eq!(result.status, Status::Idle);
    assert!(runs >= 2);
    Ok(())
}
