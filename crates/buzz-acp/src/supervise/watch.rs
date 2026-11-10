use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
};

use anyhow::{ensure, Result};
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::mpsc;

/// Resolve existing ancestors too, so a not-yet-created source keeps one identity.
pub(super) fn normalized(path: &Path) -> Result<PathBuf> {
    ensure!(
        path.is_absolute() && path.as_os_str().len() <= 8192,
        "watch_path_invalid"
    );
    let mut cursor = path;
    let mut suffix = Vec::new();
    while !cursor.exists() {
        suffix.push(
            cursor
                .file_name()
                .ok_or_else(|| anyhow::anyhow!("watch_path_unavailable"))?,
        );
        cursor = cursor
            .parent()
            .ok_or_else(|| anyhow::anyhow!("watch_path_unavailable"))?;
        ensure!(suffix.len() <= 64, "watch_path_depth_over_limit");
    }
    let mut result = cursor
        .canonicalize()
        .map_err(|_| anyhow::anyhow!("watch_path_unavailable"))?;
    for component in suffix.into_iter().rev() {
        result.push(component);
    }
    Ok(result)
}

#[derive(Clone, Debug)]
pub(super) struct Filter {
    pub roots: Vec<PathBuf>,
    pub files: Vec<PathBuf>,
    pub subtrees: Vec<PathBuf>,
    pub excluded: PathBuf,
}

impl Filter {
    fn matches(&self, path: &Path, ancestors: bool) -> bool {
        self.files
            .iter()
            .any(|source| path == source || (ancestors && source.starts_with(path)))
            || self
                .subtrees
                .iter()
                .any(|source| path.starts_with(source) || (ancestors && source.starts_with(path)))
    }

    pub fn relevant(&self, path: &Path, ancestors: bool) -> bool {
        // Atomic state replacement can remove a temporary file between exists()
        // and canonicalize(). Its known excluded path must not become a resnapshot.
        if path.starts_with(&self.excluded) {
            return false;
        }
        let Ok(path) = normalized(path) else {
            return self.matches(path, ancestors);
        };
        if path.starts_with(&self.excluded) {
            return false;
        }
        self.matches(&path, ancestors)
    }

    pub fn set_dynamic(&mut self, paths: &[PathBuf]) -> Result<()> {
        ensure!(paths.len() <= 4096, "projection_watch_inputs_over_limit");
        for path in paths {
            let path = normalized(path)?;
            ensure!(
                self.roots.iter().any(|root| path.starts_with(root)),
                "projection_watch_input_outside_roots"
            );
            ensure!(
                !path.starts_with(&self.excluded),
                "projection_watch_input_is_supervisor_state"
            );
            if !self.files.contains(&path) {
                self.files.push(path);
            }
        }
        ensure!(self.files.len() <= 8192, "retained_watch_inputs_over_limit");
        Ok(())
    }
}

#[derive(Clone)]
pub(super) struct Signal {
    pub dirty: Arc<AtomicBool>,
    pub rescan: Arc<AtomicBool>,
    tx: mpsc::Sender<()>,
}

impl Signal {
    fn event(&self, event: notify::Result<Event>, filter: &RwLock<Filter>) {
        let relevant = match event {
            Err(_) => {
                self.rescan.store(true, Ordering::SeqCst);
                true
            }
            Ok(event) if event.need_rescan() || matches!(event.kind, EventKind::Other) => {
                self.rescan.store(true, Ordering::SeqCst);
                true
            }
            Ok(event) if matches!(event.kind, EventKind::Access(_)) => false,
            Ok(event) => match filter.read() {
                Ok(filter) => {
                    if matches!(
                        event.kind,
                        EventKind::Create(_)
                            | EventKind::Remove(_)
                            | EventKind::Modify(notify::event::ModifyKind::Name(_))
                    ) && event.paths.iter().any(|path| {
                        normalized(path).is_ok_and(|path| {
                            filter.roots.iter().any(|root| root.starts_with(&path))
                        })
                    }) {
                        self.rescan.store(true, Ordering::SeqCst);
                    }
                    let ancestors = matches!(
                        event.kind,
                        EventKind::Create(_)
                            | EventKind::Remove(_)
                            | EventKind::Modify(notify::event::ModifyKind::Name(_))
                    );
                    // Parent-directory metadata from provenance/log writes is not
                    // a mission lifecycle event. Ancestors matter for topology only.
                    event
                        .paths
                        .iter()
                        .any(|path| filter.relevant(path, ancestors))
                }
                Err(_) => {
                    self.rescan.store(true, Ordering::SeqCst);
                    true
                }
            },
        };
        if relevant {
            self.dirty.store(true, Ordering::SeqCst);
            let _ = self.tx.try_send(()); // The atomic dirty bit survives a full coalescing queue.
        }
    }
}

pub(super) struct Subscription {
    watcher: Option<RecommendedWatcher>,
    pub filter: Arc<RwLock<Filter>>,
    pub signal: Signal,
    pub rx: mpsc::Receiver<()>,
}

impl Subscription {
    pub fn new(filter: Filter) -> Result<Self> {
        let (tx, rx) = mpsc::channel(1);
        let mut subscription = Self {
            watcher: None,
            filter: Arc::new(RwLock::new(filter)),
            rx,
            signal: Signal {
                dirty: Arc::new(AtomicBool::new(false)),
                rescan: Arc::new(AtomicBool::new(false)),
                tx,
            },
        };
        subscription.rebuild()?;
        Ok(subscription)
    }

    pub fn rebuild(&mut self) -> Result<()> {
        let signal = self.signal.clone();
        let filter = Arc::clone(&self.filter);
        let mut watcher = notify::recommended_watcher(move |event| signal.event(event, &filter))
            .map_err(|_| anyhow::anyhow!("native_watcher_unavailable"))?;
        let mut directories = BTreeMap::new();
        let roots = self
            .filter
            .read()
            .map_err(|_| anyhow::anyhow!("watch_filter_unavailable"))?
            .roots
            .clone();
        for root in roots {
            if root.is_dir() {
                directories.insert(root.clone(), true);
            }
            // Ancestor subscriptions retain root deletion/recreation, without recursively
            // subscribing all of HOME or /srv when a leaf temporarily disappears.
            let mut ancestor = root.parent();
            let mut depth = 0;
            while let Some(path) = ancestor {
                depth += 1;
                ensure!(depth <= 64, "watch_path_depth_over_limit");
                if path.is_dir() {
                    directories.entry(path.to_path_buf()).or_insert(false);
                }
                ancestor = path.parent();
            }
        }
        ensure!(directories.len() <= 256, "watch_directories_over_limit");
        for (path, recursive) in directories {
            watcher
                .watch(
                    &path,
                    if recursive {
                        RecursiveMode::Recursive
                    } else {
                        RecursiveMode::NonRecursive
                    },
                )
                .map_err(|_| anyhow::anyhow!("native_watch_registration_failed"))?;
        }
        self.watcher = Some(watcher);
        Ok(())
    }

    pub fn clear_pending(&mut self) {
        self.signal.dirty.store(false, Ordering::SeqCst);
        while self.rx.try_recv().is_ok() {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn real_atomic_replacement_and_late_directory_are_dirty_but_health_is_not() -> Result<()>
    {
        let directory = tempfile::tempdir()?;
        let root = normalized(directory.path())?;
        let source = root.join("authority.json");
        std::fs::write(&source, "before")?;
        let state = root.join("native-state");
        std::fs::create_dir(&state)?;
        let monitor = root.join("monitor");
        std::fs::create_dir_all(monitor.join("receipts"))?;
        let mut subscription = Subscription::new(Filter {
            roots: vec![root.clone()],
            files: vec![source.clone()],
            subtrees: vec![root.join("later"), monitor.join("receipts")],
            excluded: state.clone(),
        })?;
        std::fs::write(state.join("health.json"), "ignored")?;
        let provenance = monitor.join("provenance.tmp");
        std::fs::write(&provenance, "ignored")?;
        std::fs::rename(provenance, monitor.join("projection-provenance.json"))?;
        assert!(tokio::time::timeout(
            std::time::Duration::from_millis(100),
            subscription.rx.recv()
        )
        .await
        .is_err());
        let replacement = root.join("replacement.tmp");
        std::fs::write(&replacement, "after")?;
        std::fs::rename(replacement, &source)?;
        tokio::time::timeout(std::time::Duration::from_secs(3), subscription.rx.recv()).await?;
        assert!(subscription.signal.dirty.load(Ordering::SeqCst));
        subscription.clear_pending();
        std::fs::create_dir(root.join("later"))?;
        tokio::time::timeout(std::time::Duration::from_secs(3), subscription.rx.recv()).await?;
        assert!(subscription.signal.dirty.load(Ordering::SeqCst));
        Ok(())
    }

    #[test]
    fn overflow_keeps_dirty_even_when_queue_is_full() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let root = normalized(directory.path())?;
        let filter = RwLock::new(Filter {
            roots: vec![root.clone()],
            files: vec![root.join("source")],
            subtrees: vec![],
            excluded: root.join("state"),
        });
        let (tx, _rx) = mpsc::channel(1);
        let signal = Signal {
            dirty: Arc::new(AtomicBool::new(false)),
            rescan: Arc::new(AtomicBool::new(false)),
            tx,
        };
        signal.event(Err(notify::Error::generic("overflow")), &filter);
        signal.event(Err(notify::Error::generic("overflow")), &filter);
        assert!(signal.dirty.load(Ordering::SeqCst));
        assert!(signal.rescan.load(Ordering::SeqCst));
        Ok(())
    }
}
