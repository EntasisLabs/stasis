use std::path::Path;
#[cfg(test)]
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver};
use std::time::Duration;

use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};

use crate::error::StasisdError;

pub struct ConfigWatcher {
    _watcher: RecommendedWatcher,
    rx: Receiver<()>,
}

impl ConfigWatcher {
    pub fn start(config_path: &Path) -> Result<Self, StasisdError> {
        let (tx, rx) = mpsc::channel();
        let watch_root = if config_path.is_file() {
            config_path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .to_path_buf()
        } else {
            config_path.to_path_buf()
        };
        let filter_file = if config_path.is_file() {
            Some(config_path.to_path_buf())
        } else {
            None
        };

        let mut watcher = notify::recommended_watcher(move |result: Result<Event, notify::Error>| {
            let Ok(event) = result else {
                return;
            };
            if !is_interesting_event(&event.kind) {
                return;
            }
            if let Some(file) = &filter_file {
                if !event.paths.iter().any(|p| p == file) {
                    return;
                }
            } else if !event.paths.iter().any(|p| is_config_path(p)) {
                return;
            }
            let _ = tx.send(());
        })
        .map_err(|err| StasisdError::Runtime(format!("failed to start watcher: {err}")))?;

        watcher
            .watch(&watch_root, RecursiveMode::NonRecursive)
            .map_err(|err| StasisdError::Runtime(format!("failed to watch config path: {err}")))?;

        Ok(Self {
            _watcher: watcher,
            rx,
        })
    }

    /// Block until an event arrives, then wait for quiet `debounce`.
    #[allow(dead_code)] // Public host helper for blocking watch loops.
    pub fn recv_debounced(&self, debounce: Duration) -> Result<(), StasisdError> {
        self.rx
            .recv()
            .map_err(|_| StasisdError::Runtime("config watcher closed".into()))?;
        loop {
            match self.rx.recv_timeout(debounce) {
                Ok(()) => continue,
                Err(mpsc::RecvTimeoutError::Timeout) => return Ok(()),
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(StasisdError::Runtime("config watcher closed".into()));
                }
            }
        }
    }

    pub fn try_recv_debounced(&self, debounce: Duration) -> Result<bool, StasisdError> {
        match self.rx.try_recv() {
            Ok(()) => {
                self.drain_debounce(debounce);
                Ok(true)
            }
            Err(mpsc::TryRecvError::Empty) => Ok(false),
            Err(mpsc::TryRecvError::Disconnected) => {
                Err(StasisdError::Runtime("config watcher closed".into()))
            }
        }
    }

    fn drain_debounce(&self, debounce: Duration) {
        while let Ok(()) = self.rx.recv_timeout(debounce) {}
    }
}

fn is_interesting_event(kind: &EventKind) -> bool {
    matches!(
        kind,
        EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_) | EventKind::Any
    )
}

fn is_config_path(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|ext| ext.to_str()),
        Some("yaml" | "yml" | "toml")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("stasisd-watch-{label}-{nanos}"));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn detects_toml_write_after_debounce() {
        let dir = temp_dir("debounce");
        let watcher = ConfigWatcher::start(&dir).unwrap();
        let file = dir.join("a.toml");
        fs::write(&file, "api_version = \"stasisd/v1\"\n").unwrap();
        // Some platforms coalesce; poll with timeout.
        let started = std::time::Instant::now();
        let mut saw = false;
        while started.elapsed() < Duration::from_secs(2) {
            if watcher
                .try_recv_debounced(Duration::from_millis(50))
                .unwrap()
            {
                saw = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(saw, "expected watch event for toml write");
        let _ = fs::remove_dir_all(dir);
    }
}
