//! Learning of the bus's writes: every write modifies the bus file's `-wal` file, and no read
//! does. The event comes before the write can be read, so a watcher looks a moment later, and
//! now and then without an event too (`.agents/research/watching-the-bus-file.md`).

use std::ffi::OsString;
use std::path::Path;
use std::time::{Duration, Instant};

use jiff::Timestamp;
use notify::event::ModifyKind;
use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher};

/// How long after a write's event a watcher looks at the bus: 30 ms after it, a read saw every
/// write in the runs `.agents/research/watching-the-bus-file.md` records.
pub(crate) const SETTLE: Duration = Duration::from_millis(30);

pub(crate) const BACKSTOP: Duration = Duration::from_secs(1);

/// Watches the folder of the bus file at `path`, and calls `written` on each modification of
/// its `-wal` file. The folder, not the file, which goes whenever the last program closes the
/// bus. `SQLite` puts the `-wal` file beside the file a symbolic link names: `path` is
/// resolved first.
pub(crate) fn watch(
    path: &Path,
    written: impl Fn() + Send + 'static,
) -> Result<RecommendedWatcher, Box<dyn std::error::Error>> {
    let path = std::fs::canonicalize(path)?;
    let folder = path
        .parent()
        .filter(|folder| !folder.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let mut wal = OsString::from(path.file_name().unwrap_or_default());
    wal.push("-wal");
    let mut watcher = notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
        let Ok(event) = event else { return };
        let modified = matches!(
            event.kind,
            EventKind::Modify(ModifyKind::Data(_) | ModifyKind::Any)
        );
        if modified
            && event
                .paths
                .iter()
                .any(|path| path.file_name() == Some(&wal))
        {
            written();
        }
    })?;
    watcher.watch(folder, RecursiveMode::NonRecursive)?;
    Ok(watcher)
}

pub(crate) fn instant_of(deadline: Timestamp) -> Instant {
    const DAY: Duration = Duration::from_secs(24 * 3600);
    let left = Timestamp::now().duration_until(deadline);
    Instant::now() + Duration::try_from(left).unwrap_or_default().min(DAY)
}
