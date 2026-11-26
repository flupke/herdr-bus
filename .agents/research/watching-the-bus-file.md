# Watching the bus file

`herdr-bus serve` learns of new messages by watching the bus file's folder, and herdr-patch
refreshes the same way; its 30 ms wait and 1 s backstop come from what follows. Seen with inotify
from a second process:

- Every write modifies `bus.db-wal` (`IN_MODIFY`), whether another program holds the file open
  or not.
- No read modifies `bus.db-wal`. A read by the only program that has the file open creates
  `bus.db-wal` and `bus.db-shm`, modifies `bus.db-shm`, and deletes both when it closes; with
  another program holding the file, a read still closes all three files after a write-open
  (`IN_CLOSE_WRITE`).

So a watcher reacts to `IN_MODIFY` of `bus.db-wal` alone: on any event, a consumer that pulls
through the command would wake itself with its own pull. Watch the folder, not the file, which
is deleted whenever the last program closes the bus.

The modification comes before the write is readable: SQLite writes the commit frame, syncs it,
then marks it committed in `-shm`, through mapped memory, which sends no event. A pull right
after the event read the previous seq every time on ext4 (100 of 100 commits of the command; 198
of 200 of a writer that keeps the file open, 49 of 200 on tmpfs); 30 ms after it, it missed none
of 50. A watcher pulls once the folder has been quiet for some tens of milliseconds after the
event, and a long-lived one also pulls now and then, which bounds a miss on a slow sync.
