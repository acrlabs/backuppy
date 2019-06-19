
# Design Principles

Backups are a critical component to prevent data loss, so we've tried to follow best practices to
ensure that Backuppy is robust under failure and with as few bugs as possible.  This document
outlines our design decisions to this end.

## Testing philosophy

Backuppy strives for comprehensive test coverage, while recognizing that 100% test coverage is
usually not a worthwhile or attainable goal.  We specifically target 99% test coverage, but allow
the use of `pragma: no cover` directives to mark functions that are non-critical or "not worth
testing".  The use of such directives should be minimized and never apply to code that could prevent
a backup from being performed or restored.

"Unit" tests are contained in the `tests` directory, and "integration" tests are in the `itests`
directory.  We take these terms a little loosely; unit tests do not always test a single unit, under
the traditional definition, and integration tests are more along the lines of end-to-end testing of
functionality.  We strive to write _useful_ tests that actually cover real failure scenarios, as
opposed to just writing tests for the sake of test coverage.

Whenever a bug is discovered in the code, a test *must* be written that fails in the presence of the
bug, and passes once the bug is fixed.  Moreover any robustness guarantees made by backuppy *must*
have explicit integration tests to ensure that these guarantees are met.

## Robustness Under Failure

The software should gracefully shut down whenever it can; however, we want to make sure that even in
the event of a non-graceful shutdown such as power loss that data corruption or loss doesn't occur.
Below we outline the "core backup loop" of Backuppy and describe what happens if a failure occurs at
any point:

```
open the manifest
for each file in the directory to back up:
  check if the file matches an exclusion
  record that the file has been "seen"

  if the file doesn't exist in the manifest
    save a new copy of the file to the backup store
    add the file to the manifest
  else if the file has changed since the last backup
    compute a diff between the file and the "original" file in the manifest
    save the diff to the backup store
    update the manifest with the new file
  else if the file's metadata has changed since the last backup
    update the metadata stored in the manifest

  mark every file that hasn't been "seen" as deleted in the manifest
close the manifest
```

### Manifest Guarantees
(M1) The manifest being operated on while a backup is performed is a local copy of the version in
    the backup store.  The version in the backup store will not be updated until the backup is
    complete.  This operation will be atomic, so either the stored manifest will be the old version
    or the new version.

(M2) An entry shall not be commited to the manifest until the associated data is stored in the
    backup store.  It is OK if data is in the store but not in the manifest, as the next time we
    perform a backup we will see that the data is already there and not overwrite it.

(M3) The backup loop may decide to commit intermediate versions of the manifest to minimize the
    amount of work needed if a backup crashes partway through; this operation shall also be atomic.

(M4, TODO) To further protect against data corruption, `n` copies of older manifest versions shall
    be retained, so that even if the latest version is unreadable for some reason, there is still a
    possibility of recovering some data.

### File Guarantees
(F1) If any error occurs while trying to back up a file, the backup loop will log the error and
    continue; failure to back up a single file should not prevent the rest of the backup from
    succeeding.

(F2) If the file contents change while trying to back up the file, an error will occur to avoid
    corrupted backup data.

(F3) Backup operations shall be atomic: there should be no possibility of storing partial data in
    the backup store.  Thus, backup data should be written to a temporary location and then moved to
    their final location in the store.

(F4) Data that is in the store shall never be deleted or overwritten.  Since the stores are indexed
    by sha, it doesn't matter where the data "came from" if the sha matches.  Files that have been
    deleted will instead just be marked as "not present" in the manifest.
