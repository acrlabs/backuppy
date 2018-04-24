
# Backuppy

## Robustness Under Failure

The software should gracefully shut down whenever it can; however, we want to make sure that even in the event 
of a non-graceful shutdown that data corruption or loss don't occur.  This section outlines the failure modes
we've considered and how Backuppy will respond:

 - If the backup is interrupted before an updated Manifest has been written to disk, the next time the backup
   runs, we will see that blobs exist for any files that were backed up before the interruption, and we will
   just be able to update the manifest for these files.  We also will periodically save a copy of the Manifest
   so that it is never too out of date from the work that's been done.  Note that it could be the case that
   some files changed between when the backup was interrupted and resumed; in this case, there will be
   dangling blobs in the backup store that we can clean up via garbage collection
 - If the backup is interrupted while writing a blob to the store, we could have a corrupted blob.  We address
   this by writing to a temporary file in the store; once the write is complete, we rename the temporary file
   to the actual file name.  When the backup resumes (or maybe just during garbage collection) we purge any
   temporary files, but at no time are temporary files ever considered "valid" for the sake of restoration.
 - If the backup is interrupted while writing the manifest to the store, we also first write to a temporary
   manifest and then rename.  This failure mode is then equivalent to the first failure mode.

 - what if the file contents change while we're computing a hash?
 - what if the file contents change between when we've started doing the encryption and when we compute the
   entry to store?  We'll just have a dangling blob in this case, but... it'd be really nice to do something
   smarter here
   - use flock? not guaranteed to work but might work in most cases
   - make a copy of the file and perform operations on that?  seems expensive, and I still don't know what
     happens if you change the file while it's being copied
   - compute a before-and-after entry on the file, and discard/retry/something if they differ?
   - some combination of the above?
