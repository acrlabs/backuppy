
## Backup Loop

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

## Manifest Trace

```
a file object for the unlocked manifest is created in BACKUPPY_SCRATCH
the manifest is loaded from the backup store into the unlocked manifest file
  we create a TemporaryFile locally containing the encrypted manifest
  we decrypt and unpack the TemporaryFile contents into the unlocked manifest file
sqlite opens the unlocked manifest file
... backup runs ...
if the Manifest has changed, we save the unlocked manifest file into the store
  we create a new file at BACKUPPY_SCRATCH/manifest.sqlite
  we compress and encrypt the unlocked manifest file into BACKUPPY_SCRATCH/manifest.sqlite
  the backup_store then atomically saves the locked manifest file into the store
we remove the unlocked manifest file
```

## New File Trace

```
the file to be backed up is copied into a TemporaryFile
the TemporaryFile is saved into the store
  we create a new file at BACKUPPY_SCRATCH/sha_to_path
  we compress and encrypt the contents of the TemporaryFile into BACKUPPY_SCRATCH/sha_to_path
  the backup_store then atomically saves the encrypted file into the store
the file metadata is inserted into the unlocked manifest and committed
```

## Diff File Trace

```
the original file is loaded from the store into TemporaryFile1
  we create TemporaryFile2 containing the encrypted contents of the original file
  we decrypt and unpack the TemporaryFile2 into TemporaryFile1
the diff between the new file and the original file is written to TemporaryFile3
we save TemporaryFile3 into the backup store
  we create a new file at BACKUPPY_SCRATCH/sha_to_path
  we compress and encrypt the contents of TemporaryFile3 into BACKUPPY_SCRATCH/sha_to_path
  the backup_store then atomically saves the encrypted file into the store
the file metadata is inserted into the unlocked manifest and committed
```

## Metadata Changed Trace

```
the new file metadata is inserted into the unlocked manifest and committed
```
