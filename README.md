[![Build Status](https://travis-ci.com/drmorr0/backuppy.svg?branch=master)](https://travis-ci.com/drmorr0/backuppy)

# Backuppy

Open-source, diff-based, encrypted backup software

## Usage

```
python -m backuppy.run backup
```

## Configuration Reference

```
backups:
  backup_1:  # name of the backup set
    key_file: /path/to/encryption/key_file  # required unless disable_encryption is set
    exclusions:  # list of regex patterns that you don't want to back up for this backup
      - pattern1
      - pattern2
      - ...
    directories:  # list of all "root directories" that you want to include in this set
      - /path/to/directory1
      - /path/to/directory2
      - ...
    protocol:  # where to back up the files in this set
      type: (local|ssh|rsync|s3)
      <protocol-specific-options>
    options:
      - max_manifest_versions: (int)
        use_encryption: (true|false)
        use_compression: (true|false)
```

Currently the `local` protocol is the only supported protocol.  It takes a single parameter,
`location`, which is the name of the directory that the set should be backed up to.
