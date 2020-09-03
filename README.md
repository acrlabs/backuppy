[![Build Status](https://travis-ci.com/drmorr0/backuppy.svg?branch=master)](https://travis-ci.com/drmorr0/backuppy)

# Backuppy

Open-source, diff-based, encrypted backup software

## Usage

```
python -m backuppy.run backup --name <backup_set_name>
python -m backuppy.run list --name <backup_set_name>
python -m backuppy.run restore --name <backup_set_name> (<file-pattern to restore>)
```

## Configuration Reference

```
backups:
  backup_1:  # name of the backup set
    private_key_filename: /path/to/encryption/key_file  # required unless disable_encryption is set
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

Your private key file needs to be a 4096-bit RSA private key.  You can generate this with the following command:

```
openssl genrsa -out testing.pem 4096
```

## Backup Protocols

Currently the `local` and `s3` protocols are the only ones supported by BackupPY.

### Local backup

```
protocol:
  type: local
  location: /path/to/directory/you/want/to/back/up/to
```

### S3 backup

You must have an S3 bucket created for the backup to succeed, and you must have IAM policies configured
so that BackupPY can access this bucket.

```
protocol:
  type: s3
  aws_access_key_id: YOUR_ACCESS_KEY
  aws_secret_access_key: YOUR_SECRET_ACCESS_KEY
  aws_region: <the name of the region your bucket is in>
  bucket: the-name-of-the-bucket
  storage_class: (optional) the storage class to use for the backed-up objects; defaults to STANDARD
```

A minimal IAM profile for BackupPY to work is as follows:

```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:ListBucket",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::backuppy-testing",   # substitute your bucket name here
                "arn:aws:s3:::backuppy-testing/*"
            ]
        }
    ]
}
```

You can read more about S3 storage classes on the [AWS documentation](https://aws.amazon.com/s3/storage-classes/); valid choices are
`STANDARD`, `STANDARD_IA`, `ONEZONE_IA`, `INTELLIGENT_TIERING`, `GLACIER`, and `DEEP_ARCHIVE`.