import staticconf

from backuppy.exceptions import UnknownProtocolError
from backuppy.stores.backup_store import BackupStore
from backuppy.stores.local_backup_store import LocalBackupStore
from backuppy.stores.s3_backup_store import S3BackupStore


__all__ = [
    'LocalBackupStore',
]


def get_backup_store(backup_name) -> BackupStore:  # pragma: no cover
    protocol = staticconf.read_string('protocol.type', namespace=backup_name)  # type: ignore[attr-defined]
    if protocol == 'local':
        return LocalBackupStore(backup_name)
    elif protocol == 'ssh':
        raise NotImplementedError('ssh protocol not supported')
    elif protocol == 's3':
        return S3BackupStore(backup_name)
    else:
        raise UnknownProtocolError(f'Protocol {protocol} is not recognized')
