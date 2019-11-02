import os
import signal

import mock
import pytest

from backuppy.stores.backup_store import BackupStore


class DummyBackupStore(BackupStore):
    _save = mock.Mock()
    _load = mock.Mock()
    _delete = mock.Mock()
    _manifest = mock.Mock(changed=True)

    def _query(self, prefix):
        return ['manifest.blah']

    @property
    def options(self):
        return {
            'max_manifest_versions': 10,
            'use_compression': False,
            'use_encryption': False,
        }


def test_shutdown_works():
    backup_store = DummyBackupStore('foo')
    with mock.patch('backuppy.stores.backup_store.unlock_manifest'), \
            mock.patch('backuppy.stores.backup_store.lock_manifest'), \
            backup_store.unlock(), pytest.raises(SystemExit):
        os.kill(os.getpid(), signal.SIGINT)


def test_shutdown_works_with_error():
    backup_store = DummyBackupStore('foo')
    with mock.patch('backuppy.stores.backup_store.unlock_manifest'), \
            mock.patch(
                'backuppy.stores.backup_store.lock_manifest',
                # because we're catching SystemExit the test will call do_cleanup a second time
                side_effect=[Exception, None],
    ), \
            backup_store.unlock(), pytest.raises(SystemExit):
        os.kill(os.getpid(), signal.SIGINT)
