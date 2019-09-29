import argparse
import sqlite3

import mock
import pytest

from backuppy.cli.put import main


@pytest.fixture
def args():
    return argparse.Namespace(
        config='backuppy.conf',
        filename='/foo',
        name='fake_backup1',
        manifest=False,
    )


def test_main_file(args):
    backup_store = mock.MagicMock()
    with mock.patch('backuppy.cli.put.staticconf'), \
            mock.patch('backuppy.cli.put.get_backup_store', return_value=backup_store):
        main(args)
    assert backup_store.unlock.call_count == 1
    assert backup_store.save_if_new.call_args == mock.call('/foo')


def test_main_manifest_manifest_error(args):
    args.manifest = True
    backup_store = mock.MagicMock()
    with mock.patch('backuppy.cli.put.staticconf'), \
            mock.patch('backuppy.cli.put.get_backup_store', return_value=backup_store), \
            mock.patch('backuppy.cli.put.Manifest', side_effect=sqlite3.OperationalError), \
            mock.patch('backuppy.cli.put.lock_manifest') as mock_lock, \
            pytest.raises(sqlite3.OperationalError):
        main(args)
    assert mock_lock.call_count == 0
    assert backup_store.unlock.call_count == 0


def test_main_manifest(args):
    args.manifest = True
    backup_store = mock.MagicMock()
    with mock.patch('backuppy.cli.put.staticconf'), \
            mock.patch('backuppy.cli.put.get_backup_store', return_value=backup_store), \
            mock.patch('backuppy.cli.put.Manifest'), \
            mock.patch('backuppy.cli.put.lock_manifest') as mock_lock:
        main(args)
    assert mock_lock.call_count == 1
    assert backup_store.unlock.call_count == 0
