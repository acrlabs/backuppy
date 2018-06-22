import mock
import pytest

from backuppy.manifest import Manifest
from backuppy.stores.backup_store import BackupStore


@pytest.fixture
def backup_store():
    class DummyBackupStore(BackupStore):
        save = mock.Mock()
        load = mock.Mock()
    return DummyBackupStore


def test_get_manifest(backup_store):
    with mock.patch('backuppy.stores.backup_store.TemporaryFile'), \
            mock.patch('backuppy.stores.backup_store.Manifest', spec=Manifest) as mock_manifest, \
            mock.patch('backuppy.stores.backup_store.yaml') as mock_yaml, \
            mock.patch('backuppy.stores.backup_store.logger') as mock_logger:
        store = backup_store('foo')
        manifest1 = store.manifest
        manifest2 = store.manifest
        assert manifest1 == manifest2
        assert mock_manifest.call_count == 0
        assert store.load.call_count == 1
        assert mock_yaml.load.call_count == 1
        assert mock_logger.warning.call_count == 0


def test_get_new_manifest(backup_store):
    with mock.patch('backuppy.stores.backup_store.TemporaryFile'), \
            mock.patch('backuppy.stores.backup_store.Manifest') as mock_manifest, \
            mock.patch('backuppy.stores.backup_store.yaml') as mock_yaml, \
            mock.patch('backuppy.stores.backup_store.logger') as mock_logger:
        mock_yaml.load.side_effect = Exception
        store = backup_store('foo')
        manifest1 = store.manifest
        manifest2 = store.manifest
        assert manifest1 == manifest2
        assert mock_manifest.call_count == 1
        assert store.load.call_count == 1
        assert mock_yaml.load.call_count == 1
        assert mock_logger.warning.call_count == 1
