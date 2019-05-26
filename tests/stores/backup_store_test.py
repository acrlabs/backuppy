import mock
import pytest

from backuppy.exceptions import ManifestLockedException
from backuppy.stores.backup_store import BackupStore
from tests.conftest import count_matching_log_lines


@pytest.fixture
def backup_store():
    class DummyBackupStore(BackupStore):
        _save = mock.Mock()
        _load = mock.Mock()
    with mock.patch('backuppy.stores.backup_store.IOIter'):
        store = DummyBackupStore('fake_backup')
        store._manifest = mock.Mock()
        yield store


def test_init(backup_store):
    assert backup_store.backup_name == 'fake_backup'


@pytest.mark.parametrize('manifest_exists', [True, False])
def test_open_manifest(caplog, backup_store, manifest_exists):
    if not manifest_exists:
        backup_store._load.side_effect = FileNotFoundError
    with mock.patch('backuppy.stores.backup_store.Manifest'), \
            mock.patch('backuppy.stores.backup_store.os.remove') as mock_remove:
        with backup_store.open_manifest():
            pass
        assert count_matching_log_lines('This looks like a new backup location', caplog) == int(not manifest_exists)
        assert mock_remove.call_count == 1


def test_open_locked_manifest(backup_store):
    backup_store._manifest = None
    with pytest.raises(ManifestLockedException):
        backup_store.manifest


def test_save_if_new_with_new_file(backup_store):
    backup_store.manifest.get_entry.return_value = None
    with mock.patch('backuppy.stores.backup_store.io_copy') as mock_copy:
        mock_copy.return_value = 'abcdef123'
        backup_store.save_if_new('/foo')
        assert mock_copy.call_count == 1
        assert backup_store._save.call_args[0][0] == 'ab/cd/ef123'
        assert backup_store._load.call_count == 0


@pytest.mark.parametrize('base_sha', [None, '123456abc'])
def test_save_if_new_with_diff(backup_store, base_sha):
    backup_store.manifest.get_entry.return_value = mock.Mock(sha='abcdef123', base_sha=base_sha)
    with mock.patch('backuppy.stores.backup_store.io_copy') as mock_copy, \
            mock.patch('backuppy.stores.backup_store.compute_sha') as mock_compute_sha, \
            mock.patch('backuppy.stores.backup_store.compute_sha_and_diff') as mock_compute_sha_diff:
        mock_compute_sha.return_value = '321fedcba'
        mock_compute_sha_diff.return_value = ('111111111', mock.Mock())
        backup_store.save_if_new('/foo')
        assert mock_copy.call_count == 0
        assert backup_store._save.call_args[0][0] == '11/11/11111'
        assert backup_store._load.call_count == 1


def test_save_if_new_no_change(backup_store):
    entry = mock.Mock(sha='abcdef123')
    backup_store.manifest.get_entry.return_value = entry
    with mock.patch('backuppy.stores.backup_store.io_copy') as mock_copy, \
            mock.patch('backuppy.stores.backup_store.compute_sha') as mock_compute_sha, \
            mock.patch('backuppy.stores.backup_store.ManifestEntry.from_stat', return_value=entry):
        mock_compute_sha.return_value = 'abcdef123'
        backup_store.save_if_new('/foo')
        assert mock_copy.call_count == 0
        assert backup_store._save.call_count == 0
        assert backup_store._load.call_count == 0
