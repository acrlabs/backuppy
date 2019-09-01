import os

import mock
import pytest

from backuppy.exceptions import ManifestLockedException
from backuppy.stores.backup_store import BackupStore
from backuppy.util import get_scratch_dir
from tests.conftest import count_matching_log_lines


@pytest.fixture(autouse=True)
def mock_save_load(request):
    if 'no_mocksaveload' in request.keywords:
        yield
    else:
        with mock.patch('backuppy.stores.backup_store.BackupStore.save'), \
                mock.patch('backuppy.stores.backup_store.BackupStore.load'):
            yield


@pytest.fixture
def backup_store(dummy_save_load=True):
    backup_name = 'fake_backup1'

    class DummyBackupStore(BackupStore):
        _save = mock.Mock()
        _load = mock.Mock()
        _delete = mock.Mock()
        _query = mock.Mock(return_value=[])

    with mock.patch('backuppy.stores.backup_store.IOIter') as mock_io_iter:
        mock_io_iter.return_value.__enter__.return_value.stat.return_value = mock.Mock(
            st_uid=1000,
            st_gid=1000,
            st_mode=12345,
        )
        store = DummyBackupStore(backup_name)
        store._manifest = mock.Mock()
        yield store


def test_init(backup_store):
    assert backup_store.backup_name == 'fake_backup1'


@pytest.mark.parametrize('manifest_changed', [True, False])
@pytest.mark.parametrize('manifest_exists', [True, False])
def test_unlock(fs, caplog, backup_store, manifest_changed, manifest_exists):
    os.makedirs(get_scratch_dir())
    with mock.patch('backuppy.stores.backup_store.Manifest') as mock_manifest, \
            mock.patch('backuppy.stores.backup_store.unlock_manifest') as mock_unlock_manifest, \
            mock.patch('backuppy.stores.backup_store.lock_manifest') as mock_lock_manifest, \
            mock.patch('backuppy.stores.backup_store.rmtree') as mock_remove:
        mock_manifest.return_value.changed = manifest_changed
        mock_unlock_manifest.return_value = mock_manifest.return_value
        if manifest_exists:
            backup_store._query.return_value = ['manifest.1234123', 'manifest.1234123.key']
        backup_store.rotate_manifests = mock.Mock()
        with backup_store.unlock():
            pass
        assert mock_unlock_manifest.call_count == manifest_exists
        assert mock_remove.call_count == 2
        assert count_matching_log_lines(
            'No changes detected; nothing to do',
            caplog,
        ) == 1 - manifest_changed
        assert mock_lock_manifest.call_count == manifest_changed
        assert backup_store._manifest is None


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
        assert backup_store.save.call_args[0][1] == 'abcdef123'
        assert backup_store.load.call_count == 0
        assert backup_store.manifest.insert_or_update.call_count == 1


@pytest.mark.parametrize('base_sha', [None, '123456abc'])
def test_save_if_new_sha_different(backup_store, base_sha):
    backup_store.manifest.get_entry.return_value = mock.Mock(sha='abcdef123', base_sha=base_sha)
    with mock.patch('backuppy.stores.backup_store.io_copy') as mock_copy, \
            mock.patch('backuppy.stores.backup_store.compute_sha') as mock_compute_sha, \
            mock.patch('backuppy.stores.backup_store.compute_sha_and_diff') as mock_compute_sha_diff:
        mock_compute_sha.return_value = '321fedcba'
        mock_compute_sha_diff.return_value = ('111111111', mock.Mock())
        backup_store.save_if_new('/foo')
        assert mock_copy.call_count == 0
        assert backup_store.save.call_args[0][1] == '111111111'
        assert backup_store.load.call_count == 1
        assert backup_store.manifest.insert_or_update.call_count == 1


@pytest.mark.parametrize('uid_changed', [True, False])
def test_save_if_new_sha_equal(backup_store, uid_changed):
    entry = mock.Mock(sha='abcdef123', uid=(2000 if uid_changed else 1000), gid=1000, mode=12345)
    backup_store.manifest.get_entry.return_value = entry
    with mock.patch('backuppy.stores.backup_store.io_copy') as mock_copy, \
            mock.patch('backuppy.stores.backup_store.compute_sha') as mock_compute_sha, \
            mock.patch('backuppy.stores.backup_store.ManifestEntry', return_value=entry):
        mock_compute_sha.return_value = 'abcdef123'
        backup_store.save_if_new('/foo')
        assert mock_copy.call_count == 0
        assert backup_store._save.call_count == 0
        assert backup_store._load.call_count == 0
        assert backup_store.manifest.insert_or_update.call_count == int(uid_changed)


@pytest.mark.no_mocksaveload
def test_save(backup_store):
    expected_path = '/tmp/backuppy/12/34/5678'
    with mock.patch('backuppy.stores.backup_store.IOIter') as mock_io_iter, \
            mock.patch('backuppy.stores.backup_store.compress_and_encrypt') as mock_compress:
        backup_store.save(mock.Mock(), '12345678', b'1111')
        src = mock_io_iter.return_value.__enter__.return_value
        assert mock_compress.call_count == 1
        assert mock_io_iter.call_args[0][0] == expected_path
        assert backup_store._save.call_args == mock.call(src, '12/34/5678')


@pytest.mark.no_mocksaveload
def test_load(backup_store):
    with mock.patch('backuppy.stores.backup_store.IOIter') as mock_io_iter, \
            mock.patch('backuppy.stores.backup_store.decrypt_and_unpack') as mock_decrypt:
        backup_store.load('12345678', mock.Mock(), b'1111')
        dest = mock_io_iter.return_value.__enter__.return_value
        assert mock_decrypt.call_count == 1
        assert mock_io_iter.call_args == mock.call()
        assert backup_store._load.call_args == mock.call('12/34/5678', dest)
