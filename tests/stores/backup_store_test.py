import mock
import pytest

from backuppy.exceptions import ManifestLockedException
from backuppy.stores.backup_store import BackupStore
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
    class DummyBackupStore(BackupStore):
        _save = mock.Mock()
        _load = mock.Mock()
    with mock.patch('backuppy.stores.backup_store.IOIter') as mock_io_iter:
        mock_io_iter.return_value.__enter__.return_value.stat.return_value = mock.Mock(
            st_uid=1000,
            st_gid=1000,
            st_mode=12345,
        )
        store = DummyBackupStore('fake_backup')
        store._manifest = mock.Mock()
        yield store


def test_init(backup_store):
    assert backup_store.backup_name == 'fake_backup'


@pytest.mark.parametrize('manifest_changed', [True, False])
def test_open_manifest(caplog, backup_store, manifest_changed):
    with mock.patch('backuppy.stores.backup_store.Manifest') as mock_manifest, \
            mock.patch('backuppy.stores.backup_store.os.remove') as mock_remove:
        mock_manifest.return_value.changed = manifest_changed
        with backup_store.open_manifest():
            pass
        assert mock_remove.call_count == 1
        assert count_matching_log_lines(
            'No changes detected; nothing to do', caplog) == 1 - manifest_changed


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
        assert backup_store.save.call_args[1]['dest'] == 'abcdef123'
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
        assert backup_store.save.call_args[1]['dest'] == '111111111'
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
@pytest.mark.parametrize('is_manifest', [True, False])
def test_save(backup_store, is_manifest):
    expected_path = '/tmp/backuppy/12345678' if is_manifest else '/tmp/backuppy/12/34/5678'
    with mock.patch('backuppy.stores.backup_store.IOIter') as mock_io_iter, \
            mock.patch('backuppy.stores.backup_store.compress_and_encrypt') as mock_compress:
        backup_store.save(mock.Mock(), '12345678', is_manifest=is_manifest)
        assert mock_compress.call_count == 1
        assert mock_io_iter.call_args[0][0] == expected_path
        assert backup_store._save.call_args == mock.call(
            expected_path,
            '12345678' if is_manifest else '12/34/5678',
            overwrite=is_manifest,
        )


@pytest.mark.no_mocksaveload
@pytest.mark.parametrize('is_manifest', [True, False])
def test_load(backup_store, is_manifest):
    with mock.patch('backuppy.stores.backup_store.IOIter') as mock_io_iter, \
            mock.patch('backuppy.stores.backup_store.decrypt_and_unpack') as mock_decrypt:
        backup_store.load('12345678', mock.Mock(), is_manifest=is_manifest)
        assert mock_decrypt.call_count == 1
        assert mock_io_iter.call_args == mock.call()
        assert backup_store._load.call_args[0][0] == '12345678' if is_manifest else '12/34/5678'
