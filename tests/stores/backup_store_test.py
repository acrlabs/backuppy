import os

import mock
import pytest
import staticconf.testing

from backuppy.exceptions import DiffTooLargeException
from backuppy.exceptions import ManifestLockedException
from backuppy.manifest import ManifestEntry
from backuppy.stores.backup_store import BackupStore
from backuppy.util import get_scratch_dir
from tests.conftest import count_matching_log_lines


@pytest.fixture(autouse=True)
def mock_save_load(request):
    if 'no_mocksaveload' in request.keywords:
        yield
    else:
        with mock.patch('backuppy.stores.backup_store.BackupStore.save', return_value=b'2222'), \
                mock.patch('backuppy.stores.backup_store.BackupStore.load'):
            yield


@pytest.fixture
def current_entry():
    return ManifestEntry(
        '/foo',
        'abcdef123',
        None,
        1000,
        1000,
        12345,
        b'aaaaa2222',
        None,
    )


@pytest.fixture
def backup_store():
    backup_name = 'fake_backup1'

    class DummyBackupStore(BackupStore):
        _save = mock.Mock()
        _load = mock.Mock()
        _delete = mock.Mock()
        _query = mock.Mock(return_value=[])

    with mock.patch('backuppy.stores.backup_store.IOIter') as mock_io_iter:
        mock_io_iter.return_value.__enter__.return_value.uid = 1000
        mock_io_iter.return_value.__enter__.return_value.gid = 1000
        mock_io_iter.return_value.__enter__.return_value.mode = 12345
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
            backup_store._query.return_value = ['manifest.1234123']
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


@pytest.mark.parametrize('dry_run', [True, False])
def test_save_if_new_with_new_file(backup_store, dry_run):
    backup_store.manifest.get_entry.return_value = None
    backup_store._write_copy = mock.Mock()
    backup_store._write_diff = mock.Mock()
    with mock.patch('backuppy.stores.backup_store.compute_sha', return_value=None):
        backup_store.save_if_new('/foo', dry_run)
    assert backup_store._write_copy.call_count == 1
    assert backup_store._write_diff.call_count == 0
    assert backup_store.manifest.insert_or_update.call_count == int(not dry_run)


@pytest.mark.parametrize('dry_run', [True, False])
def test_save_if_new_sha_different(backup_store, dry_run):
    backup_store.manifest.get_entry.return_value = mock.Mock(sha='abcdef123')
    backup_store._write_copy = mock.Mock()
    backup_store._write_diff = mock.Mock()
    with mock.patch('backuppy.stores.backup_store.compute_sha', return_value='321fedcba'):
        backup_store.save_if_new('/foo', dry_run)
    assert backup_store._write_copy.call_count == 0
    assert backup_store._write_diff.call_count == 1
    assert backup_store.manifest.insert_or_update.call_count == int(not dry_run)


@pytest.mark.parametrize('uid_changed', [True, False])
@pytest.mark.parametrize('dry_run', [True, False])
def test_save_if_new_sha_equal(backup_store, uid_changed, dry_run):
    entry = mock.Mock(sha='abcdef123', uid=(2000 if uid_changed else 1000), gid=1000, mode=12345)
    backup_store.manifest.get_entry.return_value = entry
    backup_store._write_copy = mock.Mock()
    backup_store._write_diff = mock.Mock()
    with mock.patch('backuppy.stores.backup_store.compute_sha', return_value='abcdef123'):
        backup_store.save_if_new('/foo', dry_run)
    assert backup_store._write_copy.call_count == 0
    assert backup_store._write_diff.call_count == 0
    assert backup_store.manifest.insert_or_update.call_count == int(uid_changed and not dry_run)


@pytest.mark.parametrize('dry_run', [True, False])
def test_save_if_new_skip_diff(backup_store, dry_run):
    backup_store._write_copy = mock.Mock()
    backup_store._write_diff = mock.Mock()
    with mock.patch('backuppy.stores.backup_store.compute_sha', return_value='321fedcba'), \
            staticconf.testing.PatchConfiguration(
                {'options': [{'skip_diff_patterns': ['.*oo']}]},
                namespace='fake_backup1',
    ):
        backup_store.save_if_new('/foo', dry_run)
    assert backup_store._write_copy.call_count == 1
    assert backup_store._write_diff.call_count == 0
    assert backup_store.manifest.insert_or_update.call_count == int(not dry_run)


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


@pytest.mark.parametrize('max_manifest_versions', [None, 2])
def test_rotate_manifests(backup_store, max_manifest_versions):
    backup_store._query.return_value = ['manifest.1234', 'manifest.1235', 'manifest.1236']
    with staticconf.testing.PatchConfiguration(
        {'options': [{'max_manifest_versions': max_manifest_versions}]},
        namespace='fake_backup1',
    ):
        backup_store.rotate_manifests()
    if not max_manifest_versions:
        assert backup_store._delete.call_count == 0
    else:
        assert backup_store._delete.call_args_list == [
            mock.call('manifest.1234'),
            mock.call('manifest-key.1234'),
        ]


@pytest.mark.parametrize('dry_run', [True, False])
def test_write_copy(backup_store, dry_run, caplog):
    with mock.patch('backuppy.stores.backup_store.generate_key_pair', return_value=b'11111'), \
            mock.patch('backuppy.stores.backup_store.io_copy', return_value='12345678'):
        entry = backup_store._write_copy('/foo', mock.MagicMock(), dry_run)
    assert entry.sha == '12345678'
    # no signature computed in dry-run mode
    assert entry.key_pair == b'111112222' if not dry_run else b'11111'
    assert backup_store.save.call_count == int(not dry_run)
    assert 'Saving a new copy of /foo' in caplog.text


@pytest.mark.parametrize('base_sha', [None, '321fedcba'])
@pytest.mark.parametrize('dry_run', [True, False])
def test_write_diff(backup_store, current_entry, base_sha, dry_run, caplog):
    current_entry.base_sha = base_sha
    if base_sha:
        current_entry.base_key_pair = b'bbbbb3333'
    with mock.patch('backuppy.stores.backup_store.generate_key_pair', return_value=b'11111'), \
            mock.patch('backuppy.stores.backup_store.compute_sha_and_diff') as mock_sha_diff:
        mock_sha_diff.return_value = ('12345678', mock.Mock())
        entry = backup_store._write_diff('/foo', current_entry, mock.MagicMock(), dry_run)
    assert entry.sha == '12345678'
    assert entry.base_sha == ('321fedcba' if base_sha else 'abcdef123')
    # no signature computed in dry-run mode
    assert entry.key_pair == b'111112222' if not dry_run else b'11111'
    assert entry.base_key_pair == (b'bbbbb3333' if base_sha else b'aaaaa2222')
    assert backup_store.save.call_count == int(not dry_run)
    assert 'Saving a diff for /foo' in caplog.text


@pytest.mark.parametrize('dry_run', [True, False])
def test_write_diff_too_big(backup_store, current_entry, dry_run, caplog):
    with mock.patch('backuppy.stores.backup_store.generate_key_pair', return_value=b'11111'), \
            mock.patch('backuppy.stores.backup_store.compute_sha_and_diff') as mock_sha_diff, \
            mock.patch('backuppy.stores.backup_store.io_copy', return_value='12345678'):
        mock_sha_diff.side_effect = DiffTooLargeException
        entry = backup_store._write_diff('/foo', current_entry, mock.MagicMock(), dry_run)
    assert entry.sha == '12345678'
    assert entry.base_sha is None
    # no signature computed in dry-run mode
    assert entry.key_pair == b'111112222' if not dry_run else b'11111'
    assert entry.base_key_pair is None
    assert backup_store.save.call_count == int(not dry_run)
    assert 'Saving a new copy of /foo' in caplog.text
