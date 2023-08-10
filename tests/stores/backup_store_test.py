import os
import signal

import mock
import pytest
import staticconf.testing

from backuppy.exceptions import DiffTooLargeException
from backuppy.exceptions import ManifestLockedException
from backuppy.manifest import Manifest
from backuppy.manifest import ManifestEntry
from backuppy.stores.backup_store import _cleanup_and_exit
from backuppy.stores.backup_store import _register_unlocked_store
from backuppy.stores.backup_store import _SIGNALS_TO_HANDLE
from backuppy.stores.backup_store import _unregister_store
from backuppy.stores.backup_store import BackupStore
from backuppy.util import get_scratch_dir


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
def preexisting_entry():
    return ManifestEntry(
        '/some/other/file',
        '12345678',
        '123123',
        1000,
        1000,
        55555,
        b'lkjhasdf',
        b'12341234',
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
        store._manifest = mock.Mock(
            get_entries_by_sha=mock.Mock(return_value=[]),
            spec=Manifest,
        )
        yield store


def test_init(backup_store):
    assert backup_store.backup_name == 'fake_backup1'


def test_unlock_no_private_key(backup_store):
    backup_store.do_cleanup = mock.Mock()
    with pytest.raises(FileNotFoundError), backup_store.unlock():
        pass

    assert backup_store.do_cleanup.call_args == mock.call(False, False)


@pytest.mark.parametrize('manifest_exists', [True, False])
def test_unlock(fs, backup_store, manifest_exists):
    fs.create_file('/my/private/key', contents='THIS IS VERY SECRET')
    os.makedirs(get_scratch_dir())
    with mock.patch('backuppy.stores.backup_store.Manifest') as mock_manifest, \
            mock.patch('backuppy.stores.backup_store.unlock_manifest') as mock_unlock_manifest, \
            mock.patch('backuppy.stores.backup_store.rmtree') as mock_remove, \
            mock.patch('backuppy.stores.backup_store._register_unlocked_store') as mock_register, \
            mock.patch('backuppy.stores.backup_store._unregister_store') as mock_unregister:
        backup_store.do_cleanup = mock.Mock()
        mock_unlock_manifest.return_value = mock_manifest.return_value
        if manifest_exists:
            backup_store._query.return_value = ['manifest.1234123']
        with backup_store.unlock():
            pass
        assert mock_unlock_manifest.call_count == manifest_exists
        assert mock_remove.call_count == 1
        assert backup_store.do_cleanup.call_args == mock.call(False, False)
        assert mock_register.call_count == 1
        assert mock_unregister.call_count == 1


def test_open_locked_manifest(backup_store):
    backup_store._manifest = None
    with pytest.raises(ManifestLockedException):
        backup_store.manifest


@pytest.mark.parametrize('dry_run', [True, False])
class TestSaveIfNew:
    @pytest.fixture(autouse=True)
    def setup_store(self, backup_store):
        backup_store._write_copy = mock.Mock()
        backup_store._write_diff = mock.Mock()

    def test_force_save_if_new(self, backup_store, dry_run):
        backup_store.manifest.get_entry.return_value = None
        with mock.patch('backuppy.stores.backup_store.compute_sha', return_value=None):
            backup_store.save_if_new('/foo', force_copy=True, dry_run=dry_run)
        assert backup_store._write_copy.call_count == 1
        assert backup_store._write_diff.call_count == 0
        assert backup_store.manifest.insert_or_update.call_count == int(not dry_run)

    def test_save_if_new_with_new_file(self, backup_store, dry_run):
        backup_store.manifest.get_entry.return_value = None
        with mock.patch('backuppy.stores.backup_store.compute_sha', return_value=None):
            backup_store.save_if_new('/foo', dry_run=dry_run)
        assert backup_store._write_copy.call_count == 1
        assert backup_store._write_diff.call_count == 0
        assert backup_store.manifest.insert_or_update.call_count == int(not dry_run)

    def test_save_if_new_sha_different(self, backup_store, dry_run):
        backup_store.manifest.get_entry.return_value = mock.Mock(sha='abcdef123')
        with mock.patch('backuppy.stores.backup_store.compute_sha', return_value='321fedcba'):
            backup_store.save_if_new('/foo', dry_run=dry_run)
        assert backup_store._write_copy.call_count == 0
        assert backup_store._write_diff.call_count == 1
        assert backup_store.manifest.insert_or_update.call_count == int(not dry_run)

    @pytest.mark.parametrize('uid_changed', [True, False])
    def test_save_if_new_sha_equal(self, backup_store, uid_changed, dry_run):
        entry = mock.Mock(sha='abcdef123', uid=(2000 if uid_changed else 1000), gid=1000, mode=12345)
        backup_store.manifest.get_entry.return_value = entry
        with mock.patch('backuppy.stores.backup_store.compute_sha', return_value='abcdef123'):
            backup_store.save_if_new('/foo', dry_run=dry_run)
        assert backup_store._write_copy.call_count == 0
        assert backup_store._write_diff.call_count == 0
        assert backup_store.manifest.insert_or_update.call_count == int(uid_changed and not dry_run)

    def test_save_if_new_skip_diff(self, backup_store, dry_run):
        with mock.patch('backuppy.stores.backup_store.compute_sha', return_value='321fedcba'), \
                staticconf.testing.PatchConfiguration(
                    {'options': [{'skip_diff_patterns': ['.*oo']}]},
                    namespace='fake_backup1',
        ):
            backup_store.save_if_new('/foo', dry_run=dry_run)
        assert backup_store._write_copy.call_count == 1
        assert backup_store._write_diff.call_count == 0
        assert backup_store.manifest.insert_or_update.call_count == int(not dry_run)


@pytest.mark.parametrize('base_sha', [None, 'ffffffff'])
def test_restore_entry(backup_store, base_sha, current_entry):
    current_entry.base_sha = base_sha
    if base_sha:
        current_entry.base_key_pair = b'2222'
        orig_file, diff_file, restore_file = mock.MagicMock(), mock.MagicMock(), mock.MagicMock()
        backup_store.restore_entry(current_entry, orig_file, diff_file, restore_file)
        if base_sha:
            assert backup_store.load.call_args_list[0] == mock.call(base_sha, orig_file, b'2222')
        assert backup_store.load.call_args_list[-1] == mock.call(
            'abcdef123',
            diff_file,
            b'aaaaa2222',
        )


@pytest.mark.no_mocksaveload
def test_save(backup_store):
    expected_path = '/tmp/backuppy/12/34/5678'
    with mock.patch('backuppy.stores.backup_store.IOIter') as mock_io_iter, \
            mock.patch('backuppy.stores.backup_store.compress_and_encrypt') as mock_compress, \
            mock.patch('backuppy.stores.backup_store.os.remove') as mock_remove:
        backup_store.save(mock.Mock(), '12345678', b'1111')
        src = mock_io_iter.return_value.__enter__.return_value
        assert mock_compress.call_count == 1
        assert mock_io_iter.call_args[0][0] == expected_path
        assert backup_store._save.call_args == mock.call(src, '12/34/5678')
        assert mock_remove.call_count == 1


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
@pytest.mark.parametrize('preserve_scratch', [True, False])
@pytest.mark.parametrize('manifest', [None, mock.Mock(changed=True), mock.Mock(changed=False)])
def test_do_cleanup(fs, backup_store, manifest, dry_run, preserve_scratch):
    with mock.patch('backuppy.stores.backup_store.rmtree') as mock_remove, \
            mock.patch('backuppy.stores.backup_store.lock_manifest') as mock_lock:
        backup_store._manifest = manifest
        backup_store.rotate_manifests = mock.Mock()
        backup_store.do_cleanup(
            dry_run=dry_run,
            preserve_scratch=preserve_scratch
        )
        assert mock_lock.call_count == int(bool(manifest and manifest.changed and not dry_run))
        assert backup_store.rotate_manifests.call_count == int(bool(
            manifest and manifest.changed and not dry_run
        ))
        assert mock_remove.call_count == int(bool(manifest and not preserve_scratch))
        assert backup_store._manifest is None


@pytest.mark.parametrize('dry_run', [True, False])
def test_write_copy(backup_store, dry_run, caplog):
    with mock.patch('backuppy.stores.backup_store.generate_key_pair', return_value=b'11111'):
        entry = backup_store._write_copy('/foo', '12345678', mock.MagicMock(), False, dry_run)
    assert entry.sha == '12345678'
    # no signature computed in dry-run mode
    assert entry.key_pair == b'111112222' if not dry_run else b'11111'
    assert backup_store.save.call_count == int(not dry_run)
    assert 'Saving a new copy of /foo' in caplog.text


@pytest.mark.parametrize('force_copy', [True, False])
def test_write_copy_preexisting_sha(backup_store, force_copy, preexisting_entry):
    backup_store.manifest.get_entries_by_sha.return_value = [preexisting_entry]
    entry = backup_store._write_copy(
        '/foo',
        preexisting_entry.sha,
        mock.MagicMock(),
        force_copy,
        False,
    )
    assert entry.sha == preexisting_entry.sha
    if not force_copy:
        assert entry.key_pair == preexisting_entry.key_pair
    assert entry.base_sha == (preexisting_entry.base_sha if not force_copy else None)
    assert entry.base_key_pair == (preexisting_entry.base_key_pair if not force_copy else None)
    assert backup_store.save.call_count == int(force_copy)


@pytest.mark.parametrize('base_sha', [None, '321fedcba'])
@pytest.mark.parametrize('dry_run', [True, False])
def test_write_diff(backup_store, current_entry, base_sha, dry_run, caplog):
    current_entry.base_sha = base_sha
    if base_sha:
        current_entry.base_key_pair = b'bbbbb3333'
    with mock.patch('backuppy.stores.backup_store.generate_key_pair', return_value=b'11111'), \
            mock.patch('backuppy.stores.backup_store.compute_diff') as mock_compute_diff:
        mock_compute_diff.return_value = ('12345678', mock.Mock())
        entry = backup_store._write_diff(
            '/foo',
            '12345678',
            current_entry,
            mock.MagicMock(),
            dry_run,
        )
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
            mock.patch('backuppy.stores.backup_store.compute_diff') as mock_compute_diff:
        mock_compute_diff.side_effect = DiffTooLargeException
        entry = backup_store._write_diff(
            '/foo',
            '12345678',
            current_entry,
            mock.MagicMock(),
            dry_run,
        )
    assert entry.sha == '12345678'
    assert entry.base_sha is None
    # no signature computed in dry-run mode
    assert entry.key_pair == b'111112222' if not dry_run else b'11111'
    assert entry.base_key_pair is None
    assert backup_store.save.call_count == int(not dry_run)
    assert 'Saving a new copy of /foo' in caplog.text


def test_write_diff_preexisting_sha(backup_store, current_entry, preexisting_entry):
    backup_store.manifest.get_entries_by_sha.return_value = [preexisting_entry]
    with mock.patch('backuppy.stores.backup_store.compute_diff') as mock_compute_diff:
        entry = backup_store._write_diff(
            '/foo',
            preexisting_entry.sha,
            current_entry,
            mock.MagicMock(),
            False
        )
    assert entry.sha == preexisting_entry.sha
    assert entry.key_pair == preexisting_entry.key_pair
    assert entry.base_sha == preexisting_entry.base_sha
    assert entry.base_key_pair == preexisting_entry.base_key_pair
    assert backup_store.save.call_count == 0
    assert mock_compute_diff.call_count == 0


def test_cleanup_and_exit_no_store(backup_store):
    backup_store.do_cleanup = mock.Mock()
    with mock.patch('backuppy.stores.backup_store.signal.signal') as mock_signal, \
            pytest.raises(SystemExit):
        _cleanup_and_exit(signal.SIGINT, mock.Mock(), True, True)

    assert mock_signal.call_args_list == [
        mock.call(signal.SIGINT, signal.SIG_IGN)
    ]
    assert backup_store.do_cleanup.call_count == 0


@pytest.mark.parametrize('side_effect', [None, Exception])
def test_cleanup_and_exit(backup_store, side_effect):
    backup_store.do_cleanup = mock.Mock(side_effect=side_effect)
    with mock.patch('backuppy.stores.backup_store._UNLOCKED_STORE', backup_store), \
            mock.patch('backuppy.stores.backup_store.signal.signal') as mock_signal, \
            pytest.raises(SystemExit):
        _cleanup_and_exit(signal.SIGINT, mock.Mock(), True, True)

    assert mock_signal.call_args_list == [
        mock.call(signal.SIGINT, signal.SIG_IGN)
    ]
    assert backup_store.do_cleanup.call_count == 1


def test_register_unlocked_store(backup_store):
    with mock.patch('backuppy.stores.backup_store._UNLOCKED_STORE', backup_store) as store, \
            mock.patch('backuppy.stores.backup_store.signal.signal') as mock_signal:
        _register_unlocked_store(backup_store, True, True)
    assert store == backup_store
    assert mock_signal.call_args_list == [
        mock.call(sig, mock.ANY)
        for sig in _SIGNALS_TO_HANDLE
    ]


def test_unregister_store():
    with mock.patch('backuppy.stores.backup_store._UNLOCKED_STORE'), \
            mock.patch('backuppy.stores.backup_store.signal.signal') as mock_signal:
        _unregister_store()
    assert mock_signal.call_args_list == [
        mock.call(sig, signal.SIG_DFL)
        for sig in _SIGNALS_TO_HANDLE
    ]
