import re
from hashlib import sha256

import mock
import pytest
import staticconf.testing

from backuppy.backup import _save_copy
from backuppy.backup import _save_diff
from backuppy.backup import _scan_directory
from backuppy.backup import backup
from backuppy.manifest import Manifest
from backuppy.stores.backup_store import MANIFEST_PATH


@pytest.fixture
def mock_open():
    with mock.patch('builtins.open') as m:
        yield m.return_value.__enter__.return_value


@pytest.fixture
def mock_tf():
    with mock.patch('backuppy.backup.TemporaryFile') as m:
        yield m.return_value.__enter__.return_value


@mock.patch('backuppy.backup.file_walker')
def test_scan_directory(file_walker):
    file_walker.return_value = ['/file1', '/file2', '/file3', '/skip']
    manifest = mock.MagicMock(spec=Manifest)
    manifest.is_current = lambda f: f == '/file2'

    modified, marked = _scan_directory('/', manifest, [re.compile('skip')])
    assert modified == set(['/file1', '/file3'])
    assert marked == set(['/file1', '/file2', '/file3'])


def test_save_copy(mock_open, mock_tf):
    mock_data = [b'asdfh', b'jklqw', b'ertyu', b'iop']
    sha_fn = sha256()
    sha_fn.update(b''.join(mock_data))
    backup_store = mock.Mock()

    with mock.patch('backuppy.backup.ManifestEntry') as mock_entry:
        mock_open.read.side_effect = mock_data
        _save_copy('/file', backup_store)

        assert mock_tf.write.call_args_list == [mock.call(s) for s in mock_data]
        assert mock_entry.call_args == mock.call('/file', sha_fn.hexdigest())


def test_save_copy_crash(mock_open, mock_tf):
    mock_open.read.side_effect = [b'asdfh', b'jklqw', b'ertyu', b'iop']
    mock_tf.write.side_effect = [None, Exception]
    backup_store = mock.Mock()
    with pytest.raises(Exception):
        _save_copy('/file', backup_store)

    assert backup_store.save.call_count == 0
    assert backup_store.manifest.insert_or_update.call_count == 0


@pytest.mark.parametrize('base,latest', [('1234', '1234'), ('abcd', '5678')])
def test_save_diff(mock_open, mock_tf, base, latest):
    backup_store = mock.Mock()
    with mock.patch('backuppy.backup.compute_diff') as mock_diff, \
            mock.patch('backuppy.backup.ManifestEntry') as mock_entry:
        mock_diff.return_value = '1234'
        mock_entry.return_value.sha = '1234'
        _save_diff('/file', mock.Mock(sha=base), mock.Mock(sha=latest), backup_store)
        assert backup_store.save.call_count == int(latest == '5678')
        assert backup_store.manifest.insert_or_update.call_args == mock.call(
            '/file',
            mock_entry.return_value,
            is_diff=(latest == '5678'),
        )


def test_save_diff_crash(mock_open, mock_tf):
    backup_store = mock.Mock()
    with mock.patch('backuppy.backup.compute_diff') as mock_diff:
        mock_diff.side_effect = Exception
        with pytest.raises(Exception):
            _save_diff('/file', mock.Mock(), mock.Mock(), backup_store)

    assert backup_store.save.call_count == 0
    assert backup_store.manifest.insert_or_update.call_count == 0


def test_backup():
    backup_store = mock.Mock()
    backup_store.manifest.files.return_value = set(['/foo/file1', '/foo/file2', '/foo/file3', '/bar/file3'])
    with staticconf.testing.PatchConfiguration({'directories': ['/foo', '/bar']}, namespace='test_backup'), \
            mock.patch('backuppy.backup.compile_exclusions') as mock_compile, \
            mock.patch('backuppy.backup._scan_directory') as mock_scan, \
            mock.patch('backuppy.backup._save_copy') as mock_save_copy, \
            mock.patch('backuppy.backup._save_diff') as mock_save_diff, \
            mock.patch('backuppy.backup.logger') as mock_logger:
        mock_compile.return_value = []
        mock_scan.side_effect = [
            (set(['/foo/file1']), set(['/foo/file1', '/foo/file2', '/foo/file3'])),
            (set(['/bar/file1', '/bar/file2']), set(['/bar/file1', '/bar/file2'])),
        ]
        backup_store.manifest.get_diff_pair.side_effect = [
            ValueError('something bad happened'),
            (mock.Mock(), mock.Mock()),
            (None, None),
        ]

        backup('test_backup', backup_store)

        assert mock_scan.call_args_list == [
            mock.call('/foo', backup_store.manifest, []),
            mock.call('/bar', backup_store.manifest, []),
        ]
        assert sorted(backup_store.manifest.get_diff_pair.call_args_list) == sorted([
            mock.call('/foo/file1'),
            mock.call('/bar/file1'),
            mock.call('/bar/file2'),
        ])
        assert mock_save_copy.call_count == 1
        assert mock_save_diff.call_count == 1
        assert backup_store.manifest.delete.call_args_list == [mock.call('/bar/file3')]
        assert backup_store.save.call_args_list == [
            mock.call(MANIFEST_PATH, backup_store.manifest.stream.return_value),
        ]
        assert mock_logger.info.call_count == 4
        assert mock_logger.exception.call_count == 1


def test_backup_no_change():
    backup_store = mock.Mock()
    backup_store.manifest.files.return_value = set(['/foo/file1', '/foo/file2', '/foo/file3'])
    with staticconf.testing.PatchConfiguration({'directories': ['/foo']}, namespace='test_backup'), \
            mock.patch('backuppy.backup.compile_exclusions') as mock_compile, \
            mock.patch('backuppy.backup._scan_directory') as mock_scan, \
            mock.patch('backuppy.backup._save_copy') as mock_save_copy, \
            mock.patch('backuppy.backup._save_diff') as mock_save_diff, \
            mock.patch('backuppy.backup.logger') as mock_logger:
        mock_compile.return_value = []
        mock_scan.side_effect = [
            (set(), set(['/foo/file1', '/foo/file2', '/foo/file3'])),
        ]

        backup('test_backup', backup_store)

        assert mock_scan.call_args_list == [
            mock.call('/foo', backup_store.manifest, []),
        ]
        assert backup_store.manifest.get_diff_pair.call_count == 0
        assert mock_save_copy.call_count == 0
        assert mock_save_diff.call_count == 0
        assert backup_store.manifest.delete.call_count == 0
        assert backup_store.save.call_count == 0
        assert mock_logger.info.call_count == 0
        assert mock_logger.exception.call_count == 0
