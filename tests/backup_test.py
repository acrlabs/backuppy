import os
import re

import mock
import pytest
import staticconf.testing

from backuppy.backup import _backup_data_stream
from backuppy.backup import backup
from backuppy.manifest import ManifestEntry
from tests.conftest import INITIAL_FILES


################################################
# Tests and fixtures for _backup_data_stream() #
################################################

@pytest.fixture
def backup_data_stream_patches():
    with mock.patch('backuppy.backup.sha256') as sha256, \
            mock.patch('backuppy.backup.NamedTemporaryFile') as NamedTemporaryFile, \
            mock.patch('backuppy.backup.compress_and_encrypt') as compress_and_encrypt:
        NamedTemporaryFile.return_value.__enter__.return_value.name = 'tmp12345678.tbkpy'
        yield sha256, NamedTemporaryFile, compress_and_encrypt


@pytest.mark.parametrize('store_removes_file', [True, False])
def test_backup_data_stream(backup_data_stream_patches, store_removes_file):
    sha256, _, _ = backup_data_stream_patches
    sha256.return_value.hexdigest.return_value = 'abcdef12345678'
    backup_store = mock.Mock()
    save_path = ('ab', 'cd', 'ef12345678')
    if not store_removes_file:
        with open('tmp12345678.tbkpy', 'w') as f:
            f.write('data')

    _backup_data_stream(['asdf', 'hjkl'], backup_store)

    assert sha256.return_value.update.call_args_list == [mock.call('asdf'), mock.call('hjkl')]
    assert backup_store.write.call_args == mock.call(save_path, 'tmp12345678.tbkpy')
    assert os.path.isfile('tmp12345678.tbkpy') is False


def test_backup_data_stream_error(backup_data_stream_patches):
    _, NamedTemporaryFile, _ = backup_data_stream_patches
    NamedTemporaryFile.return_value.__enter__.return_value.write.side_effect = Exception('error')
    with open('tmp12345678.tbkpy', 'w') as f:
        f.write('data')
    backup_store = mock.Mock()

    with pytest.raises(Exception):
        _backup_data_stream(['asdf', 'hjkl'], backup_store)

    assert backup_store.write.call_count == 0
    assert os.path.isfile('tmp12345678.tbkpy') is False


###################################
# Tests and fixtures for backup() #
###################################

@pytest.fixture
def config():
    with staticconf.testing.PatchConfiguration({
        'directories': ['/a', '/b'],
        'location': ['/backup'],
        'protocol': 'fake'
    }, namespace='my_backup'):
        yield


@pytest.fixture
def backup_patches(config):
    mock_manifest = mock.Mock()
    mock_manifest.tracked_files.return_value = set()
    mock_manifest.is_current.return_value = False
    with mock.patch('backuppy.backup._backup_data_stream') as backup_data_stream, \
            mock.patch('backuppy.backup.file_contents_stream') as file_contents_stream, \
            mock.patch('backuppy.backup._get_manifest') as mock_get_manifest, \
            mock.patch('backuppy.backup.logger') as logger:
        mock_get_manifest.return_value = mock_manifest
        yield mock_manifest, backup_data_stream, file_contents_stream, logger


def test_all_specified_files(backup_patches):
    mock_manifest, mock_backup_data_stream, mock_file_contents_stream, mock_logger = backup_patches
    backup('my_backup', mock.Mock())

    assert mock_backup_data_stream.call_count == 4
    assert mock_file_contents_stream.call_args_list == [mock.call(name) for name in INITIAL_FILES]
    assert mock_manifest.insert_or_update.call_args_list == [
        mock.call(name, ManifestEntry(name, mock_backup_data_stream.return_value)) for name in INITIAL_FILES
    ]
    assert mock_logger.info.call_count == 4
    assert mock_logger.exception.call_count == 0
    assert mock_manifest.delete.call_count == 0


def test_with_local_exclusions(backup_patches):
    mock_manifest, mock_backup_data_stream, mock_file_contents_stream, mock_logger = backup_patches
    with staticconf.testing.PatchConfiguration({'exclusions': ['dummy']}, namespace='my_backup'):
        backup('my_backup', mock.Mock())

    assert mock_backup_data_stream.call_count == 2
    assert mock_manifest.insert_or_update.call_args_list == [
        mock.call(INITIAL_FILES[2], ManifestEntry(INITIAL_FILES[2], mock_backup_data_stream.return_value))
    ]
    assert mock_logger.info.call_count == 4
    assert mock_logger.exception.call_count == 0
    assert mock_manifest.delete.call_count == 0


def test_with_global_exclusions(backup_patches):
    mock_manifest, mock_backup_data_stream, mock_file_contents_stream, mock_logger = backup_patches
    backup('my_backup', mock.Mock(), [re.compile('file')])

    assert mock_backup_data_stream.call_count == 0
    assert mock_manifest.insert_or_update.call_count == 0
    assert mock_logger.info.call_count == 3
    assert mock_logger.exception.call_count == 0
    assert mock_manifest.delete.call_count == 0


def test_deleted_file(backup_patches):
    mock_manifest, mock_backup_data_stream, mock_file_contents_stream, mock_logger = backup_patches
    mock_manifest.tracked_files.return_value = set(INITIAL_FILES + ['/some/other/file'])
    backup('my_backup', mock.Mock())

    assert mock_backup_data_stream.call_count == 4
    assert mock_manifest.insert_or_update.call_args_list == [
        mock.call(name, ManifestEntry(name, mock_backup_data_stream.return_value)) for name in INITIAL_FILES
    ]
    assert mock_logger.info.call_count == 5
    assert mock_logger.exception.call_count == 0
    assert mock_manifest.delete.call_args_list == [mock.call('/some/other/file')]


def test_backup_failed(backup_patches):
    mock_manifest, mock_backup_data_stream, mock_file_contents_stream, mock_logger = backup_patches
    mock_manifest.tracked_files.return_value = set(INITIAL_FILES)
    mock_backup_data_stream.side_effect = OSError
    backup('my_backup', mock.Mock())

    assert mock_backup_data_stream.call_count == 3
    assert mock_manifest.insert_or_update.call_count == 0
    assert mock_logger.info.call_count == 0
    assert mock_logger.exception.call_count == 3
    assert mock_logger.warning.call_count == 3
    assert mock_manifest.delete.call_count == 0


def test_all_up_to_date(backup_patches):
    mock_manifest, mock_backup_data_stream, mock_file_contents_stream, mock_logger = backup_patches
    mock_manifest.tracked_files.return_value = set(INITIAL_FILES)
    mock_manifest.is_current.return_value = True
    backup('my_backup', mock.Mock())

    assert mock_backup_data_stream.call_count == 0
    assert mock_manifest.insert_or_update.call_count == 0
    assert mock_logger.info.call_count == 3
    assert mock_logger.exception.call_count == 0
    assert mock_manifest.delete.call_count == 0
