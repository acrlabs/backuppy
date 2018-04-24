import re

import mock
import pytest

from backuppy.backup import backup
from backuppy.exceptions import BackupFailedError
from tests.conftest import INITIAL_FILES


@pytest.fixture
def config():
    return {'directories': ['/a', '/b']}


@pytest.fixture
def mock_manifest():
    mock_manifest = mock.Mock()
    mock_manifest.tracked_files.return_value = set()
    mock_manifest.is_current.return_value = False
    return mock_manifest


@pytest.fixture
def mock_backup_file():
    with mock.patch('backuppy.backup._backup_file', autospec=True) as m:
        yield m


@pytest.fixture
def mock_logger():
    with mock.patch('backuppy.backup.logger') as m:
        yield m


def test_all_specified_files(mock_backup_file, mock_logger, mock_manifest, config):
    backup(mock_manifest, '/backup', config)
    assert mock_backup_file.call_args_list == [mock.call(name, '/backup') for name in INITIAL_FILES]
    assert mock_manifest.insert_or_update.call_args_list == [
        mock.call(name, mock_backup_file.return_value) for name in INITIAL_FILES
    ]
    assert mock_logger.info.call_count == 3
    assert mock_logger.warn.call_count == 0
    assert mock_manifest.delete.call_count == 0


def test_with_local_exclusions(mock_backup_file, mock_logger, mock_manifest, config):
    config['exclusions'] = ['dummy']
    backup(mock_manifest, '/backup', config)
    assert mock_backup_file.call_args_list == [mock.call(INITIAL_FILES[2], '/backup')]
    assert mock_manifest.insert_or_update.call_args_list == [
        mock.call(INITIAL_FILES[2], mock_backup_file.return_value)
    ]
    assert mock_logger.info.call_count == 3
    assert mock_logger.warn.call_count == 0
    assert mock_manifest.delete.call_count == 0


def test_with_global_exclusions(mock_backup_file, mock_logger, mock_manifest, config):
    backup(mock_manifest, '/backup', config, [re.compile('file')])
    assert mock_backup_file.call_count == 0
    assert mock_manifest.insert_or_update.call_count == 0
    assert mock_logger.info.call_count == 3
    assert mock_logger.warn.call_count == 0
    assert mock_manifest.delete.call_count == 0


def test_deleted_file(mock_backup_file, mock_logger, mock_manifest, config):
    mock_manifest.tracked_files.return_value = set(INITIAL_FILES + ['/some/other/file'])
    backup(mock_manifest, '/backup', config)
    assert mock_backup_file.call_args_list == [mock.call(name, '/backup') for name in INITIAL_FILES]
    assert mock_manifest.insert_or_update.call_args_list == [
        mock.call(name, mock_backup_file.return_value) for name in INITIAL_FILES
    ]
    assert mock_logger.info.call_count == 4
    assert mock_logger.warn.call_count == 0
    assert mock_manifest.delete.call_args_list == [mock.call('/some/other/file')]


def test_backup_failed(mock_backup_file, mock_logger, mock_manifest, config):
    mock_manifest.tracked_files.return_value = set(INITIAL_FILES)
    mock_backup_file.side_effect = BackupFailedError
    backup(mock_manifest, '/backup', config)
    assert mock_backup_file.call_count == 3
    assert mock_manifest.insert_or_update.call_count == 0
    assert mock_logger.info.call_count == 0
    assert mock_logger.warn.call_count == 3
    assert mock_manifest.delete.call_count == 0


def test_all_up_to_date(mock_backup_file, mock_logger, mock_manifest, config):
    mock_manifest.tracked_files.return_value = set(INITIAL_FILES)
    mock_manifest.is_current.return_value = True
    backup(mock_manifest, '/backup', config)
    assert mock_backup_file.call_count == 0
    assert mock_manifest.insert_or_update.call_count == 0
    assert mock_logger.info.call_count == 3
    assert mock_logger.warn.call_count == 0
    assert mock_manifest.delete.call_count == 0
