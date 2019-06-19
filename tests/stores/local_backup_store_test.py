import os

import mock
import pytest
import staticconf.testing

from backuppy.io import IOIter
from backuppy.stores.local_backup_store import LocalBackupStore
from tests.conftest import count_matching_log_lines


@pytest.fixture
def mock_backup_store():
    backup_name = 'fake_backup'
    with mock.patch('backuppy.stores.local_backup_store.BackupStore'), \
            staticconf.testing.PatchConfiguration(
                {'protocol': {'location': '/fake/path'}},
                namespace=backup_name
    ):
        yield LocalBackupStore(backup_name)


@pytest.fixture(autouse=True)
def fake_filesystem(fs):
    fs.create_file('/scratch/foo', contents="i'm a copy of foo")
    fs.create_file('/scratch/asdf/bar', contents="i'm a copy of bar")
    fs.create_file('/fake/path/foo', contents='old boring content')


def fake_output_func(content, tmp, loc, key, iv):
    with open(loc.filename, 'w') as f:
        f.write(content)


@pytest.mark.parametrize('overwrite', [True, False])
def test_save(caplog, mock_backup_store, overwrite):
    mock_backup_store._save('/scratch/foo', '/foo', overwrite)
    mock_backup_store._save('/scratch/asdf/bar', '/asdf/bar', overwrite)
    assert os.path.exists('/fake/path/foo')
    with open('/fake/path/foo', 'r') as f:
        assert f.read() == (
            "i'm a copy of foo"
            if overwrite
            else 'old boring content'
        )
    assert os.path.exists('/fake/path/asdf/bar')
    with open('/fake/path/asdf/bar', 'r') as f:
        assert f.read() == "i'm a copy of bar"
    if not overwrite:
        assert count_matching_log_lines('/fake/path/foo already exists', caplog) == 1


def test_load(mock_backup_store):
    with IOIter('/restored_file') as output:
        mock_backup_store._load('/foo', output)
    with open('/restored_file') as f:
        assert f.read() == 'old boring content'
