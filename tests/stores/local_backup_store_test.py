import os

import mock
import pytest
import staticconf.testing

from backuppy.io import IOIter
from backuppy.stores.local_backup_store import LocalBackupStore


@pytest.fixture
def mock_backup_store():
    backup_name = 'fake_backup'
    with mock.patch('backuppy.stores.local_backup_store.BackupStore'), \
            staticconf.testing.PatchConfiguration(
                {'protocol': {'location': '/fake/path'}},
                namespace=backup_name,
    ):
        yield LocalBackupStore(backup_name)


def fake_output_func(content, tmp, loc, key, iv):
    with open(loc.filename, 'w') as f:
        f.write(content)


def test_save(caplog, mock_backup_store):
    with IOIter('/scratch/foo') as input1, IOIter('/scratch/asdf/bar') as input2:
        mock_backup_store._save(input1, '/foo')
        mock_backup_store._save(input2, '/asdf/bar')
    assert os.path.exists('/fake/path/fake_backup/foo')
    with open('/fake/path/fake_backup/foo', 'r') as f:
        assert f.read() == "i'm a copy of foo"
    assert os.path.exists('/fake/path/fake_backup/asdf/bar')
    with open('/fake/path/fake_backup/asdf/bar', 'r') as f:
        assert f.read() == "i'm a copy of bar"


def test_load(mock_backup_store):
    with IOIter('/restored_file') as output:
        mock_backup_store._load('/foo', output)
    with open('/restored_file') as f:
        assert f.read() == 'old boring content'


def test_query(mock_backup_store):
    assert set(mock_backup_store._query('')) == {'/biz/baz', '/foo', '/fuzz/buzz'}


def test_query_2(mock_backup_store):
    assert set(mock_backup_store._query('f')) == {'/foo', '/fuzz/buzz'}


def test_query_no_results(mock_backup_store):
    assert mock_backup_store._query('not_here') == []


def test_delete(mock_backup_store):
    mock_backup_store._delete('/biz/baz')
    assert not os.path.exists('/fake/path/fake_backup/biz/baz')
