import os
from functools import partial

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
            staticconf.testing.PatchConfiguration({'location': '/fake/path'}, namespace=backup_name):
        yield LocalBackupStore(backup_name)


@pytest.fixture(autouse=True)
def fake_filesystem(fs):
    fs.create_file('/fake/path/foo', contents='old boring content')


def fake_output_func(content, tmp, loc, key, iv):
    with open(loc.filename, 'w') as f:
        f.write(content)


@pytest.mark.parametrize('overwrite', [True, False])
def test_save(caplog, mock_backup_store, overwrite):
    with mock.patch('backuppy.stores.local_backup_store.compress_and_encrypt') as mock_compress:
        mock_compress.side_effect = partial(fake_output_func, 'xXx SECRET ENCRYPTED CONTENT xXx')
        mock_backup_store._save('/foo', mock.Mock(), overwrite)
        mock_backup_store._save('/asdf/bar', mock.Mock(), overwrite)
    assert os.path.exists('/fake/path/foo')
    with open('/fake/path/foo', 'r') as f:
        assert f.read() == ('xXx SECRET ENCRYPTED CONTENT xXx' if overwrite else 'old boring content')
    assert os.path.exists('/fake/path/asdf/bar')
    with open('/fake/path/asdf/bar', 'r') as f:
        assert f.read() == 'xXx SECRET ENCRYPTED CONTENT xXx'
    if not overwrite:
        assert count_matching_log_lines('/fake/path/foo already exists', caplog) == 1


def test_load(mock_backup_store):
    output = IOIter('/restored_file')
    with mock.patch('backuppy.stores.local_backup_store.decrypt_and_unpack') as mock_decrypt:
        mock_decrypt.side_effect = partial(fake_output_func, 'all my decrypted secrets')
        mock_backup_store._load('/foo', output)
    with open('/restored_file') as f:
        assert f.read() == 'all my decrypted secrets'
