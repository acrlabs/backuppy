import mock
import pytest
import staticconf.testing

from backuppy.stores.local_backup_store import LocalBackupStore


@pytest.fixture
def patches():
    with staticconf.testing.PatchConfiguration({'location': '/fake_backup'}, namespace='fake_backup'), \
            mock.patch('backuppy.stores.local_backup_store.os'), \
            mock.patch('backuppy.stores.local_backup_store.compress_and_encrypt') as mock_compress, \
            mock.patch('backuppy.stores.local_backup_store.decrypt_and_unpack') as mock_decrypt, \
            mock.patch('builtins.open'):
        yield mock_compress, mock_decrypt, LocalBackupStore('fake_backup')


def test_save(patches):
    mock_compress, mock_decrypt, local_backup_store = patches
    local_backup_store.save('asdf', mock.Mock())
    assert mock_compress.call_count == 1
    assert mock_decrypt.call_count == 0


def test_load(patches):
    mock_compress, mock_decrypt, local_backup_store = patches
    local_backup_store.load('asdf', mock.Mock())
    assert mock_compress.call_count == 0
    assert mock_decrypt.call_count == 1
