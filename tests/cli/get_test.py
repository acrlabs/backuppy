import argparse

import mock
import pytest

from backuppy.cli.get import _get
from backuppy.cli.get import ACTIONS
from backuppy.cli.get import main
from backuppy.manifest import MANIFEST_PREFIX
from backuppy.options import DEFAULT_OPTIONS


@pytest.fixture
def args():
    return argparse.Namespace(
        action='fetch',
        config='backup.conf',
        manifest=None,
        name='fake_backup1',
        sha='abcdef123',
    )


@pytest.mark.parametrize('action', ACTIONS)
@pytest.mark.parametrize('filename', ['abcdef123', MANIFEST_PREFIX + '1234.123'])
def test_get(action, filename):
    backup_store = mock.Mock(options=DEFAULT_OPTIONS)
    with mock.patch('backuppy.cli.get.IOIter'), \
            mock.patch('backuppy.cli.get.decrypt_and_unpack') as mock_decrypt:
        _get(filename, b'asdfasdf', backup_store, action)

        expected_options = dict(DEFAULT_OPTIONS)
        expected_options['use_encryption'] = (action in {'decrypt', 'unpack'})
        expected_options['use_compression'] = (action in {'unpack'})
        assert backup_store._load.call_args[0][0] == (
            filename if filename.startswith(MANIFEST_PREFIX)
            else 'ab/cd/ef123'
        )
        assert mock_decrypt.call_args[0][3] == expected_options


def test_main_two_args():
    args = argparse.Namespace(
        sha='abcdef123',
        manifest=0,
    )
    with pytest.raises(ValueError):
        main(args)


def test_main_get_sha(args):
    backup_store = mock.MagicMock(manifest=mock.Mock(
        get_entries_by_sha=mock.Mock(return_value=[mock.Mock(sha='abcdef123', key_pair=b'1234')]),
    ))
    with mock.patch('backuppy.cli.get.get_backup_store', return_value=backup_store), \
            mock.patch('backuppy.cli.get._get') as mock_get:
        main(args)
        assert mock_get.call_args == mock.call('abcdef123', b'1234', backup_store, 'fetch')


def test_main_no_entries(args):
    backup_store = mock.MagicMock(manifest=mock.Mock(get_entries_by_sha=mock.Mock(return_value=[])))
    with mock.patch('backuppy.cli.get.get_backup_store', return_value=backup_store), \
            mock.patch('backuppy.cli.get._get'), \
            pytest.raises(ValueError):
        main(args)


def test_main_get_manifest(args):
    args.sha = None
    args.manifest = 2
    backup_store = mock.MagicMock(_query=mock.Mock(return_value=[
        '/' + MANIFEST_PREFIX + '123',
        '/' + MANIFEST_PREFIX + '456',
        '/' + MANIFEST_PREFIX + '789',
        '/' + MANIFEST_PREFIX + '999',
    ]))
    with mock.patch('backuppy.cli.get.get_backup_store', return_value=backup_store), \
            mock.patch('backuppy.cli.get.get_manifest_keypair'), \
            mock.patch('backuppy.cli.get._get') as mock_get:
        main(args)
        assert mock_get.call_args[0][0] == MANIFEST_PREFIX + '456'
