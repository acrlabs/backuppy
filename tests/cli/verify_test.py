import argparse
from contextlib import ExitStack

import mock
import pytest

from backuppy.cli.verify import _verify
from backuppy.cli.verify import main
from backuppy.manifest import ManifestEntry


@pytest.fixture
def mock_manifest_entry_list():
    return [ManifestEntry(
        '/path/0/foo/bar',
        'abcd1234',
        None,
        1000,
        1000,
        35677,
        b'1111',
        None,
    )]


def test_verify_ok(mock_manifest_entry_list, capsys):
    with mock.patch('backuppy.cli.verify.compute_sha', return_value='abcd1234'):
        _verify(mock_manifest_entry_list, mock.Mock(), True)
    out, _ = capsys.readouterr()
    assert 'Checking /path/0/foo/bar... OK!' in out


def test_verify_bad_sha(mock_manifest_entry_list):
    backup_store = mock.Mock()
    with mock.patch('backuppy.cli.verify.ask_for_confirmation', return_value=True):
        _verify(mock_manifest_entry_list, backup_store, True)
    assert backup_store.save_if_new.call_args == mock.call('/path/0/foo/bar', force_copy=True)


@pytest.mark.parametrize('sha,entries', [
    (None, None),
    ('abcd1234', []),
    ('abcd1234', [mock.Mock()]),
])
def test_main(sha, entries):
    backup_store = mock.MagicMock(
        manifest=mock.Mock(
            get_entries_by_sha=mock.Mock(return_value=entries),
            search=mock.Mock(return_value=[('/foo', [mock.Mock()]), ('/bar', [mock.Mock()])])
        ),
    )
    with mock.patch('backuppy.cli.verify._verify') as mock_verify, \
            mock.patch('backuppy.cli.verify.get_backup_store', return_value=backup_store), \
            mock.patch('backuppy.cli.verify.staticconf'), \
            (pytest.raises(ValueError) if (sha and not entries) else ExitStack()):
        main(argparse.Namespace(
            config='backuppy.conf',
            like='',
            name='fake_backup1',
            sha=sha,
            preserve_scratch_dir=False,
            yes=False,
            show_all=True,
        ))
        assert mock_verify.call_count == 1
        if entries:
            assert mock_verify.call_args_list == [mock.call(
                entries,
                backup_store,
                True,
            )]
