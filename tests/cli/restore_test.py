import argparse
from contextlib import ExitStack

import mock
import pytest

from backuppy.cli.restore import _confirm_restore
from backuppy.cli.restore import _parse_destination
from backuppy.cli.restore import main
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


def test_parse_destination(fs):
    assert _parse_destination('foo/bar', 'fake_backup1') == (
        '/foo/bar/fake_backup1',
        './foo/bar/fake_backup1',
    )
    assert _parse_destination('/foo/bar', 'fake_backup1') == (
        '/foo/bar/fake_backup1',
        '/foo/bar/fake_backup1',
    )
    assert _parse_destination(None, 'fake_backup1') == (
        '/fake_backup1',
        './fake_backup1',
    )


@pytest.mark.parametrize('retval', [True, False])
def test_confirm_restore(retval, mock_manifest_entry_list, capsys):
    mock_manifest_entry_list[0].commit_timestamp = 100
    with mock.patch('backuppy.cli.restore.ask_for_confirmation', return_value=retval), \
            mock.patch('backuppy.cli.restore.os.path.exists', return_value=retval):
        assert _confirm_restore(mock_manifest_entry_list, '/home/foo/bar', './foo/bar') == retval

    out, _ = capsys.readouterr()
    assert ('WARNING' in out) == retval


@pytest.mark.parametrize('sha,entries', [
    (None, None),
    ('abcd1234', []),
    ('abcd1234', [mock.Mock()]),
])
@pytest.mark.parametrize('retval', [True, False])
def test_main(retval, sha, entries):
    backup_store = mock.MagicMock(
        manifest=mock.Mock(
            get_entries_by_sha=mock.Mock(return_value=entries),
            search=mock.Mock(return_value=[('/foo', [mock.Mock()]), ('/bar', [mock.Mock()])])
        ),
    )
    with mock.patch(
            'backuppy.cli.restore._parse_destination',
            return_value=('/restore/path/fake_backup1', '/restore/path/fake_backup1'),
        ), mock.patch('backuppy.cli.restore._confirm_restore', return_value=retval), \
            mock.patch('backuppy.cli.restore._restore') as mock_restore, \
            mock.patch('backuppy.cli.restore.parse_time'), \
            mock.patch('backuppy.cli.restore.get_backup_store', return_value=backup_store), \
            mock.patch('backuppy.cli.restore.staticconf'), \
            (pytest.raises(ValueError) if (sha and not entries) else ExitStack()):
        main(argparse.Namespace(
            before='2019-04-03',
            config='backuppy.conf',
            dest='/restore/path',
            like='',
            name='fake_backup1',
            sha=sha,
            preserve_scratch_dir=False,
            yes=False,
        ))
        assert mock_restore.call_count == int(retval)
        if entries and retval:
            assert mock_restore.call_args_list == [mock.call(
                entries,
                '/restore/path/fake_backup1',
                backup_store,
            )]
