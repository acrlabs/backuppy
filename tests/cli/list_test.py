import argparse
import re

import mock
import pytest

from backuppy.cli.list import _print_details
from backuppy.cli.list import _print_summary
from backuppy.cli.list import _split_root_prefix
from backuppy.cli.list import main
from backuppy.manifest import ManifestEntry
from backuppy.util import format_time


@pytest.fixture
def mock_search_results():
    return [
        ('/path/1/file1', [
            ManifestEntry('/path/1/file1', 'ab1dedef', None, 1000, 1000, 12345, b'1111', None, 100),
            ManifestEntry('/path/1/file1', 'ab2dede1', None, 1000, 1000, 12345, b'2222', None, 75),
            ManifestEntry('/path/1/file1', 'ab3dede2', None, 1000, 1000, 12345, b'3333', None, 20),
        ]),
        ('/path/2/file2', [
            ManifestEntry('/path/2/file2', '1b1dedef', None, 1000, 1000, 12345, b'4444', None, 105),
            ManifestEntry('/path/2/file2', '1b2dede1', None, 1000, 1000, 12345, b'5555', None, 65),
        ]),
        ('/path/2/file3', [
            ManifestEntry('/path/2/file2', None, None, None, None, None, None, None, 176),
            ManifestEntry('/path/2/file2', 'ffffeeee', None, 1000, 1000, 12345, b'6666', None, 105),
        ]),
        ('/path/2/file4', [
            ManifestEntry('/path/2/file2', '12834567', None, 1000, 1000, 12345, b'7777', None, 105),
        ]),
    ]


def test_split_root_prefix():
    assert _split_root_prefix('/path/1/the_file', 'fake_backup2') == ('/path/1/', 'the_file')


def test_split_root_prefix_not_present():
    with pytest.raises(ValueError):
        _split_root_prefix('/path/0/the_file', 'fake_backup2')


@pytest.mark.parametrize('deleted,changed', [
    (False, False),
    (True, False),
    (False, True),
    (True, True),
])
def test_print_summary(deleted, changed, mock_search_results, capsys):
    _print_summary('fake_backup2', mock_search_results, deleted, changed)
    out, err = capsys.readouterr()
    if not deleted:
        assert re.search(r'file1\s+3\s+' + format_time(100), out)
        assert re.search(r'file2\s+2\s+' + format_time(105), out)
        if not changed:
            assert re.search(r'file4\s+1\s+' + format_time(105), out)
    elif changed:
        assert not re.search('file[12]', out)
    else:
        assert not re.search('file[124]', out)

    assert re.search(r'file3\s+2\s+y\s+' + format_time(176), out)


def test_print_details(mock_search_results, capsys):
    _print_details('fake_backup2', mock_search_results, False, False, 5)
    out, err = capsys.readouterr()
    assert re.search(r'ab1de\.\.\..*' + format_time(100), out)
    assert re.search(r'ab2de\.\.\..*' + format_time(75), out)
    assert re.search(r'ab3de\.\.\..*' + format_time(20), out)
    assert re.search(r'1b1de\.\.\..*' + format_time(105), out)
    assert re.search(r'1b2de\.\.\..*' + format_time(65), out)


@pytest.mark.parametrize('after,before,details', [
    (None, None, False),
    ('2009-12-31T16:00:10+00:00', '2009-12-31T16:05:00+00:00', True)
])
def test_main(after, before, details):
    args = argparse.Namespace(
        after=after,
        before=before,
        details=details,
        config='backuppy.conf',
        file_limit=None,
        history_limit=None,
        like=None,
        name='fake_backup1',
        sha_length=17,
        preserve_scratch_dir=False,
        deleted=False,
        changed=False,
    )
    with mock.patch('backuppy.cli.list.get_backup_store') as mock_get_store, \
            mock.patch('backuppy.cli.list.staticconf.YamlConfiguration'), \
            mock.patch('backuppy.cli.list.time.time', return_value=1000), \
            mock.patch('backuppy.cli.list._print_summary') as mock_summary, \
            mock.patch('backuppy.cli.list._print_details') as mock_details:
        backup_store = mock_get_store.return_value
        main(args)
        assert backup_store.manifest.search.call_args == mock.call(
            after_timestamp=(0 if not after else 1262275210),
            before_timestamp=(1000 if not before else 1262275500),
            file_limit=None,
            history_limit=None,
            like=None,
        )
        assert mock_summary.call_count == int(not details)
        assert mock_details.call_count == int(details)
