import argparse
import re

import mock
import pytest
import staticconf

from backuppy.cli.list import _find_root_prefix
from backuppy.cli.list import _print_details
from backuppy.cli.list import _print_summary
from backuppy.cli.list import main
from backuppy.manifest import ManifestEntry
from backuppy.util import format_time


@pytest.fixture
def backup_configs():
    for name, config in staticconf.read('backups').items():
        staticconf.DictConfiguration(config, namespace=name)


@pytest.fixture
def mock_search_results():
    return [
        ('/path/1/file1', [
            (ManifestEntry('/path/1/file1', 'ab1defabcdefabcdef', None, 1000, 1000, 12345), 100),
            (ManifestEntry('/path/1/file1', 'ab2defabcdefabcde1', None, 1000, 1000, 12345), 75),
            (ManifestEntry('/path/1/file1', 'ab3defabcdefabcde2', None, 1000, 1000, 12345), 20),
        ]),
        ('/path/2/file2', [
            (ManifestEntry('/path/2/file2', '1b1defabcdefabcdef', None, 1000, 1000, 12345), 105),
            (ManifestEntry('/path/2/file2', '1b2defabcdefabcde1', None, 1000, 1000, 12345), 65),
        ]),
    ]


def test_find_root_prefix(backup_configs):
    assert _find_root_prefix('/path/1/the_file', 'backup2') == '/path/1/'


def test_find_root_prefix_not_present(backup_configs):
    with pytest.raises(ValueError):
        assert _find_root_prefix('/path/0/the_file', 'backup2')


def test_print_summary(mock_search_results, capsys):
    _print_summary('backup2', mock_search_results)
    out, err = capsys.readouterr()
    assert re.search('file1.*3.*' + format_time(100), out)
    assert re.search('file2.*2.*' + format_time(105), out)


def test_print_details(mock_search_results, capsys):
    _print_details('backup2', mock_search_results, sha_len=5)
    out, err = capsys.readouterr()
    assert re.search(r'ab1de\.\.\..*' + format_time(100), out)
    assert re.search(r'ab2de\.\.\..*' + format_time(75), out)
    assert re.search(r'ab3de\.\.\..*' + format_time(20), out)
    assert re.search(r'1b1de\.\.\..*' + format_time(105), out)
    assert re.search(r'1b2de\.\.\..*' + format_time(65), out)


@pytest.mark.parametrize('after,before,details', [
    (None, None, False),
    ('1969-12-31 16:00:10', '1969-12-31 16:05:00', True)
])
def test_main(backup_configs, after, before, details):
    args = argparse.Namespace(
        after=after,
        before=before,
        details=details,
        config='backuppy.conf',
        file_limit=None,
        history_limit=None,
        like=None,
        name='backup1',
        sha_length=17,
    )
    with mock.patch('backuppy.cli.list.get_backup_store') as mock_get_store, \
            mock.patch('backuppy.cli.list.staticconf.YamlConfiguration'), \
            mock.patch('backuppy.cli.list.time.time', return_value=1000), \
            mock.patch('backuppy.cli.list._print_summary') as mock_summary, \
            mock.patch('backuppy.cli.list._print_details') as mock_details:
        backup_store = mock_get_store.return_value
        main(args)
        assert backup_store.manifest.search.call_args == mock.call(
            after_timestamp=(0 if not after else 10),
            before_timestamp=(1000 if not before else 300),
            file_limit=None,
            history_limit=None,
            like=None,
        )
        assert mock_summary.call_count == int(not details)
        assert mock_details.call_count == int(details)
