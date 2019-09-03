import argparse
import re

import mock

from backuppy.cli.backup import _scan_directory
from backuppy.cli.backup import main
from backuppy.stores.backup_store import BackupStore


@mock.patch('backuppy.cli.backup.file_walker')
def test_scan_directory(file_walker):
    file_walker.return_value = ['/file1', '/error', '/file2', '/file3', '/skip']
    store = mock.MagicMock(spec=BackupStore)

    def save_if_new(filename):
        if filename == '/error':
            raise Exception('oops!')

    store.save_if_new.side_effect = save_if_new

    _scan_directory('/', store, [re.compile('skip')])
    assert store.save_if_new.call_args_list == [
        mock.call('/file1'),
        mock.call('/error'),
        mock.call('/file2'),
        mock.call('/file3'),
    ]


def test_main():
    with mock.patch('backuppy.cli.backup.staticconf.YamlConfiguration'), \
            mock.patch('backuppy.cli.backup._scan_directory') as mock_scan, \
            mock.patch('backuppy.cli.backup.get_backup_store') as mock_get_store:
        store = mock_get_store.return_value
        store.manifest.files.return_value = {'/file1', '/file2', '/file3', '/file4'}
        mock_scan.side_effect = [{'/file1', '/file2', '/file3'}, {'/file1'}, {'/file2', '/file3'}]
        main(argparse.Namespace(config='backuppy.conf', preserve_scratch_dir=False))
        assert mock_scan.call_count == 3
        for i in range(3):
            assert mock_scan.call_args_list[i][0][0] == f'/path/{i}'
        assert mock_scan.call_args_list[0][0][2] == [
            re.compile('dont_back_this_up'), re.compile('foo')]
        assert mock_scan.call_args_list[1][0][2] == [
            re.compile('dont_back_this_up'), re.compile('bar')]
        assert mock_scan.call_args_list[2][0][2] == [
            re.compile('dont_back_this_up'), re.compile('bar')]
        assert all([c == mock.call('/file4') for c in store.manifest.delete.call_args_list])
