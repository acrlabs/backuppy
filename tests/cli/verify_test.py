import argparse
from contextlib import ExitStack

import mock
import pytest

from backuppy.cli.verify import _fix_duplicate_entries
from backuppy.cli.verify import _fix_shas_with_multiple_key_pairs
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


def test_fix_duplicate_entries_ok(capsys):
    backup_store = mock.MagicMock()
    _fix_duplicate_entries(backup_store)
    out, _ = capsys.readouterr()
    assert 'trying to clean up' not in out


@pytest.mark.parametrize('has_good_entry,path_exists', [(True, None), (False, False), (False, True)])
def test_fix_duplicate_entries(has_good_entry, path_exists, mock_manifest_entry_list, capsys):
    mock_manifest_entry_list.extend(mock_manifest_entry_list)
    backup_store = mock.Mock()
    backup_store.manifest.find_duplicate_entries.return_value = mock_manifest_entry_list
    with mock.patch('backuppy.cli.verify.ask_for_confirmation', return_value=True), \
            mock.patch('backuppy.cli.verify._check_entry') as mock_check, \
            mock.patch('backuppy.cli.verify.os.path.exists', return_value=path_exists):
        mock_check.side_effect = None if has_good_entry else Exception
        _fix_duplicate_entries(backup_store)
    out, _ = capsys.readouterr()
    assert 'trying to clean up' in out
    assert backup_store.manifest.delete_entry.call_count == (1 if has_good_entry else 2)
    if has_good_entry:
        assert backup_store.save_if_new.call_count == 0
        assert backup_store.manifest.delete.call_count == 0
    else:
        assert backup_store.save_if_new.call_count == (1 if path_exists else 0)
        assert backup_store.manifest.delete.call_count == (0 if path_exists else 1)


def test_fix_shas_with_multiple_key_pairs_ok(capsys):
    backup_store = mock.MagicMock()
    _fix_shas_with_multiple_key_pairs(backup_store)
    out, _ = capsys.readouterr()
    assert 'trying to clean up' not in out


@pytest.mark.parametrize('has_good_entry', [True, False])
def test_fix_shas_with_multiple_key_pairs(has_good_entry, mock_manifest_entry_list, capsys):
    mock_manifest_entry_list.extend(mock_manifest_entry_list)
    mock_manifest_entry_list[1].key_pair = b'2222'
    backup_store = mock.Mock()
    backup_store.manifest.find_shas_with_multiple_key_pairs.return_value = mock_manifest_entry_list
    with mock.patch('backuppy.cli.verify.ask_for_confirmation', return_value=True), \
            mock.patch('backuppy.cli.verify._check_entry') as mock_check:
        mock_check.side_effect = [None, Exception] if has_good_entry else Exception
        _fix_shas_with_multiple_key_pairs(backup_store)
    out, _ = capsys.readouterr()
    assert 'trying to clean up' in out
    assert backup_store.manifest.insert_or_update.call_count == (2 if has_good_entry else 0)
    assert backup_store.save_if_new.call_count == (0 if has_good_entry else 1)


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


@pytest.mark.parametrize('sha,entries,fast', [
    (None, None, False),
    (None, None, True),
    ('abcd1234', [], False),
    ('abcd1234', [mock.Mock()], False),
])
def test_main(sha, entries, fast):
    backup_store = mock.MagicMock(
        manifest=mock.Mock(
            get_entries_by_sha=mock.Mock(return_value=entries),
            search=mock.Mock(return_value=[('/foo', [mock.Mock()]), ('/bar', [mock.Mock()])])
        ),
    )
    with mock.patch('backuppy.cli.verify._verify') as mock_verify, \
            mock.patch('backuppy.cli.verify._fast_verify') as mock_fast_verify, \
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
            fast=fast,
        ))
        if not fast:
            assert mock_verify.call_count == 1
            assert mock_fast_verify.call_count == 0
            if entries:
                assert mock_verify.call_args_list == [mock.call(
                    entries,
                    backup_store,
                    True,
                )]
        else:
            assert mock_fast_verify.call_count == 1
            assert mock_verify.call_count == 0
