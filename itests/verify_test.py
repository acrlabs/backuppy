import argparse
from time import sleep

import pytest
import staticconf
import staticconf.testing

from backuppy.cli.backup import main as backup
from backuppy.cli.verify import main as verify
from backuppy.stores import get_backup_store
from itests.conftest import _TestFileData
from itests.conftest import BACKUP_ARGS
from itests.conftest import clean_up_temp_directories
from itests.conftest import ITEST_CONFIG
from itests.conftest import itest_setup

test_file_history = dict()  # type: ignore
VERIFY_ARGS = argparse.Namespace(
    log_level='debug',
    config=ITEST_CONFIG,
    name='data1_backup',
    preserve_scratch_dir=True,
    sha=None,
    like='',
    show_all=True,
    yes=True,
)


@pytest.fixture(autouse=True)
def use_encryption():
    with staticconf.testing.PatchConfiguration({
        'options': [{'use_encryption': True, 'discard_diff_percentage': None}],
    }, namespace='data1_backup'):
        yield


@pytest.fixture(autouse=True)
def run_backup(use_encryption):
    global test_file_history
    test_file_history = dict()
    clean_up_temp_directories()
    with itest_setup(
        test_file_history,
        _TestFileData('foo', 'asdfasdfasdf'),
    ):
        BACKUP_ARGS.dry_run = False
        backup(BACKUP_ARGS)


@pytest.mark.parametrize('fast', [True, False])
def test_verify(fast, capsys):
    VERIFY_ARGS.fast = fast
    verify(VERIFY_ARGS)
    out, _ = capsys.readouterr()
    assert 'ERROR' not in out


def test_verify_corrupted(capsys):
    VERIFY_ARGS.fast = False
    backup_store = get_backup_store('data1_backup')
    with backup_store.unlock(preserve_scratch=True):
        backup_store.manifest._cursor.execute(
            'update manifest set key_pair =?',
            (b'asdf',),
        )
        backup_store.manifest._commit()
    verify(VERIFY_ARGS)
    out, _ = capsys.readouterr()
    assert 'ERROR' in out

    # Shouldn't create duplicate entries when we fix
    with backup_store.unlock(preserve_scratch=True):
        backup_store.manifest._cursor.execute('select * from manifest')
        rows = backup_store.manifest._cursor.fetchall()
        assert len(rows) == 1

    # After the fix, verify should be clean
    verify(VERIFY_ARGS)
    out, _ = capsys.readouterr()
    assert 'ERROR' not in out


def test_base_sha_corrupted(capsys):
    sleep(1)  # make sure the backup timestamps differ
    with itest_setup(
        test_file_history,
        _TestFileData('bar', 'asdfasdfasdf'),
        _TestFileData('foo', 'asdfasdfasd'),
    ):
        BACKUP_ARGS.dry_run = False
        backup(BACKUP_ARGS)

    backup_store = get_backup_store('data1_backup')
    with backup_store.unlock(preserve_scratch=True):
        backup_store.manifest._cursor.execute(
            'update manifest set key_pair=? where abs_file_name like ? ',
            (b'hjkl', '%bar'),
        )
        backup_store.manifest._commit()

    VERIFY_ARGS.fast = False
    verify(VERIFY_ARGS)
    out, _ = capsys.readouterr()
    assert 'ERROR' in out

    # Shouldn't create duplicate entries when we fix
    with backup_store.unlock(preserve_scratch=True):
        backup_store.manifest._cursor.execute('select * from manifest')
        rows = backup_store.manifest._cursor.fetchall()
        assert len(rows) == 3

    # After the fix, verify should be clean
    verify(VERIFY_ARGS)
    out, _ = capsys.readouterr()
    assert 'ERROR' not in out


@pytest.mark.parametrize('both_bad', [True, False])
def test_duplicate_entries(both_bad, capsys):
    VERIFY_ARGS.fast = True
    backup_store = get_backup_store('data1_backup')
    with backup_store.unlock(preserve_scratch=True):
        # We can't test this with the "unique" index in place
        backup_store.manifest._cursor.execute('drop index mfst_unique_idx')
        backup_store.manifest._cursor.execute('select * from manifest')
        row = backup_store.manifest._cursor.fetchone()
        backup_store.manifest._cursor.execute(
            '''
            insert into manifest (abs_file_name, sha, uid, gid, mode, key_pair, commit_timestamp)
            values (?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                row['abs_file_name'],
                row['sha'],
                row['uid'],
                row['gid'],
                row['mode'],
                row['key_pair'],
                row['commit_timestamp'] + 10,
            )
        )
        if both_bad:
            backup_store.manifest._cursor.execute('update manifest set key_pair=?', (b'asdf',))
        backup_store.manifest._commit()
    verify(VERIFY_ARGS)
    out, _ = capsys.readouterr()
    assert 'Found 2 duplicate entries' in out
    if both_bad:
        assert 'is corrupt' in out
        assert 'No valid entries' in out
    else:
        assert 'seems good' in out

    # Shouldn't create duplicate entries when we fix
    with backup_store.unlock(preserve_scratch=True):
        backup_store.manifest._cursor.execute('select * from manifest')
        rows = backup_store.manifest._cursor.fetchall()
        assert len(rows) == 1

    with backup_store.unlock(preserve_scratch=True):
        # Restore the "unique" index
        backup_store.manifest._cursor.execute(
            'create unique index mfst_unique_idx on manifest(abs_file_name, sha, uid, gid, mode)',
        )
        backup_store.manifest._commit()

    # After the fix, verify should be clean
    VERIFY_ARGS.fast = False
    verify(VERIFY_ARGS)
    out, _ = capsys.readouterr()
    assert 'ERROR' not in out


@pytest.mark.parametrize('both_bad', [True, False])
def test_shas_with_bad_key_pairs(both_bad, capsys):
    with itest_setup(
        test_file_history,
        _TestFileData('bar', 'asdfasdfasdf'),
    ):
        BACKUP_ARGS.dry_run = False
        backup(BACKUP_ARGS)

    VERIFY_ARGS.fast = True
    backup_store = get_backup_store('data1_backup')
    with backup_store.unlock(preserve_scratch=True):
        backup_store.manifest._cursor.execute(
            'update manifest set key_pair=? where abs_file_name like ?',
            (b'asdf', '%bar'),
        )
        if both_bad:
            backup_store.manifest._cursor.execute(
                'update manifest set key_pair=? where abs_file_name like ?',
                (b'hjkl', '%foo'),
            )
        backup_store.manifest._commit()
    verify(VERIFY_ARGS)
    out, _ = capsys.readouterr()
    assert 'Found 2 entries for' in out
    if both_bad:
        assert 'No valid entries' in out
    else:
        assert 'seems good' in out

    # Shouldn't create duplicate entries when we fix
    with backup_store.unlock(preserve_scratch=True):
        backup_store.manifest._cursor.execute('select * from manifest')
        rows = backup_store.manifest._cursor.fetchall()
        assert len(rows) == 2

    # After the fix, verify should be clean
    VERIFY_ARGS.fast = False
    verify(VERIFY_ARGS)
    out, _ = capsys.readouterr()
    assert 'ERROR' not in out
