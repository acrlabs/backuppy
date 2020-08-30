import argparse
import os
import sqlite3
import time
from contextlib import contextmanager
from shutil import rmtree

import pytest

from backuppy.blob import apply_diff
from backuppy.cli.backup import main as backup
from backuppy.cli.restore import main as restore
from backuppy.io import IOIter
from backuppy.util import file_walker
from backuppy.util import path_join
from itests.conftest import _TestFileData
from itests.conftest import BACKUP_DIR
from itests.conftest import get_latest_manifest
from itests.conftest import ITEST_CONFIG
from itests.conftest import itest_setup
from itests.conftest import RESTORE_DIR

test_file_history = dict()  # type: ignore


BACKUP_ARGS = argparse.Namespace(
    log_level='debug',
    config=ITEST_CONFIG,
    preserve_scratch_dir=True,
    name='data1_backup',
)
RESTORE_ARGS = argparse.Namespace(
    disable_compression=True,
    disble_encryption=True,
    before='now',
    config=ITEST_CONFIG,
    dest=RESTORE_DIR,
    name='data1_backup',
    sha=None,
    like='',
    preserve_scratch_dir=True,
    yes=False,
)


@pytest.fixture(autouse=True)
def clear_restore():
    try:
        rmtree(RESTORE_DIR)
        time.sleep(1)  # give the filesystem some time to catch up since we do this for every test
    except FileNotFoundError:
        pass


def get_backup_dir_state():
    backup_dir_state = dict()
    if not os.path.exists(BACKUP_DIR):
        return dict()

    for f in file_walker(BACKUP_DIR):
        backup_dir_state[f] = os.stat(f).st_mtime
    return backup_dir_state


def assert_backup_store_correct():
    latest_manifest = get_latest_manifest()
    manifest_conn = sqlite3.connect(latest_manifest)
    manifest_conn.row_factory = sqlite3.Row
    manifest_cursor = manifest_conn.cursor()
    for path, history in test_file_history.items():
        latest = history[-1]

        manifest_cursor.execute(
            'select * from manifest where abs_file_name=? order by commit_timestamp',
            (os.path.abspath(latest.path),),
        )
        rows = manifest_cursor.fetchall()
        if 'dont_back_me_up' in path:
            assert len(rows) == 0
            continue
        else:
            assert len(rows) == len(history)
            for row, expected in zip(rows, history):
                assert row['sha'] == expected.sha
                assert row['mode'] == expected.mode

        if latest.backup_path:
            manifest_cursor.execute(
                'select * from base_shas where sha=?',
                (latest.sha,),
            )
            row = manifest_cursor.fetchone()
            with IOIter(latest.backup_path) as n:
                if not row or not row[1]:
                    assert n.fd.read() == latest.contents
                else:
                    orig_file_path = path_join(BACKUP_DIR, row[1][:2], row[1][2:4], row[1][4:])
                    with IOIter(orig_file_path) as o, IOIter() as tmp:
                        apply_diff(o, n, tmp)
                        tmp.fd.seek(0)
                        assert tmp.fd.read() == latest.contents


def assert_restore_correct():
    itest_restore_root = os.path.join(RESTORE_DIR, 'data1_backup')
    for f in file_walker(itest_restore_root):
        abs_file_name = f[f.find(itest_restore_root) + len(itest_restore_root):]
        with open(f) as restore_file, open(abs_file_name) as orig_file:
            assert restore_file.read() == orig_file.read()


def check_backup_restore(dry_run):
    BACKUP_ARGS.dry_run = dry_run
    original_state = get_backup_dir_state()
    backup(BACKUP_ARGS)
    if dry_run:
        assert original_state == get_backup_dir_state()
    else:
        assert_backup_store_correct()
        restore(RESTORE_ARGS)
        assert_restore_correct()


@contextmanager
def initial_backup_files():
    with itest_setup(
        test_file_history,
        _TestFileData('foo', 'asdf'),
        _TestFileData('bar', 'hjkl'),
        _TestFileData('baz/buz', 'qwerty'),
        _TestFileData('dont_back_me_up_1', 'secrets!'),
        _TestFileData('baz/dont_back_me_up_2', 'moar secrets!'),
        _TestFileData('fizzbuzz', 'I am a walrus', data_dir_index=1),
    ):
        yield


@contextmanager
def unchanged():
    with itest_setup(test_file_history):
        yield


@contextmanager
def contents_changed():
    with itest_setup(
        test_file_history,
        _TestFileData('foo', 'adz foobar'),
        _TestFileData('bar', 'hhhhh'),
    ):
        yield


@contextmanager
def file_deleted():
    with itest_setup(
        test_file_history,
        _TestFileData('foo', None),
    ):
        yield


@contextmanager
def file_restored():
    with itest_setup(
        test_file_history,
        _TestFileData('foo', 'adz foobar'),
    ):
        yield


@contextmanager
def mode_changed():
    with itest_setup(
        test_file_history,
        _TestFileData('foo', 'adz foobar', mode=0o100755),
    ):
        yield


@contextmanager
def contents_changed_after_delete():
    with itest_setup(
        test_file_history,
        _TestFileData('foo', 'adfoo blah blah blah blah blah'),
    ):
        yield


@contextmanager
def new_file_same_contents():
    with itest_setup(
        test_file_history,
        _TestFileData('new_file', 'adz foobar'),  # this points at a diff
    ):
        yield


@contextmanager
def old_file_same_contents():
    with itest_setup(
        test_file_history,
        _TestFileData('bar', 'I am a walrus'),  # this points at an original
    ):
        yield


@pytest.mark.parametrize('dry_run', [True, False])
@pytest.mark.parametrize('fixture', [
    initial_backup_files,
    unchanged,
    contents_changed,
    file_deleted,
    file_restored,
    mode_changed,
    contents_changed_after_delete,
    new_file_same_contents,
    old_file_same_contents,
])
def test_initial_backup(dry_run, fixture):
    with fixture():
        check_backup_restore(dry_run)
