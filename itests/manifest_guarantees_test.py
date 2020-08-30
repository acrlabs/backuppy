import argparse
import os
import sqlite3
import sys

import pytest

from backuppy.cli.backup import main as backup
from backuppy.util import sha_to_path
from itests.conftest import _TestFileData
from itests.conftest import BACKUP_DIR
from itests.conftest import DATA_DIRS
from itests.conftest import get_latest_manifest
from itests.conftest import ITEST_CONFIG
from itests.conftest import itest_setup
from itests.conftest import ItestException
from itests.conftest import make_trace_func

test_file_history = dict()  # type: ignore
DATA_DIR = DATA_DIRS[0]
BACKUP_ARGS = argparse.Namespace(
    log_level='debug',
    config=ITEST_CONFIG,
    preserve_scratch_dir=True,
    dry_run=False,
    name='data1_backup',
)


def abort():
    raise ItestException('abort')


def assert_manifest_correct(before):
    manifest_conn = sqlite3.connect(get_latest_manifest())
    manifest_conn.row_factory = sqlite3.Row
    manifest_cursor = manifest_conn.cursor()

    manifest_cursor.execute('select * from manifest')
    rows = manifest_cursor.fetchall()
    assert len(rows) == (3 if before else 4)
    for row in rows:
        start_pos = row['abs_file_name'].find(DATA_DIR)
        filename = row['abs_file_name'][start_pos:]
        assert row['sha'] in set([tfd.sha for tfd in test_file_history[filename]])


@pytest.fixture(autouse=True, scope='module')
def setup_manifest():
    with itest_setup(
        test_file_history,
        _TestFileData('foo', 'asdf'),
        _TestFileData('bar', 'hjkl'),
        _TestFileData('baz/buz', 'qwerty'),
    ):
        backup(BACKUP_ARGS)
        yield


def test_m1_crash_before_save():
    sys.settrace(make_trace_func('test_m1_crash_before_save', abort))
    with itest_setup(
        test_file_history,
        _TestFileData('foo', 'asdfhjkl'),
    ), pytest.raises(Exception):
        backup(BACKUP_ARGS)
    assert_manifest_correct(before=True)
    file_data_path = os.path.join(DATA_DIR, 'foo')
    file_backup_path = os.path.join(
        BACKUP_DIR,
        sha_to_path(test_file_history[file_data_path][1].sha),
    )
    assert os.path.exists(file_backup_path)


def test_m1_crash_after_save():
    sys.settrace(make_trace_func('test_m1_crash_after_save', abort))
    with itest_setup(test_file_history), pytest.raises(Exception):
        backup(BACKUP_ARGS)
    assert_manifest_correct(before=False)


def test_m2_crash_before_file_save():
    sys.settrace(make_trace_func('test_m2_crash_before_file_save', abort))
    with itest_setup(
        test_file_history,
        _TestFileData('another_file', '1234'),
    ):
        backup(BACKUP_ARGS)
    manifest_conn = sqlite3.connect(get_latest_manifest())
    manifest_conn.row_factory = sqlite3.Row
    manifest_cursor = manifest_conn.cursor()

    manifest_cursor.execute('select * from manifest where abs_file_name like "%another_file"')
    rows = manifest_cursor.fetchall()
    assert not rows


def test_m2_crash_after_file_save():
    sys.settrace(make_trace_func('test_m2_crash_after_file_save', abort))
    with itest_setup(test_file_history):
        backup(BACKUP_ARGS)
    manifest_conn = sqlite3.connect(get_latest_manifest())
    manifest_conn.row_factory = sqlite3.Row
    manifest_cursor = manifest_conn.cursor()

    manifest_cursor.execute('select * from manifest where abs_file_name like "%another_file"')
    rows = manifest_cursor.fetchall()
    assert len(rows) == 1
    assert rows[0]['sha'] == test_file_history[os.path.join(DATA_DIR, 'another_file')][0].sha
