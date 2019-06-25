import os
import sqlite3

import pytest

from itests.conftest import _TestFileData
from itests.conftest import backup_itest_wrapper
from itests.conftest import DATA_DIR
from itests.conftest import ITEST_MANIFEST_PATH

test_file_history = dict()  # type: ignore


def abort():
    raise Exception('abort')


def assert_manifest_correct(before):
    manifest_conn = sqlite3.connect(ITEST_MANIFEST_PATH)
    manifest_conn.row_factory = sqlite3.Row
    manifest_cursor = manifest_conn.cursor()

    manifest_cursor.execute('select * from manifest')
    rows = manifest_cursor.fetchall()
    assert len(rows) == 3 if before else 4
    for row in rows:
        start_pos = row[0].find(DATA_DIR)
        filename = row[0][start_pos:]
        assert row[1] in set([tfd.sha for tfd in test_file_history[filename]])


@pytest.fixture(autouse=True, scope='module')
@backup_itest_wrapper(
    test_file_history,
    _TestFileData('foo', 'asdf'),
    _TestFileData('bar', 'hjkl'),
    _TestFileData('baz/buz', 'qwerty'),
)
def setup_manifest():
    pass


@backup_itest_wrapper(
    test_file_history,
    _TestFileData('foo', 'asdfhjkl'),
    trace=(abort, pytest.raises(Exception)),
)
def test_m1_crash_before_save():
    assert_manifest_correct(before=True)


@backup_itest_wrapper(
    test_file_history,
    trace=(abort, pytest.raises(Exception)),
)
def test_m1_crash_after_save():
    assert_manifest_correct(before=False)


@backup_itest_wrapper(
    test_file_history,
    _TestFileData('another_file', '1234'),
    trace=(abort, None),
)
def test_m2_crash_before_file_save():
    manifest_conn = sqlite3.connect(ITEST_MANIFEST_PATH)
    manifest_conn.row_factory = sqlite3.Row
    manifest_cursor = manifest_conn.cursor()

    manifest_cursor.execute('select * from manifest where abs_file_name like "%another_file"')
    rows = manifest_cursor.fetchall()
    assert not rows


@backup_itest_wrapper(
    test_file_history,
    trace=(abort, None),
)
def test_m2_crash_after_file_save():
    manifest_conn = sqlite3.connect(ITEST_MANIFEST_PATH)
    manifest_conn.row_factory = sqlite3.Row
    manifest_cursor = manifest_conn.cursor()

    manifest_cursor.execute('select * from manifest where abs_file_name like "%another_file"')
    rows = manifest_cursor.fetchall()
    assert len(rows) == 1
    assert rows[0][1] == test_file_history[os.path.join(DATA_DIR, 'another_file')][0].sha
