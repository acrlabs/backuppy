import os
import sqlite3

import pytest

from backuppy.util import sha_to_path
from itests.conftest import _TestFileData
from itests.conftest import BACKUP_DIR
from itests.conftest import backup_itest_wrapper
from itests.conftest import DATA_DIRS
from itests.conftest import ITEST_MANIFEST_PATH
from itests.conftest import ItestException

test_file_history = dict()  # type: ignore
DATA_DIR = DATA_DIRS[0]


def abort():
    raise ItestException('abort')


def assert_manifest_correct(before):
    manifest_conn = sqlite3.connect(ITEST_MANIFEST_PATH)
    manifest_conn.row_factory = sqlite3.Row
    manifest_cursor = manifest_conn.cursor()

    manifest_cursor.execute('select * from manifest')
    rows = manifest_cursor.fetchall()
    assert len(rows) == (3 if before else 4)
    for row in rows:
        start_pos = row[1].find(DATA_DIR)
        filename = row[1][start_pos:]
        assert row[2] in set([tfd.sha for tfd in test_file_history[filename]])


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
    side_effect=(abort, pytest.raises(Exception)),
)
def test_m1_crash_before_save():
    assert_manifest_correct(before=True)
    file_data_path = os.path.join(DATA_DIR, 'foo')
    file_backup_path = os.path.join(
        BACKUP_DIR,
        sha_to_path(test_file_history[file_data_path][1].sha),
    )
    assert os.path.exists(file_backup_path)


@backup_itest_wrapper(
    test_file_history,
    side_effect=(abort, pytest.raises(Exception)),
)
def test_m1_crash_after_save():
    assert_manifest_correct(before=False)


@backup_itest_wrapper(
    test_file_history,
    _TestFileData('another_file', '1234'),
    side_effect=(abort, None),
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
    side_effect=(abort, None),
)
def test_m2_crash_after_file_save():
    manifest_conn = sqlite3.connect(ITEST_MANIFEST_PATH)
    manifest_conn.row_factory = sqlite3.Row
    manifest_cursor = manifest_conn.cursor()

    manifest_cursor.execute('select * from manifest where abs_file_name like "%another_file"')
    rows = manifest_cursor.fetchall()
    assert len(rows) == 1
    assert rows[0][2] == test_file_history[os.path.join(DATA_DIR, 'another_file')][0].sha
