import os
import sqlite3

from backuppy.blob import apply_diff
from backuppy.io import IOIter
from itests.conftest import _TestFileData
from itests.conftest import BACKUP_DIR
from itests.conftest import backup_itest_wrapper
from itests.conftest import ITEST_MANIFEST_PATH

test_file_history = dict()  # type: ignore


def assert_backup_store_correct():
    manifest_conn = sqlite3.connect(ITEST_MANIFEST_PATH)
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
                assert row[1] == expected.sha
                assert row[-2] == expected.mode

        if latest.backup_path:
            manifest_cursor.execute(
                'select * from diff_pairs where sha=?',
                (latest.sha,),
            )
            row = manifest_cursor.fetchone()
            with IOIter(latest.backup_path) as n:
                if not row or not row[1]:
                    assert n.fd.read() == latest.contents
                else:
                    orig_file_path = os.path.join(
                        BACKUP_DIR, row[1][:2], row[1][2:4], row[1][4:])
                    with IOIter(orig_file_path) as o, IOIter() as tmp:
                        apply_diff(o, n, tmp)
                        tmp.fd.seek(0)
                        assert tmp.fd.read() == latest.contents


@backup_itest_wrapper(
    test_file_history,
    _TestFileData('foo', 'asdf'),
    _TestFileData('bar', 'hjkl'),
    _TestFileData('baz/buz', 'qwerty'),
    _TestFileData('dont_back_me_up_1', 'secrets!'),
    _TestFileData('baz/dont_back_me_up_2', 'moar secrets!'),
    _TestFileData('fizzbuzz', 'I am a walrus', data_dir_index=1),
)
def test_initial_backup():
    assert_backup_store_correct()


@backup_itest_wrapper(test_file_history)
def test_backup_unchanged():
    assert_backup_store_correct()


@backup_itest_wrapper(
    test_file_history,
    _TestFileData('foo', 'adz foobar'),
    _TestFileData('bar', 'hhhhh'),
)
def test_file_contents_changed():
    assert_backup_store_correct()


@backup_itest_wrapper(
    test_file_history,
    _TestFileData('foo', None),
)
def test_file_deleted():
    assert_backup_store_correct()


@backup_itest_wrapper(
    test_file_history,
    _TestFileData('foo', 'adz foobar'),
)
def test_file_restored():
    assert_backup_store_correct()


@backup_itest_wrapper(
    test_file_history,
    _TestFileData('foo', 'adz foobar', mode=0o100755),
)
def test_mode_changed():
    assert_backup_store_correct()


@backup_itest_wrapper(
    test_file_history,
    _TestFileData('foo', 'adfoo blah blah blah blah blah'),
)
def test_contents_changed_after_delete():
    assert_backup_store_correct()


@backup_itest_wrapper(
    test_file_history,
    _TestFileData('new_file', 'adz foobar'),
)
def test_new_file_same_contents():
    assert_backup_store_correct()
