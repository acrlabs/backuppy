import os
import sqlite3
from subprocess import run

from backuppy.blob import apply_diff
from backuppy.io import IOIter
from itests.conftest import BACKUP_DIR
from itests.conftest import compute_sha
from itests.conftest import DATA_DIR
from itests.conftest import ITEST_MANIFEST_PATH
from itests.conftest import ITEST_ROOT

BACKUP_CMD = [
    'python', '-m', 'backuppy.run',
    '--log-level', 'debug2',
    '--disable-compression',
    '--disable-encryption',
    'backup',
    '--config', os.path.join(ITEST_ROOT, 'itest.conf'),
]
test_file_history = dict()


class _TestFileData:
    def __init__(self, filename, contents, mode=0o100644):
        self.path = os.path.join(DATA_DIR, filename)
        if contents:
            self.contents = contents.encode()
            self.sha = compute_sha(self.contents)
            self.mode = mode
        else:
            self.contents = None
            self.sha = None
            self.mode = None

    def write(self):
        if self.contents:
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            with open(self.path, 'wb') as f:
                f.write(self.contents)
            os.chmod(self.path, self.mode)
        else:
            os.remove(self.path)

    @property
    def backup_path(self):
        if self.sha:
            return os.path.join(BACKUP_DIR, self.sha[:2], self.sha[2:4], self.sha[4:])
        else:
            return None

    def __eq__(self, other):
        return other and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


def backup_itest_wrapper(*dec_args):
    global test_file_history

    def decorator(test_case):
        def wrapper(*args, **kwargs):
            for tfd in dec_args:
                if tfd.path in test_file_history and tfd != test_file_history[tfd.path][-1]:
                    test_file_history[tfd.path].append(tfd)
                    tfd.write()
                elif tfd.path not in test_file_history:
                    test_file_history[tfd.path] = [tfd]
                    tfd.write()

            run(BACKUP_CMD)
            test_case(*args, **kwargs)

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
                            orig_file_path = os.path.join(BACKUP_DIR, row[1][:2], row[1][2:4], row[1][4:])
                            with IOIter(orig_file_path) as o, IOIter() as tmp:
                                apply_diff(o, n, tmp)
                                tmp.fd.seek(0)
                                assert tmp.fd.read() == latest.contents
        return wrapper
    return decorator


@backup_itest_wrapper(
    _TestFileData('foo', 'asdf'),
    _TestFileData('bar', 'hjkl'),
    _TestFileData('baz/buz', 'qwerty'),
    _TestFileData('dont_back_me_up_1', 'secrets!'),
    _TestFileData('baz/dont_back_me_up_2', 'moar secrets!'),
)
def test_initial_backup():
    pass


@backup_itest_wrapper()
def test_backup_unchanged():
    pass


@backup_itest_wrapper(
    _TestFileData('foo', 'adz foobar'),
    _TestFileData('bar', 'hhhhh'),
)
def test_file_contents_changed():
    pass


@backup_itest_wrapper(
    _TestFileData('foo', None),
)
def test_file_deleted():
    pass


@backup_itest_wrapper(
    _TestFileData('foo', 'adz foobar'),
)
def test_file_restored():
    pass


@backup_itest_wrapper(
    _TestFileData('foo', 'adz foobar', mode=0o100755),
)
def test_mode_changed():
    pass


@backup_itest_wrapper(
    _TestFileData('foo', 'adfoo blah blah blah blah blah'),
)
def test_contents_changed_after_delete():
    pass


@backup_itest_wrapper(
    _TestFileData('new_file', 'adz foobar'),
)
def test_new_file_same_contents():
    pass
