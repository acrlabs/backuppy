import inspect
import os
import re
import sys
import traceback
from contextlib import ExitStack
from hashlib import sha256
from shutil import rmtree

import mock
import pytest

from backuppy.run import main
from backuppy.stores.backup_store import MANIFEST_PATH

ITEST_ROOT = 'itests'
ITEST_CONFIG = os.path.join(ITEST_ROOT, 'itest.conf')
DATA_DIRS = [os.path.join(ITEST_ROOT, 'data'), os.path.join(ITEST_ROOT, 'data2')]
BACKUP_DIR = os.path.join(ITEST_ROOT, 'backup')
RESTORE_DIR = os.path.join(ITEST_ROOT, 'restore')
ITEST_MANIFEST_PATH = os.path.join(BACKUP_DIR, MANIFEST_PATH)
ITEST_SCRATCH = os.path.join(ITEST_ROOT, 'scratch')
BACKUP_ARGS = [
    '--log-level', 'debug',
    '--config', ITEST_CONFIG,
    'backup',
]


def compute_sha(string):
    sha_fn = sha256()
    sha_fn.update(string)
    return sha_fn.hexdigest()


@pytest.fixture(autouse=True, scope='module')
def initialize():
    try:
        [rmtree(d) for d in DATA_DIRS]
        rmtree(BACKUP_DIR)
        rmtree(ITEST_SCRATCH)
    except FileNotFoundError:
        pass

    [os.makedirs(d) for d in DATA_DIRS]
    os.makedirs(BACKUP_DIR)
    os.makedirs(ITEST_SCRATCH)


class ItestException(Exception):
    pass


class _TestFileData:
    def __init__(self, filename, contents, data_dir_index=0, mode=0o100644):
        self.path = os.path.join(DATA_DIRS[data_dir_index], filename)
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


def make_trace_func(search_string, side_effect):
    def trace_func(frame, event, arg):
        if event == 'call':
            try:
                module = inspect.getmodule(frame)
            except TypeError:
                return None
            if module and not module.__name__.startswith('backuppy'):
                if not hasattr(frame, 'f_trace_lines'):
                    return None
                frame.f_trace_lines = False
            return trace_func

        elif event == 'line':
            line = traceback.extract_stack(frame, limit=1)[0].line
            m = re.search(f'#\s+{search_string}', line)
            if m:
                # Note that if side_effect() raises an Exception, the trace function will
                # no longer function, because this must return a reference to trace_func and
                # raising doesn't return; the practical effect of this is that each individual
                # itest can only inject one "crash" into the application.  I think this is
                # generally OK, since itests "shouldn't" be testing multiple things at once
                side_effect()
            return trace_func

    return trace_func


def backup_itest_wrapper(
    test_file_history,
    *dec_args,
    side_effect=None,
):
    def decorator(test_case):
        def wrapper(*args, **kwargs):
            context = side_effect[1] if side_effect and side_effect[1] else ExitStack()
            for tfd in dec_args:
                if tfd.path in test_file_history and tfd != test_file_history[tfd.path][-1]:
                    test_file_history[tfd.path].append(tfd)
                    tfd.write()
                elif tfd.path not in test_file_history:
                    test_file_history[tfd.path] = [tfd]
                    tfd.write()

            if side_effect and side_effect[0]:
                sys.settrace(make_trace_func(test_case.__name__, side_effect[0]))
            else:
                sys.settrace(lambda a, b, c: None)
            with mock.patch('backuppy.stores.backup_store.get_scratch_dir') as mock_scratch, \
                    mock.patch('backuppy.util.shuffle') as mock_shuffle:
                # make sure tests are repeatable, no directory-shuffling
                mock_shuffle.side_effect = lambda l: l.sort()
                mock_scratch.return_value = ITEST_SCRATCH
                with context:
                    main(BACKUP_ARGS)
                test_case(*args, **kwargs)

        return wrapper
    return decorator
