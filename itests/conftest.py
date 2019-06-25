import inspect
import os
import re
import sys
import traceback
from contextlib import ExitStack
from hashlib import sha256
from shutil import rmtree

import pytest

from backuppy.run import main
from backuppy.stores.backup_store import MANIFEST_PATH

ITEST_ROOT = 'itests'
DATA_DIR = os.path.join(ITEST_ROOT, 'data')
BACKUP_DIR = os.path.join(ITEST_ROOT, 'backup')
ITEST_MANIFEST_PATH = os.path.join(BACKUP_DIR, MANIFEST_PATH)
BACKUP_ARGS = [
    '--log-level', 'debug',
    '--disable-compression',
    '--disable-encryption',
    'backup',
    '--config', os.path.join(ITEST_ROOT, 'itest.conf'),
]


def compute_sha(string):
    sha_fn = sha256()
    sha_fn.update(string)
    return sha_fn.hexdigest()


@pytest.fixture(autouse=True, scope='module')
def initialize():
    try:
        rmtree(DATA_DIR)
    except FileNotFoundError:
        pass
    try:
        rmtree(BACKUP_DIR)
    except FileNotFoundError:
        pass

    os.makedirs(DATA_DIR)
    os.makedirs(BACKUP_DIR)


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


def make_trace_func(search_string, side_effect):
    def trace_func(frame, event, arg):
        if event == 'call':
            try:
                module = inspect.getmodule(frame)
            except TypeError:
                return None
            if module and not module.__name__.startswith('backuppy'):
                frame.f_trace_lines = False
            return trace_func

        elif event == 'line':
            line = traceback.extract_stack(frame, limit=1)[0].line
            m = re.search(f'#\s+{search_string}', line)
            if m:
                side_effect()
    return trace_func


def backup_itest_wrapper(
    test_file_history,
    *dec_args,
    trace=None,
):
    def decorator(test_case):
        def wrapper(*args, **kwargs):
            context = trace[1] if trace and trace[1] else ExitStack()
            for tfd in dec_args:
                if tfd.path in test_file_history and tfd != test_file_history[tfd.path][-1]:
                    test_file_history[tfd.path].append(tfd)
                    tfd.write()
                elif tfd.path not in test_file_history:
                    test_file_history[tfd.path] = [tfd]
                    tfd.write()

            if trace:
                sys.settrace(make_trace_func(test_case.__name__, trace[0]))
            with context:
                main(BACKUP_ARGS)
            test_case(*args, **kwargs)

        return wrapper
    return decorator
