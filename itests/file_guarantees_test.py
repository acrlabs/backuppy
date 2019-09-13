import argparse
import os
import sys
import time

import pytest

from backuppy.cli.backup import main as backup
from backuppy.util import sha_to_path
from itests.conftest import _TestFileData
from itests.conftest import BACKUP_DIR
from itests.conftest import DATA_DIRS
from itests.conftest import ITEST_CONFIG
from itests.conftest import itest_setup
from itests.conftest import ItestException
from itests.conftest import make_trace_func

test_file_history = dict()  # type: ignore
DATA_DIR = DATA_DIRS[0]
BACKUP_ARGS = argparse.Namespace(
    log_level='debug',
    config=ITEST_CONFIG,
    preserve_scratch_dir=False,
    dry_run=False,
)


def abort():
    raise ItestException('abort')


def make_modify_file_func(filename):
    def modify_file():
        time.sleep(1)
        with open(filename, 'w') as f:
            f.write('NEW CONTENTS')
    return modify_file


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


def test_f1_crash_file_save():
    sys.settrace(make_trace_func('test_f1_crash_file_save', abort))
    with itest_setup(
        test_file_history,
        _TestFileData('foo', 'asdfhjkl'),
        _TestFileData('new_file', 'zxcv'),
    ):
        backup(BACKUP_ARGS)

    first_file_data_path = os.path.join(DATA_DIR, 'foo')
    second_file_data_path = os.path.join(DATA_DIR, 'new_file')
    first_file_backup_path = os.path.join(
        BACKUP_DIR,
        sha_to_path(test_file_history[first_file_data_path][1].sha),
    )
    second_file_backup_path = os.path.join(
        BACKUP_DIR,
        sha_to_path(test_file_history[second_file_data_path][0].sha),
    )
    assert not os.path.exists(first_file_backup_path)
    assert os.path.exists(second_file_backup_path)

    # reset everything back to "normal" before the next test
    backup(BACKUP_ARGS)


def test_f2_lbs_atomicity_1():
    sys.settrace(make_trace_func('test_f2_lbs_atomicity_1', abort))
    with itest_setup(
        test_file_history,
        _TestFileData('new_file_2', '1234'),
    ):
        backup(BACKUP_ARGS)
    file_data_path = os.path.join(DATA_DIR, 'new_file_2')
    file_backup_path = os.path.join(
        BACKUP_DIR,
        sha_to_path(test_file_history[file_data_path][0].sha),
    )
    assert not os.path.exists(file_backup_path)


def test_f2_lbs_atomicity_2():
    sys.settrace(make_trace_func('test_f2_lbs_atomicity_2', abort))
    with itest_setup(
        test_file_history,
    ):
        backup(BACKUP_ARGS)
    file_data_path = os.path.join(DATA_DIR, 'new_file_2')
    file_backup_path = os.path.join(
        BACKUP_DIR,
        sha_to_path(test_file_history[file_data_path][0].sha),
    )
    assert os.path.exists(file_backup_path)


def test_f3_file_changed_while_saving():
    sys.settrace(make_trace_func(
        'test_f3_file_changed_while_saving',
        make_modify_file_func(os.path.join(DATA_DIR, 'new_file_3')),
    ))
    with itest_setup(
        test_file_history,
        _TestFileData('new_file_3', '12345'),
    ):
        backup(BACKUP_ARGS)
    file_data_path = os.path.join(DATA_DIR, 'new_file_3')
    file_backup_path = os.path.join(
        BACKUP_DIR,
        sha_to_path(test_file_history[file_data_path][0].sha),
    )
    assert not os.path.exists(file_backup_path)
