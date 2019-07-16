import os
import time

import pytest

from backuppy.util import sha_to_path
from itests.conftest import _TestFileData
from itests.conftest import BACKUP_DIR
from itests.conftest import backup_itest_wrapper
from itests.conftest import DATA_DIRS
from itests.conftest import ItestException

test_file_history = dict()  # type: ignore
DATA_DIR = DATA_DIRS[0]


def abort():
    raise ItestException('abort')


def make_modify_file_func(filename):
    def modify_file():
        time.sleep(1)
        with open(filename, 'w') as f:
            f.write('NEW CONTENTS')
    return modify_file


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
    _TestFileData('new_file', 'zxcv'),
    side_effect=(abort, None)
)
def test_f1_crash_file_save():
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


@backup_itest_wrapper(
    test_file_history,
)
def test_reset_back_to_clean_state():
    pass  # not a real test, just saving the changes to 'foo' before running the next one


@backup_itest_wrapper(
    test_file_history,
    _TestFileData('new_file_2', '1234'),
    side_effect=(abort, None),
)
def test_f2_lbs_atomicity_1():
    file_data_path = os.path.join(DATA_DIR, 'new_file_2')
    file_backup_path = os.path.join(
        BACKUP_DIR,
        sha_to_path(test_file_history[file_data_path][0].sha),
    )
    assert not os.path.exists(file_backup_path)


@backup_itest_wrapper(
    test_file_history,
    side_effect=(abort, None),
)
def test_f2_lbs_atomicity_2():
    file_data_path = os.path.join(DATA_DIR, 'new_file_2')
    file_backup_path = os.path.join(
        BACKUP_DIR,
        sha_to_path(test_file_history[file_data_path][0].sha),
    )
    assert os.path.exists(file_backup_path)


@backup_itest_wrapper(
    test_file_history,
    _TestFileData('new_file_3', '12345'),
    side_effect=(make_modify_file_func(os.path.join(DATA_DIR, 'new_file_3')), None),
)
def test_f3_file_changed_while_saving():
    file_data_path = os.path.join(DATA_DIR, 'new_file_3')
    file_backup_path = os.path.join(
        BACKUP_DIR,
        sha_to_path(test_file_history[file_data_path][0].sha),
    )
    assert not os.path.exists(file_backup_path)


@backup_itest_wrapper(
    test_file_history,
    _TestFileData('new_file_2', 'asdfhjkl'),
)
def test_f4_file_not_overwritten():
    file_data_path = os.path.join(DATA_DIR, 'new_file_2')
    file_backup_path = os.path.join(
        BACKUP_DIR,
        sha_to_path(test_file_history[file_data_path][1].sha),
    )
    with open(file_backup_path) as f:
        assert f.read() == '@4|I4|hjkl'
