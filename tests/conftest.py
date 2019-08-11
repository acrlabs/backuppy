from io import BytesIO

import mock
import pytest
import staticconf.testing

from backuppy.io import IOIter
from backuppy.run import setup_logging


def _make_stat_fn(io_iter):
    io_iter.stat = lambda: mock.Mock(
        st_size=len(io_iter._fd.getvalue()),
        st_mtime=1,
    )


@pytest.fixture(autouse=True, scope='session')
def setup_logging_for_tests():
    setup_logging('debug2')


@pytest.fixture(autouse=True)
def test_config_file():
    config = {
        'backups': {
            'backup1': {
                'directories': ['/path/0'],
                'exclusions': ['dont_back_this_up', 'foo'],
            },
            'backup2': {
                'directories': ['/path/1', '/path/2'],
                'exclusions': ['dont_back_this_up', 'bar']
            },
        }
    }
    with staticconf.testing.PatchConfiguration(config, flatten=False), \
            staticconf.testing.PatchConfiguration(
                config['backups']['backup1'],
                namespace='backup1'), \
            staticconf.testing.PatchConfiguration(
                config['backups']['backup2'],
                namespace='backup2'):
        yield


@pytest.fixture
def mock_open_streams():
    orig, new, diff = IOIter('/orig'), IOIter('/new'), IOIter('/diff')
    with mock.patch('builtins.open'), \
            mock.patch('backuppy.io.os.open'), \
            mock.patch('backuppy.io.os.fdopen'), \
            mock.patch('backuppy.io.os.stat'), \
            mock.patch('backuppy.io.os.makedirs'), \
            orig, new, diff:
        orig.block_size = new.block_size = diff.block_size = 2
        orig._fd = BytesIO(b'asdfasdfa')
        new._fd = BytesIO()
        diff._fd = BytesIO()
        _make_stat_fn(orig)
        _make_stat_fn(new)
        _make_stat_fn(diff)
        yield orig, new, diff


def count_matching_log_lines(msg, caplog):
    return len([r for r in caplog.records if msg in r.message])
