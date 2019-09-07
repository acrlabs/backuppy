from io import BytesIO

import mock
import pytest
import staticconf.testing

from backuppy.io import IOIter
from backuppy.run import setup_logging


@pytest.fixture(autouse=True, scope='session')
def setup_logging_for_tests():
    setup_logging('debug2')


@pytest.fixture(autouse=True)
def test_config_file():
    config = {
        'backups': {
            'fake_backup1': {
                'directories': ['/path/0'],
                'exclusions': ['dont_back_this_up', 'foo'],
                'options': [],
                'private_key_filename': '/my/private/key',
            },
            'fake_backup2': {
                'directories': ['/path/1', '/path/2'],
                'exclusions': ['dont_back_this_up', 'bar'],
                'options': [],
                'private_key_filename': '/my/private/key',
            },
        },
    }
    with staticconf.testing.PatchConfiguration(config, flatten=False), \
            staticconf.testing.PatchConfiguration(
                config['backups']['fake_backup1'],
                namespace='fake_backup1'), \
            staticconf.testing.PatchConfiguration(
                config['backups']['fake_backup2'],
                namespace='fake_backup2'):
        yield


@pytest.fixture
def mock_open_streams():
    class MockBytesIO(BytesIO):
        def fileno(self):  # make this work with fstat
            return self

    orig, new, diff = IOIter('/orig'), IOIter('/new'), IOIter('/diff')
    with mock.patch('builtins.open'), \
            mock.patch('backuppy.io.os.open'), \
            mock.patch('backuppy.io.os.fdopen'), \
            mock.patch('backuppy.io.os.stat'), \
            mock.patch('backuppy.io.os.makedirs'), \
            mock.patch('os.fstat') as mock_fstat, \
            orig, new, diff:
        mock_fstat.side_effect = lambda bio: mock.Mock(st_size=len(bio.getvalue()))
        orig.block_size = new.block_size = diff.block_size = 2
        orig._fd = MockBytesIO(b'asdfasdfa')
        new._fd = MockBytesIO()
        diff._fd = MockBytesIO()
        yield orig, new, diff


def count_matching_log_lines(msg, caplog):
    return len([r for r in caplog.records if msg in r.message])
