import pytest
import staticconf.testing


@pytest.fixture(autouse=True)
def test_config_file():
    with staticconf.testing.PatchConfiguration({
        'exclusions': ['dont_back_this_up'],
        'backups': {
            'backup1': {
                'directories': ['/path/0'],
                'exclusions': ['foo'],
            },
            'backup2': {
                'directories': ['/path/1', '/path/2'],
                'exclusions': ['bar']
            },
        }
    }, flatten=False):
        yield
