import pytest

INITIAL_FILES = ['/a/dummy/file1', '/a/dummy/file2', '/b/dummy/file1']


@pytest.fixture(autouse=True)
def fake_filesystem(fs):
    for name in INITIAL_FILES:
        fs.CreateFile(name, contents=name)
    fs.CreateFile('/c/not/backed/up', contents='whatever')
