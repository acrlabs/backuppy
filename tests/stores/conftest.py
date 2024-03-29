import os

import pytest


@pytest.fixture(autouse=True)
def fake_filesystem(fs):
    fs.pause()
    # boto (and possibly other stuff) needs to be able to read stuff in the real filesystem
    if os.path.exists(os.getenv("VIRTUAL_ENV")):
        fs.add_real_directory(os.getenv("VIRTUAL_ENV"))
    fs.resume()

    fs.create_file('/scratch/foo', contents="i'm a copy of foo")
    fs.create_file('/scratch/asdf/bar', contents="i'm a copy of bar")
    fs.create_file('/fake/path/fake_backup/foo', contents='old boring content')
    fs.create_file('/fake/path/fake_backup/biz/baz', contents='old boring content 2')
    fs.create_file('/fake/path/fake_backup/fuzz/buzz', contents='old boring content 3')
