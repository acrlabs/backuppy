import os
import shutil

import pytest


@pytest.fixture(autouse=True)
def fake_filesystem():
    try:
        shutil.rmtree("/fake/path/fake_backup")
        shutil.rmtree("/scratch")
    except:  # noqa
        pass

    os.makedirs("/scratch/asdf")
    os.makedirs("/fake/path/fake_backup/biz")
    os.makedirs("/fake/path/fake_backup/fuzz")

    with open("/scratch/foo", "w") as f:
        f.write("i'm a copy of foo")
    with open("/scratch/asdf/bar", "w") as f:
        f.write("i'm a copy of bar")
    with open("/fake/path/fake_backup/foo", "w") as f:
        f.write("old boring content")
    with open("/fake/path/fake_backup/biz/baz", "w") as f:
        f.write("old boring content 2")
    with open("/fake/path/fake_backup/fuzz/buzz", "w") as f:
        f.write("old boring content 3")
