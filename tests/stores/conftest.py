import os

import pytest


@pytest.fixture(autouse=True)
def fake_filesystem(fs_path):
    os.makedirs(f"{fs_path}/scratch/asdf")
    os.makedirs(f"{fs_path}/fake/path/fake_backup/biz")
    os.makedirs(f"{fs_path}/fake/path/fake_backup/fuzz")

    with open(f"{fs_path}/scratch/foo", "w") as f:
        f.write("i'm a copy of foo")
    with open(f"{fs_path}/scratch/asdf/bar", "w") as f:
        f.write("i'm a copy of bar")
    with open(f"{fs_path}/fake/path/fake_backup/foo", "w") as f:
        f.write("old boring content")
    with open(f"{fs_path}/fake/path/fake_backup/biz/baz", "w") as f:
        f.write("old boring content 2")
    with open(f"{fs_path}/fake/path/fake_backup/fuzz/buzz", "w") as f:
        f.write("old boring content 3")
