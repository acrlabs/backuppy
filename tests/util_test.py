import re
from pathlib import Path

from backuppy.util import file_walker

# def test_ask_for_confirmation():


def tetst_file_walker(fs_path):
    Path(f"{fs_path}/foo").touch()
    Path(f"{fs_path}/bar").touch()
    Path(f"{fs_path}/skip/baz").touch()
    Path(f"{fs_path}/skip/dip").touch()
    Path(f"{fs_path}/fizz/buzz").touch()
    Path(f"{fs_path}/fizz/skip2").touch()
    results = {f for f in file_walker(fs_path, exclusions=[re.compile("skip")])}
    assert results == {f"{fs_path}/foo", f"{fs_path}/bar", f"{fs_path}/fizz/buzz"}
