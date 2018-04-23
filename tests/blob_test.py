import mock
import pytest

from backuppy.blob import _find_sep_index
from backuppy.blob import apply_diff
from backuppy.blob import compute_diff
from backuppy.exceptions import DiffParseError


CONTENTS = b'asdfasdf'


@pytest.fixture
def mock_edlib():
    with mock.patch('backuppy.blob.edlib') as m:
        yield m


def test_find_sep_index():
    assert _find_sep_index(b'a|sdf|asdf', 3) == 5


def test_find_sep_index_not_found():
    with pytest.raises(DiffParseError):
        _find_sep_index(b'a|sdfasdf', 3)


def test_apply_parse_error_1():
    with pytest.raises(DiffParseError):
        apply_diff(b'asdf', CONTENTS)


def test_apply_parse_error_2():
    with pytest.raises(DiffParseError):
        apply_diff(b'@3|q4|asdf', CONTENTS)


def test_contents_length_error():
    with pytest.raises(DiffParseError):
        apply_diff(b'@3|+7|xy', CONTENTS)


def test_apply_del():
    assert bytes(apply_diff(b'@3|-2|', CONTENTS)) == b'asdsdf'


def test_apply_ins():
    assert bytes(apply_diff(b'@3|+2|xy', CONTENTS)) == b'asdxyfasdf'


def test_apply_replace():
    assert bytes(apply_diff(b'@3|!2|xy', CONTENTS)) == b'asdxysdf'


def test_compute_diff_eq(mock_edlib):
    mock_edlib.align.return_value = {'cigar': '8='}
    assert compute_diff('foo', CONTENTS) == b''


def test_compute_diff_del(mock_edlib):
    mock_edlib.align.return_value = {'cigar': '2D'}
    assert compute_diff('foo', CONTENTS) == b'@0|-2|\n'


def test_compute_diff_ins(mock_edlib):
    mock_edlib.align.return_value = {'cigar': '2I'}
    assert compute_diff('foo', CONTENTS) == b'@0|+2|as\n'


def test_compute_diff_repl(mock_edlib):
    mock_edlib.align.return_value = {'cigar': '2X'}
    assert compute_diff('foo', CONTENTS) == b'@0|!2|as\n'
