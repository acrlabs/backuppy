from hashlib import sha256
from io import BytesIO

import mock
import pytest

from backuppy.blob import _copy
from backuppy.blob import apply_diff
from backuppy.blob import compute_diff
from backuppy.exceptions import DiffParseError


@pytest.fixture(autouse=True)
def mock_block_size():
    with mock.patch('backuppy.blob.BLOCK_SIZE', 2):
        yield


@pytest.fixture
def mock_open_streams():
    orig = BytesIO(b'asdfasdfa')
    '@3|I4|hjkl'
    orig.fileno = mock.Mock(return_value=1234)
    with mock.patch('backuppy.blob.os.stat') as mock_stat:
        mock_stat.return_value.st_size = len(orig.getvalue())
        yield orig, BytesIO(), BytesIO()


@pytest.fixture
def sha_fn():
    return sha256()


def test_copy(mock_open_streams):
    orig, new, _ = mock_open_streams
    _copy(orig, new)
    assert new.getvalue() == orig.getvalue()


def test_copy_to_pos(mock_open_streams):
    orig, new, _ = mock_open_streams
    _copy(orig, new, 5)
    assert new.getvalue() == orig.getvalue()[:5]


def test_copy_off_the_end(mock_open_streams):
    orig, new, _ = mock_open_streams
    with pytest.raises(DiffParseError):
        _copy(orig, new, 100)


def test_copy_to_pos_with_offset(mock_open_streams):
    orig, new, _ = mock_open_streams
    _copy(orig, new, 5, 2)
    assert new.getvalue() == orig.getvalue()[:3]


def test_apply_parse_error_1(mock_open_streams):
    orig, diff, new = mock_open_streams
    diff.write(b'asdf')
    with pytest.raises(DiffParseError) as e:
        apply_diff(orig, diff, new)
    assert 'Un-parseable diff' in str(e)


def test_apply_parse_error_2(mock_open_streams):
    orig, diff, new = mock_open_streams
    diff.write(b'@3|q4|asdf')
    with pytest.raises(DiffParseError) as e:
        apply_diff(orig, diff, new)
    assert 'Expected an action' in str(e)


def test_contents_length_error(mock_open_streams):
    orig, diff, new = mock_open_streams
    diff.write(b'@3|I7|xy')
    with pytest.raises(DiffParseError) as e:
        apply_diff(orig, diff, new)
    assert 'Un-parseable diff' in str(e)


def test_apply_del(mock_open_streams):
    orig, diff, new = mock_open_streams
    diff.write(b'@3|D2|')
    apply_diff(orig, diff, new)
    assert new.getvalue() == b'asdsdfa'


def test_apply_ins(mock_open_streams):
    orig, diff, new = mock_open_streams
    diff.write(b'@3|I2|xy')
    apply_diff(orig, diff, new)
    assert new.getvalue() == b'asdxyfasdfa'


def test_apply_replace(mock_open_streams):
    orig, diff, new = mock_open_streams
    diff.write(b'@3|X2|xy')
    apply_diff(orig, diff, new)
    assert new.getvalue() == b'asdxysdfa'


def test_compute_diff_eq(mock_open_streams, sha_fn):
    orig, new, diff = mock_open_streams
    new.write(b'asdfasdfa')
    sha_fn.update(new.getvalue())
    sha = compute_diff(orig, new, diff)
    assert diff.getvalue() == b''
    assert sha == sha_fn.hexdigest()


def test_compute_diff_del(mock_open_streams, sha_fn):
    orig, new, diff = mock_open_streams
    new.write(b'dfasdfa')
    sha_fn.update(new.getvalue())
    sha = compute_diff(orig, new, diff)
    assert diff.getvalue() == b'@0|D2|\n'
    assert sha == sha_fn.hexdigest()


def test_compute_diff_ins(mock_open_streams, sha_fn):
    orig, new, diff = mock_open_streams
    new.write(b'asasdfasdfa')
    sha_fn.update(new.getvalue())
    sha = compute_diff(orig, new, diff)
    assert diff.getvalue() == b'@2|I2|as\n'
    assert sha == sha_fn.hexdigest()


def test_compute_diff_repl(mock_open_streams, sha_fn):
    orig, new, diff = mock_open_streams
    new.write(b'asxyasdfa')
    sha_fn.update(new.getvalue())
    sha = compute_diff(orig, new, diff)
    assert diff.getvalue() == b'@2|X2|xy\n'
    assert sha == sha_fn.hexdigest()


def test_compute_diff_orig_long(mock_open_streams, sha_fn):
    orig, new, diff = mock_open_streams
    new.write(b'a')
    sha_fn.update(new.getvalue())
    with mock.patch('backuppy.io.BLOCK_SIZE', 2):
        sha = compute_diff(orig, new, diff)
    assert diff.getvalue() == b'@1|D1|\n@2|D2|\n@4|D2|\n@6|D2|\n@8|D1|\n'
    assert sha == sha_fn.hexdigest()


def test_compute_diff_new_long(mock_open_streams, sha_fn):
    orig, new, diff = mock_open_streams
    new.write(b'asdfasdfasdfa')
    sha_fn.update(new.getvalue())
    with mock.patch('backuppy.io.BLOCK_SIZE', 2):
        sha = compute_diff(orig, new, diff)
    assert diff.getvalue() == b'@9|I1|s\n@10|I2|df\n@12|I1|a\n'
    assert sha == sha_fn.hexdigest()
