from hashlib import sha256

import pytest

from backuppy.blob import apply_diff
from backuppy.blob import compute_sha_and_diff
from backuppy.exceptions import DiffParseError


@pytest.fixture
def sha_fn():
    return sha256()


def test_apply_parse_error_1(mock_open_streams):
    orig, new, diff = mock_open_streams
    diff._fd.write(b'asdf')
    with pytest.raises(DiffParseError) as e:
        apply_diff(orig, diff, new)
    assert 'Un-parseable diff' in str(e)


def test_apply_parse_error_2(mock_open_streams):
    orig, new, diff = mock_open_streams
    diff._fd.write(b'@3|q4|asdf')
    with pytest.raises(DiffParseError) as e:
        apply_diff(orig, diff, new)
    assert 'Expected an action' in str(e)


def test_contents_length_error(mock_open_streams):
    orig, new, diff = mock_open_streams
    diff._fd.write(b'@3|I7|xy')
    with pytest.raises(DiffParseError) as e:
        apply_diff(orig, diff, new)
    assert 'Un-parseable diff' in str(e)


def test_apply_del(mock_open_streams):
    orig, new, diff = mock_open_streams
    diff._fd.write(b'@3|D2|')
    apply_diff(orig, diff, new)
    assert new._fd.getvalue() == b'asdsdfa'


def test_apply_ins(mock_open_streams):
    orig, new, diff = mock_open_streams
    diff._fd.write(b'@3|I2|xy')
    apply_diff(orig, diff, new)
    assert new._fd.getvalue() == b'asdxyfasdfa'


def test_apply_replace(mock_open_streams):
    orig, new, diff = mock_open_streams
    diff._fd.write(b'@3|X2|xy')
    apply_diff(orig, diff, new)
    assert new._fd.getvalue() == b'asdxysdfa'


def test_compute_sha_and_diff_eq(mock_open_streams, sha_fn):
    orig, new, diff = mock_open_streams
    new._fd.write(b'asdfasdfa')
    new._fd.seek(0)
    sha_fn.update(new._fd.getvalue())
    sha, diff = compute_sha_and_diff(orig, new, diff)
    assert diff._fd.getvalue() == b''
    assert sha == sha_fn.hexdigest()


def test_compute_sha_and_diff_del(mock_open_streams, sha_fn):
    orig, new, diff = mock_open_streams
    orig.block_size = new.block_size = diff.block_size = 100
    new._fd.write(b'dfasdfa')
    new._fd.seek(0)
    sha_fn.update(new._fd.getvalue())
    sha, diff = compute_sha_and_diff(orig, new, diff)
    assert diff._fd.getvalue() == b'@0|D2|\n'
    assert sha == sha_fn.hexdigest()


def test_compute_sha_and_diff_ins(mock_open_streams, sha_fn):
    orig, new, diff = mock_open_streams
    orig.block_size = new.block_size = diff.block_size = 100
    new._fd.write(b'asasdfasdfa')
    new._fd.seek(0)
    sha_fn.update(new._fd.getvalue())
    sha, diff = compute_sha_and_diff(orig, new, diff)
    assert diff._fd.getvalue() == b'@2|I2|as\n'
    assert sha == sha_fn.hexdigest()


def test_compute_sha_and_diff_repl(mock_open_streams, sha_fn):
    orig, new, diff = mock_open_streams
    new._fd.write(b'asxyasdfa')
    new._fd.seek(0)
    sha_fn.update(new._fd.getvalue())
    sha, diff = compute_sha_and_diff(orig, new, diff)
    assert diff._fd.getvalue() == b'@2|X2|xy\n'
    assert sha == sha_fn.hexdigest()


def test_compute_sha_and_diff_orig_long(mock_open_streams, sha_fn):
    orig, new, diff = mock_open_streams
    new._fd.write(b'a')
    new._fd.seek(0)
    sha_fn.update(new._fd.getvalue())
    sha, diff = compute_sha_and_diff(orig, new, diff)
    assert diff._fd.getvalue() == b'@1|D1|\n@2|D2|\n@4|D2|\n@6|D2|\n@8|D1|\n'
    assert sha == sha_fn.hexdigest()


def test_compute_sha_and_diff_new_long(mock_open_streams, sha_fn):
    orig, new, diff = mock_open_streams
    new._fd.write(b'asdfasdfasdfa')
    new._fd.seek(0)
    sha_fn.update(new._fd.getvalue())
    sha, diff = compute_sha_and_diff(orig, new, diff)
    assert diff._fd.getvalue() == b'@9|I1|s\n@10|I2|df\n@12|I1|a\n'
    assert sha == sha_fn.hexdigest()
