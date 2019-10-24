from hashlib import sha256

import pytest

from backuppy.blob import apply_diff
from backuppy.blob import compute_diff
from backuppy.exceptions import DiffParseError
from backuppy.exceptions import DiffTooLargeException


@pytest.fixture
def sha_fn():
    return sha256()


def test_apply_parse_error_1(mock_open_streams):
    orig, new, diff = mock_open_streams
    diff._fd.write(b'asdf')
    with pytest.raises(DiffParseError) as e:
        apply_diff(orig, diff, new)
    assert 'Un-parseable diff' in str(e.value)


def test_round_trip(mock_open_streams, sha_fn):
    new_contents = b'asdfrfsdcac'
    orig, new, diff = mock_open_streams
    new._fd.write(new_contents)
    sha_fn.update(new_contents)
    compute_diff(orig, new, diff)
    assert new.sha() == sha_fn.hexdigest()

    new._fd.seek(0)
    new._fd.write(b'')
    apply_diff(orig, diff, new)
    new._fd.seek(0)
    assert new._fd.read() == new_contents


def test_compute_diff_with_large_diff(mock_open_streams):
    orig, new, diff = mock_open_streams
    new._fd.write(b'asdfasdfasdfa')
    new._fd.seek(0)
    with pytest.raises(DiffTooLargeException):
        compute_diff(orig, new, diff, 0.5)
