import os
import time
from hashlib import sha256

import mock
import pytest

from backuppy.exceptions import DoubleBufferError
from backuppy.exceptions import FileChangedException
from backuppy.io import compute_sha
from backuppy.io import io_copy
from backuppy.io import IOIter


@pytest.fixture
def mock_io_iter(fs):
    yield IOIter('/foo', block_size=2)


@pytest.fixture
def foo_contents(fs):
    contents = b'asdfhjklqwerty'
    with open('/foo', 'wb') as f:
        f.write(contents)
    yield contents


def test_tmp_io_iter(fs):
    with mock.patch('backuppy.io.TemporaryFile') as mock_tmp_file, IOIter() as tmp:
        tmp._check_mtime()
        assert mock_tmp_file.call_count == 1
        with pytest.raises(BufferError):
            tmp.stat()


def test_context_manager(mock_io_iter):
    with mock.patch('backuppy.io.TemporaryFile') as mock_tmp_file, mock_io_iter:
        assert mock_tmp_file.call_count == 0
        assert os.path.exists('/foo')


def test_context_manager_twice(mock_io_iter):
    with pytest.raises(DoubleBufferError), mock_io_iter, mock_io_iter:
        pass


def test_reader_not_open(mock_io_iter):
    with pytest.raises(BufferError):
        next(mock_io_iter.reader())


@pytest.mark.parametrize('reset_pos', [True, False])
@pytest.mark.parametrize('end', [None, 5])
def test_reader_contents(mock_io_iter, foo_contents, reset_pos, end):
    with mock_io_iter:
        for i, data in enumerate(mock_io_iter.reader(end=end, reset_pos=reset_pos)):
            start_pos = i * 2
            end_pos = start_pos + 2
            if end and end_pos > end:
                end_pos = end
            assert data == foo_contents[start_pos:end_pos]
        remainder = len(foo_contents) - (end if end else len(foo_contents))
        assert len(mock_io_iter.fd.read()) == (len(foo_contents) if reset_pos else remainder)
        sha_fn = sha256()
        sha_fn.update(foo_contents[:(end if end else len(foo_contents))])
        assert mock_io_iter.sha() == sha_fn.hexdigest()


def test_writer_not_open(mock_io_iter):
    with pytest.raises(BufferError):
        next(mock_io_iter.writer())


def test_writer(mock_io_iter):
    with open('/foo', 'wb') as f:
        f.write(b'This data will get overwritten')
    contents = b'asdfhjlkqwerty'
    with mock_io_iter:
        writer = mock_io_iter.writer(); next(writer)
        writer.send(contents)
    with open('/foo', 'rb') as f:
        assert f.read() == contents
    sha_fn = sha256()
    sha_fn.update(contents)
    assert mock_io_iter.sha() == sha_fn.hexdigest()


def test_no_sha_computed(mock_io_iter):
    with pytest.raises(BufferError):
        mock_io_iter.sha()


def test_check_mtime_no_mtime(mock_io_iter):
    with pytest.raises(BufferError):
        mock_io_iter._check_mtime()


def test_check_mtime(mock_io_iter):
    with mock_io_iter:
        time.sleep(1)
        with open('/foo', 'wb') as f, pytest.raises(FileChangedException):
            f.write(b'asdf')
            mock_io_iter._check_mtime()


def test_compute_sha(mock_io_iter, foo_contents):
    sha_fn = sha256()
    sha_fn.update(foo_contents)
    with mock_io_iter:
        assert compute_sha(mock_io_iter) == sha_fn.hexdigest()


def test_copy(mock_io_iter, foo_contents):
    with mock_io_iter, IOIter('/bar') as copy:
        io_copy(mock_io_iter, copy)
    with open('/bar', 'rb') as f:
        assert f.read() == foo_contents
