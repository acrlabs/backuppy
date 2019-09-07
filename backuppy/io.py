import _hashlib  # for typing
import os
from hashlib import sha256
from tempfile import TemporaryFile
from typing import Generator
from typing import IO
from typing import Optional

import colorlog

from backuppy.exceptions import DoubleBufferError
from backuppy.exceptions import FileChangedException

logger = colorlog.getLogger(__name__)
BLOCK_SIZE = (1 << 16)
O_BINARY = getattr(os, 'O_BINARY', 0x0)  # O_BINARY only available on windows


class IOIter:
    """ Wrapper around file IO operations that automates SHA computations """

    def __init__(
        self,
        filename: Optional[str] = None,
        block_size: int = BLOCK_SIZE,
        check_mtime: bool = True,
    ) -> None:
        """
        :param filename: the name of the file to operate on; if None, the IOIter will wrap a
            TemporaryFile
        :param block_size: how much data to read or write at a time
        :param check_mtime: watch the mtime of the file to see if it's changed while open
        """
        self.filename = filename
        self.block_size = block_size
        self._fd: Optional[IO[bytes]] = None
        self._mode = 'r+b'
        self._sha_fn: Optional[_hashlib.HASH] = None
        self._should_check_mtime = check_mtime
        self._enter_mtime: Optional[int] = None

    def __enter__(self) -> 'IOIter':
        """ Context manager function to open the file (the file will be created if it doesn't exist)

        :returns: self, as a convenience
        :raises: DoubleBufferError if you try to nest the context managers
        """
        if self._fd:
            raise DoubleBufferError(f'Buffer for {self.filename} is open twice')
        if self.filename:
            os.makedirs(os.path.dirname(self.filename), exist_ok=True)
            fd = os.open(self.filename, os.O_CREAT | os.O_RDWR | O_BINARY, mode=0o600)
            self._fd = os.fdopen(fd, 'r+b')
            self._enter_mtime = self.mtime
        else:
            self._fd = TemporaryFile(self._mode)
        self.fd.seek(0)
        return self

    def __exit__(self, type, value, traceback):
        """ Context manager cleanup; closes the file """
        self._fd.close()
        self._fd = None
        self._enter_mtime = None

    def reader(
        self,
        end: Optional[int] = None,
        reset_pos: bool = True,
    ) -> Generator[bytes, None, None]:
        """ Iterator for reading the contents of a file

        This generator will fail with a BufferError unless inside a with IOIter(...) block

        :param end: the ending position to read until
        :param reset_pos: True to seek to the beginning of the file first, false otherwise
        :returns: data for the file in self.block_size chunks
        """
        if reset_pos:
            self.fd.seek(0)
        self._sha_fn = sha256()
        while True:
            self._check_mtime()
            requested_read_size = self.block_size
            if end is not None and end - self.fd.tell() < requested_read_size:
                requested_read_size = end - self.fd.tell()
            data = self.fd.read(requested_read_size)
            logger.debug2(f'read {len(data)} bytes from {self.filename}')
            self._sha_fn.update(data)
            if not data:
                break
            yield data
        if reset_pos:
            self.fd.seek(0)

    def writer(self) -> Generator[None, bytes, None]:
        """ Iterator for writing to a file; the file is truncated to 0 bytes first

        This generator will fail with a BufferError unless inside a with IOIter(...) block
        """
        self.fd.truncate()
        self._sha_fn = sha256()
        while True:
            data = yield
            self._sha_fn.update(data)
            bytes_written = self.fd.write(data)
            logger.debug2(f'wrote {bytes_written} bytes to {self.filename}')
            self.fd.flush()

    def sha(self) -> str:
        """ Return the sha of all data that has been read or written to the file """
        if not self._sha_fn:
            raise BufferError('No SHA has been computed')
        return self._sha_fn.hexdigest()

    def _check_mtime(self) -> None:
        """ Check to see if the file has been modified during writing (this isn't guaranteed to
        be correct, because there are other ways to set the mtime to make it look like the file
        hasn't been modified, but it should be fine for most things).

        :raises BufferError: if we're not inside a with IOIter(...) block
        :raises FileChangedException: if the mtime has changed since we entered the context-managed
            block
        """
        if self.filename and self._should_check_mtime:
            if not self._enter_mtime:
                raise BufferError(
                    f"{self.filename} is missing an mtime; probably it hasn't been opened")
            if self.mtime != self._enter_mtime:
                raise FileChangedException(
                    f'{self.filename} changed while reading; {self.mtime} != {self._enter_mtime}')

    @property
    def uid(self) -> int:
        if self.filename:
            return os.stat(self.filename).st_uid
        else:
            raise BufferError('No stat for temporary file')

    @property
    def gid(self) -> int:
        if self.filename:
            return os.stat(self.filename).st_gid
        else:
            raise BufferError('No stat for temporary file')

    @property
    def mode(self) -> int:
        if self.filename:
            return os.stat(self.filename).st_mode
        else:
            raise BufferError('No stat for temporary file')

    @property
    def mtime(self) -> int:
        if self.filename:
            return int(os.stat(self.filename).st_mtime)
        else:
            raise BufferError('No stat for temporary file')

    @property
    def fd(self):
        """ Wrapper around the file object

        :raises BufferError: if the file object is accessed outside a with IOIter(...) block
        """
        if not self._fd:
            raise BufferError('No file is open')
        return self._fd


def compute_sha(file1: IOIter) -> str:
    """ Helper function for computing the sha of an IOIter; just reads the data and discards it

    :returns: the sha256sum of the file
    """
    for data in file1.reader():
        pass
    return file1.sha()


def io_copy(file1: IOIter, file2: IOIter) -> str:
    """ Helper function to copy data from one IOIter to another

    :returns: the sha256sum of the copied data
    """
    writer = file2.writer(); next(writer)
    for data in file1.reader():
        writer.send(data)
    return file2.sha()
