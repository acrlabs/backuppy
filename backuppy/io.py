import _hashlib  # for typing
import io
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
BLOCK_SIZE = (1 << 30)  # 1GB block size
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
            dirname = os.path.dirname(self.filename)
            if dirname:
                os.makedirs(dirname, exist_ok=True)
            fd = os.open(self.filename, os.O_CREAT | os.O_RDWR | O_BINARY, mode=0o600)
            self._fd = os.fdopen(fd, 'r+b')
            self._enter_mtime = self.mtime
        else:
            self._fd = io.BytesIO()
        self.fd.seek(0)
        return self

    def __exit__(self, type, value, traceback):
        """ Context manager cleanup; closes the file """
        self._fd.close()
        self._fd = None
        self._enter_mtime = None

    def reader(self) -> Generator[bytes, None, None]:
        """ Iterator for reading the contents of a file

        This generator will fail with a BufferError unless inside a with IOIter(...) block

        :returns: data for the file in self.block_size chunks
        """
        self.fd.seek(0)
        self._sha_fn = sha256()
        while True:
            self._check_mtime()
            # On Windows, even if the file is much smaller, it appears to allocate space for
            # the full requested_read_size on a read() call, so this min(...) makes sure that
            # we're not wasting time and memory doing that
            requested_read_size = min(self.block_size, self.size)

            data = self.fd.read(requested_read_size)
            logger.debug2(f'read {len(data)} bytes from {self.filename}')
            self._sha_fn.update(data)
            if not data:
                break
            yield data
        self.fd.seek(0)

    def writer(self) -> Generator[None, bytes, None]:
        """ Iterator for writing to a file; the file is truncated to 0 bytes first

        This generator will fail with a BufferError unless inside a with IOIter(...) block
        """
        self.fd.truncate()
        self._sha_fn = sha256()
        total_bytes_written = 0
        while True:
            data = yield
            self._sha_fn.update(data)
            bytes_written = self.fd.write(data)
            total_bytes_written += bytes_written

            # If we're a temporary (in-memory) file and we've exceeded our memory limits,
            # dump the file contents out to disk before continuing
            if (
                not self.filename
                and total_bytes_written >= self.block_size
                and isinstance(self.fd, io.BytesIO)
            ):
                logger.debug2('overflowed memory limits, caching to disk')
                temp_fd = TemporaryFile(self._mode)
                self.fd.seek(0)
                temp_fd.write(self.fd.read())
                self._fd = temp_fd

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
    def size(self) -> int:
        try:
            return os.fstat(self.fd.fileno()).st_size
        except io.UnsupportedOperation:
            return len(self.fd.getbuffer())

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
