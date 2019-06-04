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
    def __init__(
        self,
        filename: Optional[str] = None,
        block_size: int = BLOCK_SIZE,
        check_mtime: bool = True,
    ) -> None:
        self.filename = filename
        self.block_size = block_size
        self._fd: Optional[IO[bytes]] = None
        self._mode = 'r+b'
        self._sha_fn: Optional[_hashlib.HASH] = None
        self._should_check_mtime = check_mtime
        self._mtime: Optional[int] = None

    def reader(self, end: Optional[int] = None, reset_pos: bool = True) -> Generator[bytes, None, None]:
        if reset_pos:
            self.fd.seek(0)
        self._sha_fn = sha256()
        while True:
            self._check_mtime()
            requested_read_size = self.block_size
            if end and end - self.fd.tell() < requested_read_size:
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
        self.fd.truncate()
        self._sha_fn = sha256()
        while True:
            data = yield
            self._sha_fn.update(data)
            bytes_written = self.fd.write(data)
            logger.debug2(f'wrote {bytes_written} bytes to {self.filename}')
            self.fd.flush()

    def __enter__(self) -> 'IOIter':
        if self._fd:
            raise DoubleBufferError(f'Buffer for {self.filename} is open twice')
        if self.filename:
            fd = os.open(self.filename, os.O_CREAT | os.O_RDWR | O_BINARY, mode=0o600)
            self._fd = os.fdopen(fd, 'r+b')
            self._mtime = int(self.stat().st_mtime)
        else:
            self._fd = TemporaryFile(self._mode)
        self.fd.seek(0)
        return self

    def __exit__(self, type, value, traceback):
        self._fd.close()
        self._fd = None
        self._mtime = None

    def sha(self) -> str:
        if not self._sha_fn:
            raise BufferError('No SHA has been computed')
        return self._sha_fn.hexdigest()

    def stat(self) -> os.stat_result:
        if self.filename:
            return os.stat(self.filename)
        else:
            raise BufferError('No stat for temporary file')

    def _check_mtime(self) -> None:
        if self.filename and self._should_check_mtime:
            if not self._mtime:
                raise BufferError(f"{self.filename} is missing an mtime; probably it hasn't been opened")
            mtime = int(self.stat().st_mtime)
            if mtime != self._mtime:
                raise FileChangedException(f'{self.filename} changed while reading; {mtime} != {self._mtime}')

    @property
    def fd(self):
        if not self._fd:
            raise BufferError('No file is open')
        return self._fd


def compute_sha(file1: IOIter) -> str:
    for data in file1.reader():
        pass
    return file1.sha()


def io_copy(file1: IOIter, file2: IOIter) -> str:
    writer = file2.writer(); next(writer)
    for data in file1.reader():
        writer.send(data)
    return file2.sha()
