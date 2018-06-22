from typing import IO

BLOCK_SIZE = 32  # (1 << 16)


class IOIter:
    def __init__(self, fd: IO[bytes], chain=None, side_effects=None) -> None:
        self._fd = fd
        self._side_effects = side_effects or []
        self._chain = chain or []

    def __iter__(self):
        self._fd.seek(0)
        while True:
            data = self._fd.read(BLOCK_SIZE)
            if not data:
                break
            for fn in self._side_effects:
                fn(data)
            for fn in self._chain:
                data = fn(data)
            yield data
