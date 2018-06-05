from hashlib import sha256

BLOCK_SIZE = 32  # (1 << 16)


class ReadSha:
    def __init__(self, filename):
        self.filename = filename
        self._sha = sha256()
        self._fd = None

    @property
    def hexdigest(self):
        return self._sha.hexdigest()

    def read(self, size=BLOCK_SIZE):
        data = self._fd.read(size)
        self._sha.update(data)
        return data

    def __enter__(self):
        self._fd = open(self.filename, 'rb')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._fd.close()
        return False
