import gzip
from hashlib import sha256

BLOCK_SIZE = 100_000_000


def compress_and_encrypt(data):
    return _encrypt(gzip.compress(data.encode()))


def decrypt_and_unpack(blob):
    return gzip.decompress(_decrypt(blob)).decode()


def _encrypt(data):
    return data


def _decrypt(data):
    return data


def compute_hash(filename):
    hash_function = sha256()
    with open(filename, 'rb') as f:
        buf = f.read(BLOCK_SIZE)
        while buf:
            hash_function.update(buf)
            buf = f.read(BLOCK_SIZE)

    return hash_function.hexdigest()
