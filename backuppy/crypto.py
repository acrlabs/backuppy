import gzip


def compress_and_encrypt(data):
    return _encrypt(gzip.compress(data))


def decrypt_and_unpack(blob):
    return gzip.decompress(_decrypt(blob))


def _encrypt(data):
    return data


def _decrypt(data):
    return data
