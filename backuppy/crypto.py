import gzip
from typing import IO

import staticconf
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher
from cryptography.hazmat.primitives.ciphers.algorithms import AES
from cryptography.hazmat.primitives.ciphers.modes import OFB

from backuppy.io import IOIter

GZIP_START = b'\x1f\x8b'


def identity(x: bytes) -> bytes:
    return x


def compress_and_encrypt(fd_in: IO[bytes], fd_out: IO[bytes], key: bytes, iv: bytes) -> None:
    """ Read data from an open file descriptor, and write the compressed, encrypted data to another file descriptor

    :param fd_in: an open plaintext file descriptor in 'rb' mode to read data from
    :param fd_out: an open file descriptor in 'wb' mode to write compressed ciphertext to
    """
    zip_fn = gzip.compress if staticconf.read_bool('use_compression') else identity
    encrypt_fn = (
        Cipher(AES(key), OFB(iv), backend=default_backend()).encryptor().update
        if staticconf.read_bool('use_encryption')
        else identity
    )
    for block in IOIter(fd_in, chain=[zip_fn, encrypt_fn]):
        fd_out.write(block)


def decrypt_and_unpack(fd_in: IO[bytes], fd_out: IO[bytes], key: bytes, iv: bytes) -> None:
    """ Read encrypted, GZIPed data from an open file descriptor, and write the decoded data to another file descriptor

    :param fd_in: an open file descriptor in 'rb' mode to read ciphertext from
    :param fd_out: an open file descriptor in 'wb' mode to write uncompressed plaintext to
    """
    decrypted_data = b''
    decrypt_fn = (
        Cipher(AES(key), OFB(iv), backend=default_backend()).decryptor().update
        if staticconf.read_bool('use_encryption')
        else identity
    )
    for decrypted_block in IOIter(fd_in, chain=[decrypt_fn]):
        decrypted_data += decrypted_block

        # gzip.decompress throws an EOFError if we pass in partial data, so here we need to
        # decompress each GZIP'ed member individually; to find a complete member we look for
        # the start of the next GZIP blob, which starts with a known constant byte-pair
        if staticconf.read_bool('use_compression'):
            index = decrypted_data.find(GZIP_START, 2)
            if index != -1:
                block = gzip.decompress(decrypted_data[:index])
                fd_out.write(block)
                decrypted_data = decrypted_data[index:]
        else:
            fd_out.write(decrypted_data)
            decrypted_data = b''

    # Decompress and write out the last block
    if decrypted_data:
        block = gzip.decompress(decrypted_data) if staticconf.read_bool('use_compression') else decrypted_data
        fd_out.write(block)
