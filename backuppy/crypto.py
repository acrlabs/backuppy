import gzip
from typing import Callable

import colorlog
import staticconf
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher
from cryptography.hazmat.primitives.ciphers.algorithms import AES
from cryptography.hazmat.primitives.ciphers.modes import OFB

from backuppy.io import IOIter

logger = colorlog.getLogger(__name__)
GZIP_START = b'\x1f\x8b'


def identity(x: bytes, y: int = 0) -> bytes:
    # y is to maintain compatibility with gzip.compress
    return x


def compress_and_encrypt(input_file: IOIter, output_file: IOIter, key: bytes, iv: bytes) -> None:
    """ Read data from an open file descriptor, and write the compressed, encrypted data to another file descriptor

    :param fd_in: an open plaintext file descriptor in 'rb' mode to read data from
    :param fd_out: an open file descriptor in 'wb' mode to write compressed ciphertext to
    """
    zip_fn: Callable[[bytes], bytes] = gzip.compress if staticconf.read_bool('use_compression') else identity
    encrypt_fn: Callable[[bytes], bytes] = (
        Cipher(AES(key), OFB(iv), backend=default_backend()).encryptor().update
        if staticconf.read_bool('use_encryption')
        else identity
    )
    writer = output_file.writer(); next(writer)
    logger.debug2('starting to compress')
    for block in input_file.reader():
        block = zip_fn(block)
        logger.debug2(f'zip_fn returned {len(block)} bytes')
        block = encrypt_fn(block)
        logger.debug2(f'encrypt_fn returned {len(block)} bytes')
        writer.send(block)


def decrypt_and_unpack(input_file: IOIter, output_file: IOIter, key: bytes, iv: bytes) -> None:
    """ Read encrypted, GZIPed data from an open file descriptor, and write the decoded data to another file descriptor

    :param fd_in: an open file descriptor in 'rb' mode to read ciphertext from
    :param fd_out: an open file descriptor in 'wb' mode to write uncompressed plaintext to
    """
    decrypted_data = b''
    decrypt_fn: Callable[[bytes], bytes] = (
        Cipher(AES(key), OFB(iv), backend=default_backend()).decryptor().update
        if staticconf.read_bool('use_encryption')
        else identity
    )
    writer = output_file.writer(); next(writer)
    for block in input_file.reader():
        decrypted_data += decrypt_fn(block)
        logger.debug2(f'decrypt_fn returned {len(decrypted_data)} bytes')

        # gzip.decompress throws an EOFError if we pass in partial data, so here we need to
        # decompress each GZIP'ed member individually; to find a complete member we look for
        # the start of the next GZIP blob, which starts with a known constant byte-pair
        if staticconf.read_bool('use_compression'):
            index = decrypted_data.find(GZIP_START, 2)
            if index != -1:
                block = gzip.decompress(decrypted_data[:index])
                logger.debug2(f'unzip_fn returned {len(block)} bytes')
                writer.send(block)
                decrypted_data = decrypted_data[index:]
        else:
            logger.debug2(f'unzip_fn returned {len(decrypted_data)} bytes')
            writer.send(decrypted_data)
            decrypted_data = b''

    # Decompress and write out the last block
    if decrypted_data:
        block = gzip.decompress(decrypted_data) if staticconf.read_bool('use_compression') else decrypted_data
        logger.debug2(f'unzip_fn returned {len(block)} bytes')
        writer.send(block)
