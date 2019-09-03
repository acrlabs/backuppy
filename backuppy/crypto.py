import gzip
import os
from typing import Callable

import colorlog
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
from cryptography.hazmat.primitives.ciphers import Cipher
from cryptography.hazmat.primitives.ciphers.algorithms import AES
from cryptography.hazmat.primitives.ciphers.modes import CTR
from cryptography.hazmat.primitives.hashes import SHA256
from cryptography.hazmat.primitives.hmac import HMAC

from backuppy.exceptions import BackupCorruptedError
from backuppy.io import IOIter
from backuppy.options import OptionsDict

logger = colorlog.getLogger(__name__)
GZIP_START = b'\x1f\x8b'
AES_KEY_SIZE = 32    # 256 bits
AES_BLOCK_SIZE = 16  # 128 bits
RSA_KEY_SIZE = 512   # 4096 bits
RSA_KEY_SIZE_BITS = RSA_KEY_SIZE * 8


def identity(x: bytes, y: int = 0) -> bytes:
    # y is to maintain typing compatibility with gzip.compress
    return x


def compress_and_encrypt(
    input_file: IOIter,
    output_file: IOIter,
    key_pair: bytes,
    options: OptionsDict,
) -> bytes:
    """ Read data from an open file descriptor, and write the compressed, encrypted data to another
    file descriptor

    :param input_file: an IOIter object to read plaintext data from
    :param output_file: an IOIter object to write compressed ciphertext to
    """
    key, nonce = key_pair[:AES_KEY_SIZE], key_pair[AES_KEY_SIZE:]
    zip_fn: Callable[[bytes], bytes] = gzip.compress if options['use_compression'] else identity
    encrypt_fn: Callable[[bytes], bytes] = (
        Cipher(AES(key), CTR(nonce), backend=default_backend()).encryptor().update
        if options['use_encryption'] else identity
    )
    hmac = HMAC(key, SHA256(), default_backend())

    writer = output_file.writer(); next(writer)
    logger.debug2('starting to compress')
    for block in input_file.reader():
        block = zip_fn(block)
        logger.debug2(f'zip_fn returned {len(block)} bytes')
        block = encrypt_fn(block)
        logger.debug2(f'encrypt_fn returned {len(block)} bytes')
        if options['use_encryption']:
            hmac.update(block)
        writer.send(block)

    if options['use_encryption']:
        return hmac.finalize()
    else:
        return b''


def decrypt_and_unpack(
    input_file: IOIter,
    output_file: IOIter,
    key_pair: bytes,
    options: OptionsDict,
) -> None:
    """ Read encrypted, GZIPed data from an open file descriptor, and write the decoded data to
    another file descriptor

    :param input_file: an IOIter object to read compressed ciphertext from
    :param output_file: an IOIter object to write plaintext data to
    """
    key, nonce, signature = (
        key_pair[:AES_KEY_SIZE],
        key_pair[AES_KEY_SIZE:AES_KEY_SIZE + AES_BLOCK_SIZE],
        key_pair[AES_KEY_SIZE + AES_BLOCK_SIZE:]
    )
    decrypted_data = b''
    decrypt_fn: Callable[[bytes], bytes] = (
        Cipher(AES(key), CTR(nonce), backend=default_backend()).decryptor().update
        if options['use_encryption'] else identity
    )
    hmac = HMAC(key, SHA256(), default_backend())
    writer = output_file.writer(); next(writer)
    for block in input_file.reader():
        if options['use_encryption']:
            hmac.update(block)
        decrypted_data += decrypt_fn(block)
        logger.debug2(f'decrypt_fn returned {len(decrypted_data)} bytes')

        # gzip.decompress throws an EOFError if we pass in partial data, so here we need to
        # decompress each GZIP'ed member individually; to find a complete member we look for
        # the start of the next GZIP blob, which starts with a known constant byte-pair
        if options['use_compression']:
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
        block = gzip.decompress(decrypted_data) if options['use_compression'] else decrypted_data
        logger.debug2(f'unzip_fn returned {len(block)} bytes')
        writer.send(block)

    try:
        if options['use_encryption']:
            hmac.verify(signature)
    except InvalidSignature as e:
        raise BackupCorruptedError("The file's signature did not match the data") from e


def generate_key_pair() -> bytes:
    return os.urandom(AES_KEY_SIZE + AES_BLOCK_SIZE)


def encrypt_and_sign(data: bytes, private_key_filename: str) -> bytes:
    private_key = _get_key(private_key_filename)
    encrypted_key_pair = private_key.public_key().encrypt(
        data,
        padding.OAEP(padding.MGF1(SHA256()), SHA256(), label=None),
    )
    signature = private_key.sign(
        data,
        padding.PSS(padding.MGF1(SHA256()), padding.PSS.MAX_LENGTH),
        SHA256(),
    )
    return encrypted_key_pair + signature


def decrypt_and_verify(data: bytes, private_key_filename: str) -> bytes:
    private_key = _get_key(private_key_filename)
    message, signature = data[:RSA_KEY_SIZE], data[RSA_KEY_SIZE:]
    key_pair = private_key.decrypt(
        message,
        padding.OAEP(padding.MGF1(SHA256()), SHA256(), label=None),
    )
    try:
        private_key.public_key().verify(
            signature,
            key_pair,
            padding.PSS(padding.MGF1(SHA256()), padding.PSS.MAX_LENGTH),
            SHA256(),
        )
    except InvalidSignature as e:
        raise BackupCorruptedError('Could not decrypt archive') from e

    return key_pair


def _get_key(private_key_filename: str) -> RSAPrivateKey:
    with open(private_key_filename, 'rb') as priv_kf:
        private_key = serialization.load_pem_private_key(priv_kf.read(), None, default_backend())

    if private_key.key_size != RSA_KEY_SIZE_BITS:
        raise ValueError(
            f'Backuppy requires a {RSA_KEY_SIZE_BITS}-bit private key, '
            f'this is {private_key.key_size} bits'
        )

    return private_key
