import os
import zlib
from itertools import chain
from itertools import repeat
from typing import Callable
from typing import cast
from typing import Generator
from typing import Optional
from typing import Tuple

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
AES_KEY_SIZE = 32    # 256 bits
AES_BLOCK_SIZE = 16  # 128 bits
RSA_KEY_SIZE = 512   # 4096 bits
RSA_KEY_SIZE_BITS = RSA_KEY_SIZE * 8


def identity(x: bytes) -> bytes:
    return x


def compress_and_encrypt(
    input_file: IOIter,
    output_file: IOIter,
    key_pair: Optional[bytes],
    options: OptionsDict,
) -> bytes:
    """ Read data from an open file descriptor, and write the compressed, encrypted data to another
    file descriptor; compute the HMAC of the encrypted data to ensure integrity

    :param input_file: an IOIter object to read plaintext data from
    :param output_file: an IOIter object to write compressed ciphertext to
    """
    key, nonce = (key_pair[:AES_KEY_SIZE], key_pair[AES_KEY_SIZE:]) if key_pair else (b'', b'')
    compressobj = zlib.compressobj()
    zip_fn: Callable[[bytes], bytes] = (  # type: ignore
        compressobj.compress if options['use_compression'] else identity
    )
    encrypt_fn: Callable[[bytes], bytes] = (
        Cipher(AES(key), CTR(nonce), backend=default_backend()).encryptor().update
        if options['use_encryption'] else identity
    )
    hmac = HMAC(key, SHA256(), default_backend())

    def last_block() -> Generator[Tuple[bytes, bool], None, None]:
        yield (compressobj.flush(), False) if options['use_compression'] else (b'', False)

    writer = output_file.writer(); next(writer)
    logger.debug2('starting to compress')
    for block, needs_compression in chain(zip(input_file.reader(), repeat(True)), last_block()):
        if needs_compression:
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
    key_pair: Optional[bytes],
    options: OptionsDict,
) -> None:
    """ Read encrypted, GZIPed data from an open file descriptor, and write the decoded data to
    another file descriptor; verify the HMAC of the encrypted data to ensure integrity

    :param input_file: an IOIter object to read compressed ciphertext from
    :param output_file: an IOIter object to write plaintext data to
    """
    key, nonce, signature = (
        key_pair[:AES_KEY_SIZE],
        key_pair[AES_KEY_SIZE:AES_KEY_SIZE + AES_BLOCK_SIZE],
        key_pair[AES_KEY_SIZE + AES_BLOCK_SIZE:]
    ) if key_pair else (b'', b'', b'')
    decrypted_data = b''
    decrypt_fn: Callable[[bytes], bytes] = (
        Cipher(AES(key), CTR(nonce), backend=default_backend()).decryptor().update
        if options['use_encryption'] else identity
    )
    decompress_obj = zlib.decompressobj()
    unzip_fn: Callable[[bytes], bytes] = (
        decompress_obj.decompress  # type: ignore
        if options['use_compression'] else identity
    )
    hmac = HMAC(key, SHA256(), default_backend())
    writer = output_file.writer(); next(writer)
    for encrypted_data in input_file.reader():
        if options['use_encryption']:
            hmac.update(encrypted_data)
        decrypted_data += decrypt_fn(encrypted_data)
        logger.debug2(f'decrypt_fn returned {len(decrypted_data)} bytes')

        block = unzip_fn(decrypted_data)
        logger.debug2(f'unzip_fn returned {len(block)} bytes')
        writer.send(block)
        decrypted_data = decompress_obj.unused_data

    # Decompress and write out the last block
    if decrypted_data:
        block = unzip_fn(decrypted_data)
        logger.debug2(f'unzip_fn returned {len(block)} bytes')
        writer.send(block)

    try:
        if options['use_encryption']:
            hmac.verify(signature)
    except InvalidSignature as e:
        raise BackupCorruptedError("The file's signature did not match the data") from e


def generate_key_pair(options: OptionsDict) -> bytes:
    if not options['use_encryption']:
        return b''
    return os.urandom(AES_KEY_SIZE + AES_BLOCK_SIZE)


def encrypt_and_sign(data: bytes, private_key_filename: str) -> bytes:
    """ Use an RSA private key to encrypt and sign some data

    :param data: the bytes to encrypt
    :param private_key_filename: the location of the RSA private key file in PEM format
    :returns: the encrypted data with signature appended
    """
    private_key = _get_key(private_key_filename)

    # the public key is used to encrypt, private key to decrypt
    encrypted_key_pair = private_key.public_key().encrypt(
        data,
        padding.OAEP(padding.MGF1(SHA256()), SHA256(), label=None),
    )
    # the _private_ key is used to sign, the public key to verify
    signature = private_key.sign(
        data,
        padding.PSS(padding.MGF1(SHA256()), padding.PSS.MAX_LENGTH),
        SHA256(),
    )
    return encrypted_key_pair + signature


def decrypt_and_verify(data: bytes, private_key_filename: str) -> bytes:
    """ Use an RSA private key to decrypt and verify some data

    :param data: encrypted data with a signature appended
    :param private_key_filename: the location of the RSA private key file in PEM format
    :returns: the unencrypted data
    :raises BackupCorruptedError: if the signature cannot be verified
    """

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
        private_key = cast(
            RSAPrivateKey,
            serialization.load_pem_private_key(priv_kf.read(), None, default_backend()),
        )

    if private_key.key_size != RSA_KEY_SIZE_BITS:
        raise ValueError(
            f'Backuppy requires a {RSA_KEY_SIZE_BITS}-bit private key, '
            f'this is {private_key.key_size} bits'
        )

    return private_key
