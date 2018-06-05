import gzip

import staticconf
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher
from cryptography.hazmat.primitives.ciphers.algorithms import AES
from cryptography.hazmat.primitives.ciphers.modes import OFB

from backuppy.io import BLOCK_SIZE

GZIP_START = b'\x1f\x8b'

# THIS KEY AND NONCE FOR DEBUGGING ONLY; DO NOT USE FOR REAL DATA!!!
TEMP_AES_KEY = b'\xc8\x7fY\x1e\x963i\xf2cph\xc6\x99\xfdZ\xad4<\xa7\x83\xe5\xf0Z\x8c\xa2\xb2\xfa\xb7\xd8\x15}\xc2'
TEMP_IV = b'1234567812341234'


def compress_and_encrypt(fd_in, fd_out):
    """ Read data from an open file descriptor, and write the compressed, encrypted data to another file descriptor

    :param fd_in: an open plaintext file descriptor in 'rb' mode to read data from
    :param fd_out: an open file descriptor in 'wb' mode to write compressed ciphertext to
    """
    cipher = Cipher(AES(TEMP_AES_KEY), OFB(TEMP_IV), backend=default_backend())
    encryptor = cipher.encryptor()
    while True:
        block = fd_in.read(BLOCK_SIZE)
        if not block:
            break
        zipped_block = gzip.compress(block) if staticconf.read_bool('use_compression') else block
        encrypted_block = encryptor.update(zipped_block) if staticconf.read_bool('use_encryption') else zipped_block
        fd_out.write(encrypted_block)


def decrypt_and_unpack(fd_in, fd_out):
    """ Read encrypted, GZIPed data from an open file descriptor, and write the decoded data to another file descriptor

    :param fd_in: an open file descriptor in 'rb' mode to read ciphertext from
    :param fd_out: an open file descriptor in 'wb' mode to write uncompressed plaintext to
    """
    cipher = Cipher(AES(TEMP_AES_KEY), OFB(TEMP_IV), backend=default_backend())
    decryptor = cipher.decryptor()
    decrypted_data = b''
    while True:
        block = fd_in.read(BLOCK_SIZE)
        if not block:
            break
        decrypted_data += decryptor.update(block) if staticconf.read_bool('use_encryption') else block

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

    # Decompress and write out the last block
    if decrypted_data:
        block = gzip.decompress(decrypted_data) if staticconf.read_bool('use_compression') else decrypted_data
        fd_out.write(block)
