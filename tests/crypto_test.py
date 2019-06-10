import gzip

import staticconf.testing
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher
from cryptography.hazmat.primitives.ciphers.algorithms import AES
from cryptography.hazmat.primitives.ciphers.modes import OFB

from backuppy.crypto import compress_and_encrypt
from backuppy.crypto import decrypt_and_unpack
from tests.conftest import count_matching_log_lines

# THIS KEY AND NONCE FOR DEBUGGING ONLY; DO NOT USE FOR REAL DATA!!!
TEMP_AES_KEY = b'\xc8\x7fY\x1e\x963i\xf2cph\xc6\x99\xfdZ\xad4<\xa7\x83\xe5\xf0Z\x8c\xa2\xb2\xfa\xb7\xd8\x15}\xc2'  # noqa
TEMP_IV = b'1234567812341234'


def test_compress_and_encrypt_no_compression_no_encryption(caplog, mock_open_streams):
    orig, new, _ = mock_open_streams
    with staticconf.testing.PatchConfiguration({
        'use_compression': False,
        'use_encryption': False,
    }):
        compress_and_encrypt(orig, new, b'', b'')
    assert new._fd.getvalue() == orig._fd.getvalue()
    assert count_matching_log_lines('read 2 bytes from /orig', caplog) == 4
    assert count_matching_log_lines('wrote 2 bytes to /new', caplog) == 4


def test_compress_and_encrypt_no_compression(caplog, mock_open_streams):
    orig, new, _ = mock_open_streams
    with staticconf.testing.PatchConfiguration({
        'use_compression': False,
        'use_encryption': True,
    }):
        compress_and_encrypt(orig, new, TEMP_AES_KEY, TEMP_IV)

    cipher = Cipher(AES(TEMP_AES_KEY), OFB(TEMP_IV), backend=default_backend()).decryptor()
    decrypted = cipher.update(new._fd.getvalue())
    assert decrypted == orig._fd.getvalue()
    assert count_matching_log_lines('read 2 bytes from /orig', caplog) == 4
    assert count_matching_log_lines('wrote 2 bytes to /new', caplog) == 4


def test_compress_and_encrypt_no_encryption(caplog, mock_open_streams):
    orig, new, _ = mock_open_streams
    with staticconf.testing.PatchConfiguration({
        'use_compression': True,
        'use_encryption': False,
    }):
        compress_and_encrypt(orig, new, b'', b'')

    assert gzip.decompress(new._fd.getvalue()) == orig._fd.getvalue()
    assert count_matching_log_lines('read 2 bytes from /orig', caplog) == 4
    assert count_matching_log_lines('wrote 22 bytes to /new', caplog) == 4


def test_compress_and_encrypt(caplog, mock_open_streams):
    orig, new, _ = mock_open_streams
    with staticconf.testing.PatchConfiguration({
        'use_compression': True,
        'use_encryption': True,
    }):
        compress_and_encrypt(orig, new, TEMP_AES_KEY, TEMP_IV)

    cipher = Cipher(AES(TEMP_AES_KEY), OFB(TEMP_IV), backend=default_backend()).decryptor()
    decrypted = cipher.update(new._fd.getvalue())
    assert gzip.decompress(decrypted) == orig._fd.getvalue()
    assert count_matching_log_lines('read 2 bytes from /orig', caplog) == 4
    assert count_matching_log_lines('wrote 22 bytes to /new', caplog) == 4


def test_decrypt_and_unpack_no_compression_no_encryption(caplog, mock_open_streams):
    orig, new, _ = mock_open_streams
    with staticconf.testing.PatchConfiguration({
        'use_compression': False,
        'use_encryption': False,
    }):
        decrypt_and_unpack(orig, new, b'', b'')
    assert new._fd.getvalue() == orig._fd.getvalue()
    assert count_matching_log_lines('read 2 bytes from /orig', caplog) == 4
    assert count_matching_log_lines('wrote 2 bytes to /new', caplog) == 4


def test_decrypt_and_unpack_no_compression(caplog, mock_open_streams):
    orig, new, _ = mock_open_streams
    orig_contents = orig._fd.getvalue()
    cipher = Cipher(AES(TEMP_AES_KEY), OFB(TEMP_IV), backend=default_backend()).encryptor()
    orig._fd.write(cipher.update(orig_contents))
    with staticconf.testing.PatchConfiguration({
        'use_compression': False,
        'use_encryption': True,
    }):
        decrypt_and_unpack(orig, new, TEMP_AES_KEY, TEMP_IV)

    assert new._fd.getvalue() == orig_contents
    assert count_matching_log_lines('read 2 bytes from /orig', caplog) == 4
    assert count_matching_log_lines('wrote 2 bytes to /new', caplog) == 4


def test_decrypt_and_unpack_no_encryption(caplog, mock_open_streams):
    orig, new, _ = mock_open_streams
    orig_contents = orig._fd.getvalue()
    orig._fd.write(gzip.compress(orig_contents[:4]))
    orig._fd.write(gzip.compress(orig_contents[4:]))
    with staticconf.testing.PatchConfiguration({
        'use_compression': True,
        'use_encryption': False,
    }):
        decrypt_and_unpack(orig, new, b'', b'')

    assert new._fd.getvalue() == orig_contents
    assert count_matching_log_lines('read 2 bytes from /orig', caplog) == 24
    assert count_matching_log_lines('wrote 4 bytes to /new', caplog) == 1
    assert count_matching_log_lines('wrote 5 bytes to /new', caplog) == 1


def test_decrypt_and_unpack(caplog, mock_open_streams):
    orig, new, _ = mock_open_streams
    orig_contents = orig._fd.getvalue()
    cipher = Cipher(AES(TEMP_AES_KEY), OFB(TEMP_IV), backend=default_backend()).encryptor()
    orig._fd.write(cipher.update(gzip.compress(orig_contents)))
    with staticconf.testing.PatchConfiguration({
        'use_compression': True,
        'use_encryption': True,
    }):
        decrypt_and_unpack(orig, new, TEMP_AES_KEY, TEMP_IV)

    assert new._fd.getvalue() == orig_contents
    assert count_matching_log_lines('read 2 bytes from /orig', caplog) == 13
    assert count_matching_log_lines('wrote 9 bytes to /new', caplog) == 1
