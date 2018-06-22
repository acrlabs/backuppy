import gzip
from io import BytesIO

import mock
import pytest
import staticconf.testing
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher
from cryptography.hazmat.primitives.ciphers.algorithms import AES
from cryptography.hazmat.primitives.ciphers.modes import OFB

from backuppy.crypto import compress_and_encrypt
from backuppy.crypto import decrypt_and_unpack

# THIS KEY AND NONCE FOR DEBUGGING ONLY; DO NOT USE FOR REAL DATA!!!
TEMP_AES_KEY = b'\xc8\x7fY\x1e\x963i\xf2cph\xc6\x99\xfdZ\xad4<\xa7\x83\xe5\xf0Z\x8c\xa2\xb2\xfa\xb7\xd8\x15}\xc2'
TEMP_IV = b'1234567812341234'


@pytest.fixture(autouse=True)
def mock_io():
    with mock.patch('backuppy.io.BLOCK_SIZE', 2):
        yield


@pytest.fixture
def mock_io_streams():
    return mock.Mock(wraps=BytesIO(b'asdfasdf')), mock.Mock(wraps=BytesIO())


def test_compress_and_encrypt_no_compression_no_encryption(mock_io_streams):
    orig, new = mock_io_streams
    with staticconf.testing.PatchConfiguration({
        'use_compression': False,
        'use_encryption': False,
    }):
        compress_and_encrypt(orig, new, b'', b'')
    assert new.getvalue() == orig.getvalue()
    assert new.write.call_count == 4


def test_compress_and_encrypt_no_compression(mock_io_streams):
    orig, new = mock_io_streams
    with staticconf.testing.PatchConfiguration({
        'use_compression': False,
        'use_encryption': True,
    }):
        compress_and_encrypt(orig, new, TEMP_AES_KEY, TEMP_IV)

    cipher = Cipher(AES(TEMP_AES_KEY), OFB(TEMP_IV), backend=default_backend()).decryptor()
    decrypted = cipher.update(new.getvalue())
    assert decrypted == orig.getvalue()
    assert new.write.call_count == 4


def test_compress_and_encrypt_no_encryption(mock_io_streams):
    orig, new = mock_io_streams
    with staticconf.testing.PatchConfiguration({
        'use_compression': True,
        'use_encryption': False,
    }):
        compress_and_encrypt(orig, new, b'', b'')

    assert gzip.decompress(new.getvalue()) == orig.getvalue()
    assert new.write.call_count == 4


def test_compress_and_encrypt(mock_io_streams):
    orig, new = mock_io_streams
    with staticconf.testing.PatchConfiguration({
        'use_compression': True,
        'use_encryption': True,
    }):
        compress_and_encrypt(orig, new, TEMP_AES_KEY, TEMP_IV)

    cipher = Cipher(AES(TEMP_AES_KEY), OFB(TEMP_IV), backend=default_backend()).decryptor()
    decrypted = cipher.update(new.getvalue())
    assert gzip.decompress(decrypted) == orig.getvalue()
    assert new.write.call_count == 4


def test_decrypt_and_unpack_no_compression_no_encryption(mock_io_streams):
    orig, new = mock_io_streams
    with staticconf.testing.PatchConfiguration({
        'use_compression': False,
        'use_encryption': False,
    }):
        decrypt_and_unpack(orig, new, b'', b'')
    assert new.getvalue() == orig.getvalue()
    assert new.write.call_count == 4


def test_decrypt_and_unpack_no_compression(mock_io_streams):
    orig, new = mock_io_streams
    orig_contents = orig.getvalue()
    cipher = Cipher(AES(TEMP_AES_KEY), OFB(TEMP_IV), backend=default_backend()).encryptor()
    orig.write(cipher.update(orig_contents))
    with staticconf.testing.PatchConfiguration({
        'use_compression': False,
        'use_encryption': True,
    }):
        decrypt_and_unpack(orig, new, TEMP_AES_KEY, TEMP_IV)

    assert new.getvalue() == orig_contents
    assert new.write.call_count == 4


def test_decrypt_and_unpack_no_encryption(mock_io_streams):
    orig, new = mock_io_streams
    orig_contents = orig.getvalue()
    orig.write(gzip.compress(orig_contents[:4]))
    orig.write(gzip.compress(orig_contents[4:]))
    with staticconf.testing.PatchConfiguration({
        'use_compression': True,
        'use_encryption': False,
    }):
        decrypt_and_unpack(orig, new, b'', b'')

    assert new.getvalue() == orig_contents
    assert new.write.call_count == 2


def test_decrypt_and_unpack(mock_io_streams):
    orig, new = mock_io_streams
    orig_contents = orig.getvalue()
    cipher = Cipher(AES(TEMP_AES_KEY), OFB(TEMP_IV), backend=default_backend()).encryptor()
    orig.write(cipher.update(gzip.compress(orig_contents)))
    with staticconf.testing.PatchConfiguration({
        'use_compression': True,
        'use_encryption': True,
    }):
        decrypt_and_unpack(orig, new, TEMP_AES_KEY, TEMP_IV)

    assert new.getvalue() == orig_contents
    assert new.write.call_count == 1
