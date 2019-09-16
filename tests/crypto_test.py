import zlib

import mock
import pytest
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric.rsa import generate_private_key
from cryptography.hazmat.primitives.ciphers import Cipher
from cryptography.hazmat.primitives.ciphers.algorithms import AES
from cryptography.hazmat.primitives.ciphers.modes import CTR
from cryptography.hazmat.primitives.hashes import SHA256
from cryptography.hazmat.primitives.hmac import HMAC

from backuppy.crypto import AES_BLOCK_SIZE
from backuppy.crypto import AES_KEY_SIZE
from backuppy.crypto import compress_and_encrypt
from backuppy.crypto import decrypt_and_unpack
from backuppy.crypto import decrypt_and_verify
from backuppy.crypto import encrypt_and_sign
from backuppy.crypto import RSA_KEY_SIZE_BITS
from backuppy.exceptions import BackupCorruptedError
from tests.conftest import count_matching_log_lines

# THIS KEY AND NONCE FOR DEBUGGING ONLY; DO NOT USE FOR REAL DATA!!!
TMP_KEY = b'1' * AES_KEY_SIZE
TMP_NONCE = b'1' * AES_BLOCK_SIZE
TMP_KEYPAIR = TMP_KEY + TMP_NONCE


def test_compress_and_encrypt_no_compression_no_encryption(caplog, mock_open_streams):
    orig, new, _ = mock_open_streams
    signature = compress_and_encrypt(
        orig,
        new,
        b'',
        dict(use_compression=False, use_encryption=False),
    )
    assert new._fd.getvalue() == orig._fd.getvalue()
    assert count_matching_log_lines('read 2 bytes from /orig', caplog) == 4
    assert count_matching_log_lines('wrote 2 bytes to /new', caplog) == 4
    assert not signature


def test_compress_and_encrypt_no_compression(caplog, mock_open_streams):
    orig, new, _ = mock_open_streams
    signature = compress_and_encrypt(
        orig,
        new,
        TMP_KEYPAIR,
        dict(use_compression=False, use_encryption=True),
    )

    cipher = Cipher(AES(TMP_KEY), CTR(TMP_NONCE), backend=default_backend()).decryptor()
    hmac = HMAC(TMP_KEY, SHA256(), default_backend())
    decrypted = cipher.update(new._fd.getvalue())
    hmac.update(new._fd.getvalue())
    assert decrypted == orig._fd.getvalue()
    assert count_matching_log_lines('read 2 bytes from /orig', caplog) == 4
    assert count_matching_log_lines('wrote 2 bytes to /new', caplog) == 4
    hmac.verify(signature)


def test_compress_and_encrypt_no_encryption(caplog, mock_open_streams):
    orig, new, _ = mock_open_streams
    signature = compress_and_encrypt(
        orig,
        new,
        b'',
        dict(use_compression=True, use_encryption=False),
    )

    assert zlib.decompress(new._fd.getvalue()) == orig._fd.getvalue()
    assert count_matching_log_lines('read 2 bytes from /orig', caplog) == 4
    assert count_matching_log_lines('wrote 2 bytes to /new', caplog) == 1
    assert count_matching_log_lines('wrote 12 bytes to /new', caplog) == 1
    assert not signature


def test_compress_and_encrypt(caplog, mock_open_streams):
    orig, new, _ = mock_open_streams
    signature = compress_and_encrypt(
        orig,
        new,
        TMP_KEYPAIR,
        dict(use_compression=True, use_encryption=True),
    )

    cipher = Cipher(AES(TMP_KEY), CTR(TMP_NONCE), backend=default_backend()).decryptor()
    hmac = HMAC(TMP_KEY, SHA256(), default_backend())
    decrypted = cipher.update(new._fd.getvalue())
    hmac.update(new._fd.getvalue())
    assert zlib.decompress(decrypted) == orig._fd.getvalue()
    assert count_matching_log_lines('read 2 bytes from /orig', caplog) == 4
    assert count_matching_log_lines('wrote 2 bytes to /new', caplog) == 1
    assert count_matching_log_lines('wrote 12 bytes to /new', caplog) == 1
    hmac.verify(signature)


def test_decrypt_and_unpack_no_compression_no_encryption(caplog, mock_open_streams):
    orig, new, _ = mock_open_streams
    decrypt_and_unpack(orig, new, b'', dict(use_compression=False, use_encryption=False))
    assert new._fd.getvalue() == orig._fd.getvalue()
    assert count_matching_log_lines('read 2 bytes from /orig', caplog) == 4
    assert count_matching_log_lines('wrote 2 bytes to /new', caplog) == 4


def test_decrypt_and_unpack_no_compression(caplog, mock_open_streams):
    orig, new, _ = mock_open_streams
    orig_contents = orig._fd.getvalue()
    cipher = Cipher(AES(TMP_KEY), CTR(TMP_NONCE), backend=default_backend()).encryptor()
    ct = cipher.update(orig_contents)
    hmac = HMAC(TMP_KEY, SHA256(), default_backend())
    hmac.update(ct)
    signature = hmac.finalize()
    orig._fd.write(ct)
    decrypt_and_unpack(
        orig,
        new,
        TMP_KEYPAIR + signature,
        dict(use_compression=False, use_encryption=True),
    )

    assert new._fd.getvalue() == orig_contents
    assert count_matching_log_lines('read 2 bytes from /orig', caplog) == 4
    assert count_matching_log_lines('wrote 2 bytes to /new', caplog) == 4


def test_decrypt_and_unpack_no_encryption(caplog, mock_open_streams):
    orig, new, _ = mock_open_streams
    orig_contents = orig._fd.getvalue()
    cobj = zlib.compressobj()
    orig._fd.write(cobj.compress(orig_contents[:4]))
    orig._fd.write(cobj.compress(orig_contents[4:]))
    orig._fd.write(cobj.flush())
    decrypt_and_unpack(orig, new, b'', dict(use_compression=True, use_encryption=False))

    assert new._fd.getvalue() == orig_contents
    assert count_matching_log_lines('read 2 bytes from /orig', caplog) == 7
    assert count_matching_log_lines('wrote 1 bytes to /new', caplog) == 1
    assert count_matching_log_lines('wrote 2 bytes to /new', caplog) == 2
    assert count_matching_log_lines('wrote 4 bytes to /new', caplog) == 1


def test_decrypt_and_unpack(caplog, mock_open_streams):
    orig, new, _ = mock_open_streams
    orig_contents = orig._fd.getvalue()
    cipher = Cipher(AES(TMP_KEY), CTR(TMP_NONCE), backend=default_backend()).encryptor()
    ct = cipher.update(zlib.compress(orig_contents))
    hmac = HMAC(TMP_KEY, SHA256(), default_backend())
    hmac.update(ct)
    signature = hmac.finalize()
    orig._fd.write(ct)
    decrypt_and_unpack(
        orig,
        new,
        TMP_KEYPAIR + signature,
        dict(use_compression=True, use_encryption=True),
    )

    assert new._fd.getvalue() == orig_contents
    assert count_matching_log_lines('read 2 bytes from /orig', caplog) == 7
    assert count_matching_log_lines('wrote 1 bytes to /new', caplog) == 1
    assert count_matching_log_lines('wrote 2 bytes to /new', caplog) == 2
    assert count_matching_log_lines('wrote 4 bytes to /new', caplog) == 1


def test_decrypt_and_unpack_bad_signature(caplog, mock_open_streams):
    orig, new, _ = mock_open_streams
    orig_contents = orig._fd.getvalue()
    cipher = Cipher(AES(TMP_KEY), CTR(TMP_NONCE), backend=default_backend()).encryptor()
    ct = cipher.update(zlib.compress(orig_contents))
    hmac = HMAC(TMP_KEY, SHA256(), default_backend())
    hmac.update(ct)
    signature = hmac.finalize()
    orig._fd.write(ct)
    with pytest.raises(BackupCorruptedError):
        decrypt_and_unpack(
            orig,
            new,
            TMP_KEYPAIR + signature[:-2] + b'f',
            dict(use_compression=True, use_encryption=True),
        )


def test_rsa():
    to_encrypt = b'abcdefgh'
    private_key = generate_private_key(65537, RSA_KEY_SIZE_BITS, default_backend())
    with mock.patch('backuppy.crypto._get_key', return_value=private_key):
        encrypted_data = encrypt_and_sign(to_encrypt, '/fake/private_key_file')
        message = decrypt_and_verify(encrypted_data, '/fake/private_key_file')
    assert to_encrypt == message


def test_rsa_bad_signature():
    to_encrypt = b'abcdefgh'
    private_key = generate_private_key(65537, RSA_KEY_SIZE_BITS, default_backend())
    with mock.patch('backuppy.crypto._get_key', return_value=private_key):
        encrypted_data = encrypt_and_sign(to_encrypt, '/fake/private_key_file')
        corrupted_data = encrypted_data[:-2] + bytes([encrypted_data[-1] - 1])
        with pytest.raises(BackupCorruptedError):
            decrypt_and_verify(corrupted_data, '/fake/private_key_file')
