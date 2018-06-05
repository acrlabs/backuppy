import os

from backuppy.crypto import compress_and_encrypt
from backuppy.crypto import decrypt_and_unpack
from backuppy.stores.backup_store import BackupStore
from backuppy.util import get_color_logger

logger = get_color_logger(__name__)


class LocalBackupStore(BackupStore):
    def __init__(self, backup_name):
        super().__init__(backup_name)
        self.location = os.path.abspath(self.config.read_string('location'))

    def save(self, stored_path, tmpfile):
        abs_path = self._abs_stored_path(stored_path)
        os.makedirs(os.path.dirname(abs_path), exist_ok=True)
        if os.path.exists(abs_path):
            logger.warning(f'{abs_path} already exists, but overwriting with new data as requested')
        with open(abs_path, 'wb') as fd_out:
            compress_and_encrypt(tmpfile, fd_out)

    def load(self, stored_path, fd_out):
        with open(self._abs_stored_path(stored_path), 'rb') as fd_in:
            decrypt_and_unpack(fd_in, fd_out)
        fd_out.seek(0)

    def _abs_stored_path(self, stored_path):
        if isinstance(stored_path, str):
            stored_path = (stored_path,)
        return os.path.join(self.location, *stored_path)
