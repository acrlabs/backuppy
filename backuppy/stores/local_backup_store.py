import os

from backuppy.stores.backup_store import BackupStore
from backuppy.util import get_color_logger

logger = get_color_logger(__name__)


class LocalBackupStore(BackupStore):
    def __init__(self, backup_name):
        super().__init__(backup_name)
        self.location = os.path.abspath(self.config.read_string('location'))

    def write(self, stored_path, tmpfile):
        if isinstance(stored_path, str):
            stored_path = (stored_path,)
        os.renames(tmpfile, os.path.join(self.location, *stored_path))

    def read(self, names):
        if isinstance(names, str):
            return os.path.join(self.location, names)
        else:
            return [os.path.join(self.location, name) for name in names]
