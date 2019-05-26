import os

import colorlog

from backuppy.crypto import compress_and_encrypt
from backuppy.crypto import decrypt_and_unpack
from backuppy.io import IOIter
from backuppy.stores.backup_store import BackupStore
from backuppy.util import path_join
from tests.crypto_test import TEMP_AES_KEY  # TODO DO NOT USE IN PRODUCTION
from tests.crypto_test import TEMP_IV  # TODO DO NOT USE IN PRODUCTION


logger = colorlog.getLogger(__name__)


class LocalBackupStore(BackupStore):
    def __init__(self, backup_name):
        super().__init__(backup_name)
        self.backup_location = os.path.abspath(self.config.read_string('location'))

    def _save(self, path: str, tmpfile: IOIter, overwrite: bool = False) -> None:
        abs_backup_path = path_join(self.backup_location, path)
        os.makedirs(os.path.dirname(abs_backup_path), exist_ok=True)
        if os.path.exists(abs_backup_path) and not overwrite:
            logger.warning(f'{abs_backup_path} already exists in the store; skipping')
            return

        logger.info(f'Writing {path} to {self.backup_location}')
        with IOIter(abs_backup_path) as output_file:
            compress_and_encrypt(tmpfile, output_file, TEMP_AES_KEY, TEMP_IV)

    def _load(self, path: str, output_file: IOIter) -> IOIter:
        abs_backup_path = os.path.join(self.backup_location, path)
        logger.info(f'Reading {path} from {self.backup_location}')
        with IOIter(abs_backup_path) as input_file:
            decrypt_and_unpack(input_file, output_file, TEMP_AES_KEY, TEMP_IV)
        return output_file
