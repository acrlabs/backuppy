import os

import colorlog

from backuppy.io import io_copy
from backuppy.io import IOIter
from backuppy.stores.backup_store import BackupStore
from backuppy.util import path_join


logger = colorlog.getLogger(__name__)


class LocalBackupStore(BackupStore):
    """ Back up files to a local (on-disk) location """

    def __init__(self, backup_name):
        super().__init__(backup_name)
        self.backup_location = os.path.abspath(self.config.read_string('protocol.location'))

    def _save(self, src: str, dest: str, overwrite: bool = False) -> None:
        abs_backup_path = path_join(self.backup_location, dest)
        os.makedirs(os.path.dirname(abs_backup_path), exist_ok=True)
        if os.path.exists(abs_backup_path) and not overwrite:
            logger.warning(f'{abs_backup_path} already exists in the store; skipping')
            return

        logger.info(f'Writing {src} to {abs_backup_path}')
        os.rename(src, abs_backup_path)

    def _load(self, path: str, output_file: IOIter) -> IOIter:
        abs_backup_path = path_join(self.backup_location, path)
        logger.info(f'Reading {path} from {self.backup_location}')
        with IOIter(abs_backup_path) as input_file:
            io_copy(input_file, output_file)
        return output_file
