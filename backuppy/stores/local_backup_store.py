import os
import shutil
from typing import List

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
        self.backup_location = os.path.abspath(path_join(
            self.config.read_string('protocol.location'),
            backup_name,
        ))

    def _save(self, src: IOIter, dest: str) -> None:
        assert src.filename  # can't have a tmpfile here
        abs_backup_path = path_join(self.backup_location, dest)
        os.makedirs(os.path.dirname(abs_backup_path), exist_ok=True)
        if os.path.exists(abs_backup_path):
            logger.warning(
                f'{abs_backup_path} already exists in the store; overwriting with new data',
            )

        logger.info(f'Writing {src.filename} to {abs_backup_path}')  # test_f2_lbs_atomicity_1
        shutil.move(src.filename, abs_backup_path)
        return  # test_f2_lbs_atomicity_2

    def _load(self, path: str, output_file: IOIter) -> IOIter:
        abs_backup_path = path_join(self.backup_location, path)
        logger.info(f'Reading {path} from {self.backup_location}')
        with IOIter(abs_backup_path) as input_file:
            io_copy(input_file, output_file)
        return output_file

    def _query(self, prefix: str) -> List[str]:
        # self_rel_name returns things with a leading slash, but we don't necessary want to
        # require that for every possible prefix, so we add it here if it wasn't already present
        if prefix and prefix[0] != '/':
            prefix = '/' + prefix
        results: List[str] = []

        for root, dirs, files in os.walk(self.backup_location):
            # look through all of the directories and see if any of them match the prefix;
            # if they don't match here, there's no reason to recurse into them, so just pop
            # them off (we have to modify dirs in-place here).  We start at the end for "efficiency"
            index = -1
            while index >= -len(dirs):
                rel_dir = self._rel_name(root, dirs[index])
                cmp_len = min(len(prefix), len(rel_dir))
                if prefix[:cmp_len] != rel_dir[:cmp_len]:
                    dirs.pop(index)
                else:
                    index -= 1

            # now add any files that match the query
            for f in files:
                rel_f = self._rel_name(root, f)
                if rel_f.startswith(prefix):
                    results += [rel_f]
        return results

    def _delete(self, filename: str) -> None:
        os.remove(path_join(self.backup_location, filename))

    def _rel_name(self, root: str, filename: str) -> str:
        return path_join(root[len(self.backup_location):], filename)
