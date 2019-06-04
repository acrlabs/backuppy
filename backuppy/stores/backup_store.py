import os
from abc import ABCMeta
from abc import abstractmethod
from contextlib import contextmanager
from typing import Iterator
from typing import Optional
from uuid import uuid4

import colorlog
import staticconf

from backuppy.blob import compute_sha_and_diff
from backuppy.exceptions import ManifestLockedException
from backuppy.io import compute_sha
from backuppy.io import io_copy
from backuppy.io import IOIter
from backuppy.manifest import Manifest
from backuppy.manifest import ManifestEntry
from backuppy.util import sha_to_path

MANIFEST_PATH = 'manifest.sqlite'
logger = colorlog.getLogger(__name__)


class BackupStore(metaclass=ABCMeta):
    backup_name: str
    _manifest: Optional[Manifest]

    def __init__(self, backup_name: str) -> None:
        self.backup_name = backup_name
        self.config = staticconf.NamespaceReaders(backup_name)
        self._manifest = None

    @contextmanager
    def open_manifest(self) -> Iterator:
        unlocked_manifest_filename = f'.manifest.sqlite.{uuid4().hex}'
        logger.debug(f'Unlocked manifest located at {unlocked_manifest_filename}')
        with IOIter(unlocked_manifest_filename, check_mtime=False) as manifest_file:
            self._load(MANIFEST_PATH, manifest_file)
            self._manifest = Manifest(unlocked_manifest_filename)

            yield

            if self._manifest.changed:
                self._save(MANIFEST_PATH, manifest_file, overwrite=True)
            else:
                logger.info('No changes detected; nothing to do')
            self._manifest = None
        os.remove(unlocked_manifest_filename)

    def save_if_new(self, abs_file_name: str) -> None:
        entry = self.manifest.get_entry(abs_file_name)

        with IOIter(abs_file_name) as new_file:
            uid, gid, mode = new_file.stat().st_uid, new_file.stat().st_gid, new_file.stat().st_mode
            if not entry or not entry.sha:
                logger.info(f'Saving a new copy of {abs_file_name}')
                with IOIter() as new_file_copy:
                    new_sha = io_copy(new_file, new_file_copy)
                    new_entry = ManifestEntry(abs_file_name, new_sha, None, uid, gid, mode)
                    self.save(new_entry, new_file_copy)
                return

            new_sha = compute_sha(new_file)
            if new_sha != entry.sha:
                logger.info(f'Saving a diff for {abs_file_name}')
                base_sha = entry.base_sha or entry.sha
                new_entry = ManifestEntry(abs_file_name, new_sha, base_sha, uid, gid, mode)
                with IOIter() as orig_file, IOIter() as diff_file:
                    orig_file = self.load(base_sha, orig_file)
                    new_sha, fd_diff = compute_sha_and_diff(orig_file, new_file, diff_file)
                    new_entry.sha = new_sha
                    self.save(new_entry, fd_diff)
            elif uid != entry.uid or gid != entry.gid or mode != entry.mode:
                logger.info(f'Saving changed metadata for {abs_file_name}')
                new_entry = ManifestEntry(abs_file_name, entry.sha, entry.base_sha, uid, gid, mode)
                self.manifest.insert_or_update(new_entry)
            else:
                logger.info(f'{abs_file_name} is up to date!')

    def save(self, entry: ManifestEntry, tmpfile: IOIter) -> None:
        self._save(sha_to_path(entry.sha), tmpfile)
        self.manifest.insert_or_update(entry)

    def load(self, sha: str, tmpfile: IOIter) -> IOIter:
        return self._load(sha_to_path(sha), tmpfile)

    @abstractmethod
    def _save(self, path: str, tmpfile: IOIter, overwrite: bool = False) -> None:  # pragma: no cover
        pass

    @abstractmethod
    def _load(self, path: str, tmpfile: IOIter) -> IOIter:  # pragma: no cover
        pass

    @property
    def manifest(self) -> Manifest:
        if not self._manifest:
            raise ManifestLockedException('The manifest is currently locked')
        return self._manifest
