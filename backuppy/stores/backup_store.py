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
from backuppy.exceptions import FileChangedException
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
        with IOIter(unlocked_manifest_filename) as manifest_file:
            init_new_manifest = False
            try:
                self._load(MANIFEST_PATH, manifest_file)
            except FileNotFoundError:
                logger.info('This looks like a new backup location; initializing manifest')
                init_new_manifest = True
            self._manifest = Manifest(unlocked_manifest_filename, init_new_manifest)

            yield

            self._manifest = None
            try:
                self._save(MANIFEST_PATH, manifest_file, overwrite=True)
            except FileChangedException:  # pragma: no cover
                pass  # we expect the manifest file to have changed
        os.remove(unlocked_manifest_filename)

    def save_if_new(self, abs_file_name: str) -> None:
        entry = self.manifest.get_entry(abs_file_name)

        with IOIter(abs_file_name) as new_file:
            if not entry or not entry.sha:
                logger.info(f'Saving a new copy of {abs_file_name}')
                with IOIter() as new_file_copy:
                    new_sha = io_copy(new_file, new_file_copy)
                    new_entry = ManifestEntry.from_stat(abs_file_name, new_sha, None, new_file.stat())
                    self.save(new_entry, new_file_copy)
                return

            new_sha = compute_sha(new_file)
            base_sha = entry.base_sha or entry.sha
            new_entry = ManifestEntry.from_stat(abs_file_name, new_sha, base_sha, new_file.stat())
            if entry != new_entry:
                with IOIter() as orig_file, IOIter() as diff_file:
                    logger.info(f'Saving a diff for {abs_file_name}')
                    orig_file = self.load(base_sha, orig_file)
                    new_sha, fd_diff = compute_sha_and_diff(orig_file, new_file, diff_file)
                    new_entry.sha = new_sha

                    # the file hasn't been backed up before, or it's been deleted and re-created
                    self.save(new_entry, fd_diff)
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
