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
        """ A BackupStore object controls all the reading and writing of data from a particular
        backup location (local, S3, ssh, etc)

        This is an abstract class that needs to be subclassed with a _save and _load function
        that determines how to actually read and write data from/to the store.  The remaining
        methods for this class are common across different types of stores, and are what establish
        many of the "safety" guarantees of backuppy.

        :param backup_name: the name of the backup this store corresponds to in the
            configuration file
        """
        self.backup_name = backup_name
        self.config = staticconf.NamespaceReaders(backup_name)
        self._manifest = None

    @contextmanager
    def open_manifest(self) -> Iterator:
        """ The backup store is responsible for the manifest in the store; unfortunately, since
        sqlite3 doesn't accept an open file descriptor when opening a DB connection, we have to
        circumvent some of the IOIter functionality and do it ourselves.  We wrap this in a
        context manager so this can be abstracted away and still ensure that proper cleanup happens.
        """

        # Create a new temporary file to store the decrypted manifest; we append a UUID to
        # the filename to ensure some measure of certainty that this file isn't already going
        # to exist
        unlocked_manifest_filename = f'.manifest.sqlite.{uuid4().hex}'
        logger.debug(f'Unlocked manifest located at {unlocked_manifest_filename}')

        # We expect the manifest file to change since it will get committed after each file is
        # backed up
        try:
            with IOIter(unlocked_manifest_filename, check_mtime=False) as manifest_file:
                # Call the _load function directly (instead of load) because the manifest filename
                # isn't a SHA
                self._load(MANIFEST_PATH, manifest_file)
                self._manifest = Manifest(unlocked_manifest_filename)

                yield

                if self._manifest.changed:
                    self._save(MANIFEST_PATH, manifest_file, overwrite=True)
                else:
                    logger.info('No changes detected; nothing to do')
                self._manifest = None
        finally:
            # always do our cleanup
            os.remove(unlocked_manifest_filename)

    def save_if_new(self, abs_file_name: str) -> None:
        """ The main workhorse function; determine if a file has changed, and if so, back it up!

        :param abs_file_name: the name of the file under consideration
        """
        entry = self.manifest.get_entry(abs_file_name)

        with IOIter(abs_file_name) as new_file:
            uid, gid, mode = new_file.stat().st_uid, new_file.stat().st_gid, new_file.stat().st_mode

            # If the file hasn't been backed up before, or if it's been deleted previously, save a
            # new copy
            if not entry or not entry.sha:
                logger.info(f'Saving a new copy of {abs_file_name}')
                with IOIter() as new_file_copy:
                    # We make a copy on the local file system and then back up the copy so that
                    # we can be assured that the file doesn't change while we make the backup
                    new_sha = io_copy(new_file, new_file_copy)
                    new_entry = ManifestEntry(abs_file_name, new_sha, None, uid, gid, mode)
                    self.save(new_entry, new_file_copy)
                return

            # If the file has been backed up, check to see if it's changed by comparing shas
            new_sha = compute_sha(new_file)
            if new_sha != entry.sha:
                logger.info(f'Saving a diff for {abs_file_name}')
                base_sha = entry.base_sha or entry.sha
                new_entry = ManifestEntry(abs_file_name, new_sha, base_sha, uid, gid, mode)

                # compute a diff between the version we've previously backed up and the new version
                with IOIter() as orig_file, IOIter() as diff_file:
                    orig_file = self.load(base_sha, orig_file)

                    # we _recompute_ the sha here because the file may have changed between when
                    # we backed it up and when we computed the diff
                    new_sha, fd_diff = compute_sha_and_diff(orig_file, new_file, diff_file)
                    new_entry.sha = new_sha
                    self.save(new_entry, fd_diff)

            # If the sha is the same but metadata on the file has changed, we just store the updated
            # metadata
            elif uid != entry.uid or gid != entry.gid or mode != entry.mode:
                logger.info(f'Saving changed metadata for {abs_file_name}')
                new_entry = ManifestEntry(abs_file_name, entry.sha, entry.base_sha, uid, gid, mode)
                self.manifest.insert_or_update(new_entry)
            else:
                logger.info(f'{abs_file_name} is up to date!')

    def save(self, entry: ManifestEntry, tmpfile: IOIter) -> None:
        """ Wrapper around the _save function that converts the SHA to a path and inserts data into
        the manifest
        """
        self._save(sha_to_path(entry.sha), tmpfile)
        self.manifest.insert_or_update(entry)

    def load(self, sha: str, tmpfile: IOIter) -> IOIter:
        """ Wrapper around the _load function that converts the SHA to a path """
        return self._load(sha_to_path(sha), tmpfile)

    @abstractmethod
    def _save(self, path: str, tmpfile: IOIter, overwrite: bool = False) -> None:  # pragma: no cover
        pass

    @abstractmethod
    def _load(self, path: str, tmpfile: IOIter) -> IOIter:  # pragma: no cover
        pass

    @property
    def manifest(self) -> Manifest:
        """ Wrapper around the manifest to make sure we've unlocked it in a
        with open_manifest()... block
        """
        if not self._manifest:
            raise ManifestLockedException('The manifest is currently locked')
        return self._manifest
