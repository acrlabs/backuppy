import os
from abc import ABCMeta
from abc import abstractmethod
from contextlib import contextmanager
from shutil import rmtree
from typing import Iterator
from typing import Optional
from uuid import uuid4

import colorlog
import staticconf

from backuppy.blob import compute_sha_and_diff
from backuppy.crypto import compress_and_encrypt
from backuppy.crypto import decrypt_and_unpack
from backuppy.exceptions import ManifestLockedException
from backuppy.io import compute_sha
from backuppy.io import io_copy
from backuppy.io import IOIter
from backuppy.manifest import Manifest
from backuppy.manifest import ManifestEntry
from backuppy.util import get_scratch_dir
from backuppy.util import path_join
from backuppy.util import sha_to_path
from tests.crypto_test import TEMP_AES_KEY  # TODO DO NOT USE IN PRODUCTION
from tests.crypto_test import TEMP_IV  # TODO DO NOT USE IN PRODUCTION

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
        rmtree(get_scratch_dir())
        os.makedirs(get_scratch_dir(), exist_ok=True)

        # Create a new temporary file to store the decrypted manifest; we append a UUID to
        # the filename to ensure some measure of certainty that this file isn't already going
        # to exist

        unlocked_manifest_filename = path_join(
            get_scratch_dir(),
            f'.manifest.sqlite.{uuid4().hex}',
        )
        logger.debug(f'Unlocked manifest located at {unlocked_manifest_filename}')

        # We expect the manifest file to change since it will get committed after each file is
        # backed up
        try:
            with IOIter(unlocked_manifest_filename, check_mtime=False) as manifest_file:
                self.load(src=MANIFEST_PATH, dest=manifest_file, is_manifest=True)
                self._manifest = Manifest(unlocked_manifest_filename)

                yield

                if self._manifest.changed:  # test_m1_crash_before_save
                    self.save(src=manifest_file, dest=MANIFEST_PATH, is_manifest=True)
                else:
                    logger.info('No changes detected; nothing to do')
                self._manifest = None  # test_m1_crash_after_save
        finally:
            # always do our cleanup
            os.remove(unlocked_manifest_filename)

    def save_if_new(self, abs_file_name: str) -> None:
        """ The main workhorse function; determine if a file has changed, and if so, back it up!

        :param abs_file_name: the name of the file under consideration
        """
        entry = self.manifest.get_entry(abs_file_name)

        with IOIter(abs_file_name) as new_file:
            new_sha = compute_sha(new_file)
            uid, gid, mode = new_file.stat().st_uid, new_file.stat().st_gid, new_file.stat().st_mode

            # If the file hasn't been backed up before, or if it's been deleted previously, save a
            # new copy; we make a copy here to ensure that the contents don't change while backing
            # the file up, and that we have the correct sha
            if not entry or not entry.sha:
                logger.info(f'Saving a new copy of {abs_file_name}')
                with IOIter() as new_file_copy:  # test_f3_file_changed_while_saving
                    new_sha = io_copy(new_file, new_file_copy)  # test_m2_crash_before_file_save
                    new_entry = ManifestEntry(abs_file_name, new_sha, None, uid, gid, mode)
                    self.save(src=new_file_copy, dest=new_entry.sha)
                    self.manifest.insert_or_update(new_entry)
                return  # test_m2_crash_after_file_save

            # If the file has been backed up, check to see if it's changed by comparing shas
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
                    self.save(src=fd_diff, dest=new_entry.sha)
                    self.manifest.insert_or_update(new_entry)

            # If the sha is the same but metadata on the file has changed, we just store the updated
            # metadata
            elif uid != entry.uid or gid != entry.gid or mode != entry.mode:
                logger.info(f'Saving changed metadata for {abs_file_name}')
                new_entry = ManifestEntry(abs_file_name, entry.sha, entry.base_sha, uid, gid, mode)
                self.manifest.insert_or_update(new_entry)
            else:
                logger.info(f'{abs_file_name} is up to date!')

    def save(self, src: IOIter, dest: str, is_manifest: bool = False) -> None:
        """ Wrapper around the _save function that converts the SHA to a path and inserts data into
        the manifest
        """
        # We compress and encrypt the file on the local file system, and then pass the encrypted
        # file to the backup store to handle atomically
        if not is_manifest:
            dest = sha_to_path(dest)

        # This can't be a TemporaryFile because the backup_store needs to save it atomically
        encrypted_save_file_path = path_join(get_scratch_dir(), dest)

        with IOIter(encrypted_save_file_path) as encrypted_save_file:
            compress_and_encrypt(src, encrypted_save_file, TEMP_AES_KEY, TEMP_IV)
        self._save(encrypted_save_file_path, dest, overwrite=is_manifest)  # test_f1_crash_file_save

    def load(self, src: str, dest: IOIter, is_manifest: bool = False) -> IOIter:
        """ Wrapper around the _load function that converts the SHA to a path """
        if not is_manifest:
            src = sha_to_path(src)
        with IOIter() as encrypted_load_file:
            self._load(src, encrypted_load_file)
            decrypt_and_unpack(encrypted_load_file, dest, TEMP_AES_KEY, TEMP_IV)
        dest.fd.seek(0)
        return dest

    @abstractmethod
    def _save(self, src: str, dest: str, overwrite: bool = False) -> None:  # pragma: no cover
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
