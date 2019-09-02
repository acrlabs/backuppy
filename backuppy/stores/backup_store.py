import os
import time
from abc import ABCMeta
from abc import abstractmethod
from contextlib import contextmanager
from shutil import rmtree
from typing import Iterator
from typing import List
from typing import Optional

import colorlog
import staticconf

from backuppy.blob import compute_sha_and_diff
from backuppy.crypto import compress_and_encrypt
from backuppy.crypto import decrypt_and_unpack
from backuppy.crypto import generate_key_pair
from backuppy.exceptions import ManifestLockedException
from backuppy.io import compute_sha
from backuppy.io import io_copy
from backuppy.io import IOIter
from backuppy.manifest import lock_manifest
from backuppy.manifest import Manifest
from backuppy.manifest import MANIFEST_FILE
from backuppy.manifest import MANIFEST_KEY_FILE
from backuppy.manifest import MANIFEST_PREFIX
from backuppy.manifest import ManifestEntry
from backuppy.manifest import unlock_manifest
from backuppy.options import DEFAULT_OPTIONS
from backuppy.options import OptionsDict
from backuppy.util import get_scratch_dir
from backuppy.util import path_join
from backuppy.util import sha_to_path

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
    def unlock(self, clean_up_afterwards=True) -> Iterator:
        """
        Unlock the backup store and prep for work

        The backup store is responsible for the manifest in the store; unfortunately, since
        sqlite3 doesn't accept an open file descriptor when opening a DB connection, we have to
        circumvent some of the IOIter functionality and do it ourselves.  We wrap this in a
        context manager so this can be abstracted away and still ensure that proper cleanup happens.
        """
        rmtree(get_scratch_dir())
        os.makedirs(get_scratch_dir(), exist_ok=True)

        manifests = sorted(self._query(MANIFEST_PREFIX + '.'))
        if not manifests:
            logger.warning(
                '''
                ********************************************************************
                This looks like a new backup location; if you are not expecting this
                message, someone may be tampering with your backup!
                ********************************************************************
                '''
            )
            self._manifest = Manifest(os.path.join(
                get_scratch_dir(),
                MANIFEST_FILE.format(ts=time.time()),
            ))
        else:
            self._manifest = unlock_manifest(
                manifests[-1],
                self.config.read('private_key_filename'),
                self._load,
                self.options,
            )

        yield

        if not self._manifest.changed:  # test_m1_crash_before_save
            logger.info('No changes detected; nothing to do')
        else:
            lock_manifest(
                self._manifest,
                self.config.read('private_key_filename'),
                self._save,
                self.options,
            )
            self.rotate_manifests()
        if clean_up_afterwards:
            rmtree(get_scratch_dir())
        self._manifest = None  # test_m1_crash_after_save

    def save_if_new(self, abs_file_name: str) -> None:
        """ The main workhorse function; determine if a file has changed, and if so, back it up!

        :param abs_file_name: the name of the file under consideration
        """
        entry = self.manifest.get_entry(abs_file_name)

        with IOIter(abs_file_name) as new_file:
            new_sha = compute_sha(new_file)
            uid, gid, mode = new_file.stat().st_uid, new_file.stat().st_gid, new_file.stat().st_mode
            key_pair = generate_key_pair()

            # If the file hasn't been backed up before, or if it's been deleted previously, save a
            # new copy; we make a copy here to ensure that the contents don't change while backing
            # the file up, and that we have the correct sha
            if not entry or not entry.sha:
                logger.info(f'Saving a new copy of {abs_file_name}')
                with IOIter() as new_file_copy:  # test_f3_file_changed_while_saving
                    new_sha = io_copy(new_file, new_file_copy)  # test_m2_crash_before_file_save
                    new_entry = ManifestEntry(
                        abs_file_name,
                        new_sha,
                        None,
                        uid,
                        gid,
                        mode,
                        key_pair,
                        None,
                    )
                    self.save(new_file_copy, new_entry.sha, key_pair)
                    self.manifest.insert_or_update(new_entry)
                return  # test_m2_crash_after_file_save

            # If the file has been backed up, check to see if it's changed by comparing shas
            if new_sha != entry.sha:
                logger.info(f'Saving a diff for {abs_file_name}')

                # If the current entry is itself a diff, get its base; otherwise, this
                # entry becomes the base
                if entry.base_sha:
                    base_sha = entry.base_sha
                    base_key_pair = entry.base_key_pair
                else:
                    base_sha = entry.sha
                    base_key_pair = entry.key_pair
                assert base_key_pair  # make mypy happy; this cannot be None here

                new_entry = ManifestEntry(
                    abs_file_name,
                    new_sha,
                    base_sha,
                    uid,
                    gid,
                    mode,
                    key_pair,
                    base_key_pair,
                )

                # compute a diff between the version we've previously backed up and the new version
                with IOIter() as orig_file, IOIter() as diff_file:
                    orig_file = self.load(base_sha, orig_file, base_key_pair)

                    # we _recompute_ the sha here because the file may have changed between when
                    # we backed it up and when we computed the diff
                    new_sha, fd_diff = compute_sha_and_diff(orig_file, new_file, diff_file)
                    new_entry.sha = new_sha
                    self.save(fd_diff, new_entry.sha, key_pair)
                    self.manifest.insert_or_update(new_entry)

            # If the sha is the same but metadata on the file has changed, we just store the updated
            # metadata
            elif uid != entry.uid or gid != entry.gid or mode != entry.mode:
                logger.info(f'Saving changed metadata for {abs_file_name}')
                new_entry = ManifestEntry(
                    abs_file_name,
                    entry.sha,
                    entry.base_sha,
                    uid,
                    gid,
                    mode,
                    entry.key_pair,  # NOTE: this is safe because the data has not changed!
                    entry.base_key_pair,
                )
                self.manifest.insert_or_update(new_entry)
            else:
                logger.info(f'{abs_file_name} is up to date!')

    def save(self, src: IOIter, dest: str, key_pair: bytes) -> None:
        """ Wrapper around the _save function that converts the SHA to a path and inserts data into
        the manifest
        """
        dest = sha_to_path(dest)

        # We compress and encrypt the file on the local file system, and then pass the encrypted
        # file to the backup store to handle atomically
        with IOIter(path_join(get_scratch_dir(), dest)) as encrypted_save_file:
            compress_and_encrypt(src, encrypted_save_file, key_pair, self.options)
            self._save(encrypted_save_file, dest)  # test_f1_crash_file_save

    def load(self, src: str, dest: IOIter, key_pair: bytes) -> IOIter:
        """ Wrapper around the _load function that converts the SHA to a path """
        src = sha_to_path(src)

        with IOIter() as encrypted_load_file:
            self._load(src, encrypted_load_file)
            decrypt_and_unpack(encrypted_load_file, dest, key_pair, self.options)
        dest.fd.seek(0)
        return dest

    def rotate_manifests(self) -> None:
        max_versions = self.options['max_manifest_versions']
        if not max_versions:
            return  # this just means that there's no configured limit to the number of versions

        manifests = sorted(self._query(MANIFEST_PREFIX + '.'))
        for manifest in manifests[:-max_versions]:
            ts = manifest.split('.')[1]
            self._delete(manifest)
            self._delete(MANIFEST_KEY_FILE.format(ts=ts))

    @abstractmethod
    def _save(self, src: IOIter, dest: str) -> None:  # pragma: no cover
        pass

    @abstractmethod
    def _load(self, path: str, tmpfile: IOIter) -> IOIter:  # pragma: no cover
        pass

    @abstractmethod
    def _query(self, prefix: str) -> List[str]:  # pragma: no cover
        pass

    @abstractmethod
    def _delete(self, filename: str) -> None:  # pragma: no cover
        pass

    @property
    def manifest(self) -> Manifest:
        """ Wrapper around the manifest to make sure we've unlocked it in a
        with unlock()... block
        """
        if not self._manifest:
            raise ManifestLockedException('The manifest is currently locked')
        return self._manifest

    @property
    def options(self) -> OptionsDict:
        try:
            options = self.config.read_list('options')[0]
        except IndexError:
            options = dict()

        return {**DEFAULT_OPTIONS, **options}  # type: ignore
