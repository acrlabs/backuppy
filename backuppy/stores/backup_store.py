import os
import signal
import sys
import time
from abc import ABCMeta
from abc import abstractmethod
from contextlib import contextmanager
from functools import partial
from shutil import rmtree
from types import FrameType
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple

import colorlog
import staticconf

from backuppy.blob import apply_diff
from backuppy.blob import compute_diff
from backuppy.crypto import compress_and_encrypt
from backuppy.crypto import decrypt_and_unpack
from backuppy.crypto import generate_key_pair
from backuppy.exceptions import DiffTooLargeException
from backuppy.exceptions import ManifestLockedException
from backuppy.io import compute_sha
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
from backuppy.util import regex_search_list
from backuppy.util import sha_to_path

logger = colorlog.getLogger(__name__)
_UNLOCKED_STORE = None
_SIGNALS_TO_HANDLE = (signal.SIGINT, signal.SIGTERM)


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
    def unlock(self, *, dry_run=False, preserve_scratch=False) -> Iterator:
        """
        Unlock the backup store and prep for work

        The backup store is responsible for the manifest in the store; unfortunately, since
        sqlite3 doesn't accept an open file descriptor when opening a DB connection, we have to
        circumvent some of the IOIter functionality and do it ourselves.  We wrap this in a
        context manager so this can be abstracted away and still ensure that proper cleanup happens.

        :param dry_run: whether to actually save any data or not
        :param preserve_scratch: whether to clean up the scratch directory before we exit; mainly
            used for debugging purposes
        """
        # we have to create the scratch dir regardless of whether --dry-run is enabled
        # because we still need to be able to figure out what's changed and what we should do
        rmtree(get_scratch_dir(), ignore_errors=True)
        os.makedirs(get_scratch_dir(), exist_ok=True)

        try:
            manifests = sorted(self._query(MANIFEST_PREFIX))
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
                    self.config.read('private_key_filename', default=''),
                    self._load,
                    self.options,
                )
            _register_unlocked_store(self, dry_run, preserve_scratch)

            yield

        finally:
            self.do_cleanup(dry_run, preserve_scratch)
            _unregister_store()

    def save_if_new(
        self,
        abs_file_name: str,
        *,
        dry_run: bool = False,
        force_copy: bool = False,
    ) -> Optional[ManifestEntry]:
        """ The main workhorse function; determine if a file has changed, and if so, back it up!

        :param abs_file_name: the name of the file under consideration
        :param dry_run: whether to actually save any data or not
        :param force_copy: make a new copy of the file even if we could compute a diff instead
        """
        curr_entry, new_entry = self.manifest.get_entry(abs_file_name), None
        with IOIter(abs_file_name) as new_file:
            new_sha = compute_sha(new_file)

            # If the file hasn't been backed up before, or if it's been deleted previously, save a
            # new copy; we make a copy here to ensure that the contents don't change while backing
            # the file up, and that we have the correct sha
            if force_copy or not curr_entry or not curr_entry.sha:
                new_entry = self._write_copy(abs_file_name, new_sha, new_file, force_copy, dry_run)

            # If the file has been backed up, check to see if it's changed by comparing shas
            elif new_sha != curr_entry.sha:
                if regex_search_list(abs_file_name, self.options['skip_diff_patterns']):
                    new_entry = self._write_copy(abs_file_name, new_sha, new_file, False, dry_run)
                else:
                    new_entry = self._write_diff(
                        abs_file_name,
                        new_sha,
                        curr_entry,
                        new_file,
                        dry_run,
                    )

            # If the sha is the same but metadata on the file has changed, we just store the updated
            # metadata
            elif (
                new_file.uid != curr_entry.uid or
                new_file.gid != curr_entry.gid or
                new_file.mode != curr_entry.mode
            ):
                logger.info(f'Saving changed metadata for {abs_file_name}')
                new_entry = ManifestEntry(
                    abs_file_name,
                    curr_entry.sha,
                    curr_entry.base_sha,
                    new_file.uid,
                    new_file.gid,
                    new_file.mode,
                    curr_entry.key_pair,  # NOTE: this is safe because the data has not changed!
                    curr_entry.base_key_pair,
                )
            else:
                # we don't want to flood the log with all the files that haven't changed
                logger.debug(f'{abs_file_name} is up to date!')

            if new_entry and not dry_run:
                self.manifest.insert_or_update(new_entry)
            return new_entry  # test_m2_crash_after_file_save

    def restore_entry(
        self,
        entry: ManifestEntry,
        orig_file: IOIter,
        diff_file: IOIter,
        restore_file: IOIter,
    ) -> None:
        if entry.base_sha:
            self.load(entry.base_sha, orig_file, entry.base_key_pair)
            self.load(entry.sha, diff_file, entry.key_pair)
            apply_diff(orig_file, diff_file, restore_file)
        else:
            self.load(entry.sha, restore_file, entry.key_pair)

    def save(self, src: IOIter, dest: str, key_pair: bytes) -> bytes:
        """ Wrapper around the _save function that converts the SHA to a path and does encryption

        :param src: the file to save
        :param dest: the name of the file to write to in the store
        :param key_pair: an AES key + nonce to use to encrypt the file
        :returns: the HMAC of the saved file
        """
        dest = sha_to_path(dest)

        # We compress and encrypt the file on the local file system, and then pass the encrypted
        # file to the backup store to handle atomically
        filename = path_join(get_scratch_dir(), dest)

        with IOIter(filename) as encrypted_save_file:
            signature = compress_and_encrypt(src, encrypted_save_file, key_pair, self.options)
            self._save(encrypted_save_file, dest)  # test_f1_crash_file_save
        os.remove(filename)
        return signature

    def load(
        self,
        src: str,
        dest: IOIter,
        key_pair: Optional[bytes],
    ) -> IOIter:
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

        manifests = sorted(self._query(MANIFEST_PREFIX))
        for manifest in manifests[:-max_versions]:
            ts = manifest.split('.', 1)[1]
            self._delete(manifest)
            self._delete(MANIFEST_KEY_FILE.format(ts=ts))

    def do_cleanup(
        self,
        dry_run: bool,
        preserve_scratch: bool,
    ) -> None:
        """ Ensure that the backup store gets cleaned up appropriately before we shut down; this
        can be called as a signal handler, hence the first two arguments.  Otherwise this should
        be called whenever we lock the store.

        :param signum: if called as a signal handler, the signal num; otherwise None
        :param frame: if called as a signal handler, the stack trace; otherwise None
        :param dry_run: whether to actually save any data or not
        :param preserve_scratch: whether to clean up the scratch directory before we exit; mainly
            used for debugging purposes
        """
        if not self._manifest:
            return

        if not self._manifest.changed:  # test_m1_crash_before_save
            logger.info('No changes detected; nothing to do')
        elif not dry_run:
            lock_manifest(
                self._manifest,
                self.config.read('private_key_filename', default=''),
                self._save,
                self._load,
                self.options,
            )
            self.rotate_manifests()

        if not preserve_scratch:
            rmtree(get_scratch_dir(), ignore_errors=True)
        self._manifest = None  # test_m1_crash_after_save

    def _write_copy(
        self,
        abs_file_name: str,
        new_sha: str,
        file_obj: IOIter,
        force_copy: bool,
        dry_run: bool,
    ) -> ManifestEntry:
        logger.info(f'Saving a new copy of {abs_file_name}')

        entry_data = None
        if not force_copy:
            entry_data = self._find_existing_entry_data(new_sha)  # test_f3_file_changed_while_saving
        key_pair, base_sha, base_key_pair = entry_data or (
            generate_key_pair(self.options),
            None,
            None,
        )
        new_entry = ManifestEntry(  # test_m2_crash_before_file_save
            abs_file_name,
            new_sha,
            base_sha,
            file_obj.uid,
            file_obj.gid,
            file_obj.mode,
            key_pair,
            base_key_pair,
        )
        if not dry_run and not entry_data:
            signature = self.save(file_obj, new_entry.sha, key_pair)
            new_entry.key_pair = key_pair + signature  # append the HMAC before writing to db
        return new_entry

    def _write_diff(
        self,
        abs_file_name: str,
        new_sha: str,
        curr_entry: ManifestEntry,
        file_obj: IOIter,
        dry_run: bool,
    ) -> ManifestEntry:
        logger.info(f'Saving a diff for {abs_file_name}')

        entry_data = self._find_existing_entry_data(new_sha)
        # If the current entry is itself a diff, get its base; otherwise, this
        # entry becomes the base
        if entry_data:
            key_pair, base_sha, base_key_pair = entry_data
        elif curr_entry.base_sha:
            key_pair = generate_key_pair(self.options)
            base_sha = curr_entry.base_sha
            base_key_pair = curr_entry.base_key_pair
        else:
            key_pair = generate_key_pair(self.options)
            base_sha = curr_entry.sha
            base_key_pair = curr_entry.key_pair

        # compute a diff between the version we've previously backed up and the new version
        new_entry = ManifestEntry(
            abs_file_name,
            new_sha,
            base_sha,
            file_obj.uid,
            file_obj.gid,
            file_obj.mode,
            key_pair,
            base_key_pair,
        )

        if not entry_data:
            assert base_sha
            with IOIter() as orig_file, IOIter() as diff_file:
                orig_file = self.load(base_sha, orig_file, base_key_pair)
                try:
                    fd_diff = compute_diff(
                        orig_file,
                        file_obj,
                        diff_file,
                        self.options['discard_diff_percentage'],
                    )
                except DiffTooLargeException:
                    logger.info('The computed diff was too large; saving a copy instead.')
                    logger.info(
                        '(you can configure this threshold with the discard_diff_percentage option)'
                    )
                    file_obj.fd.seek(0)
                    return self._write_copy(abs_file_name, new_sha, file_obj, False, dry_run)

                new_entry.sha = new_sha
                if not dry_run:
                    signature = self.save(fd_diff, new_entry.sha, key_pair)
                    new_entry.key_pair = key_pair + signature
        return new_entry

    def _find_existing_entry_data(
        self,
        sha: str,
    ) -> Optional[Tuple[bytes, Optional[str], Optional[bytes]]]:
        entries = self.manifest.get_entries_by_sha(sha)
        if entries:
            assert len({e.key_pair for e in entries}) == 1
            logger.debug('Found pre-existing sha in the manifest, using that data')
            return entries[0].key_pair, entries[0].base_sha, entries[0].base_key_pair
        else:
            return None

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
            options = self.config.read_list('options', default=[{}])[0]
        except IndexError:
            options = dict()

        return {**DEFAULT_OPTIONS, **options}  # type: ignore


def _cleanup_and_exit(signum: int, frame: FrameType, dry_run: bool, preserve_scratch: bool) -> None:
    """ Signal handler to safely clean up after Ctrl-C or SIGTERM; this minimizes the amount of
    duplicate work we have to do in the event that we cancel the backup partway through
    """

    signal.signal(signum, signal.SIG_IGN)
    logger.info(f'Received signal {signum}, cleaning up the backup store')

    if _UNLOCKED_STORE:
        try:
            _UNLOCKED_STORE.do_cleanup(dry_run, preserve_scratch)
        except Exception as e:
            logger.exception(f'Shutdown was requested, but there was an error cleaning up: {str(e)}')
            sys.exit(1)

    logger.info('Cleanup complete; shutting down')
    sys.exit(0)


def _register_unlocked_store(store: BackupStore, dry_run: bool, preserve_scratch: bool) -> None:
    global _UNLOCKED_STORE
    _UNLOCKED_STORE = store

    sig_handler = partial(
        _cleanup_and_exit,
        dry_run=dry_run,
        preserve_scratch=preserve_scratch
    )
    for sig in _SIGNALS_TO_HANDLE:
        signal.signal(sig, sig_handler)


def _unregister_store() -> None:
    global _UNLOCKED_STORE
    _UNLOCKED_STORE = None

    for sig in _SIGNALS_TO_HANDLE:
        signal.signal(sig, signal.SIG_DFL)
