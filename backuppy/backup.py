import os
from hashlib import sha256
from tempfile import TemporaryFile
from typing import List
from typing import Pattern
from typing import Tuple

import colorlog
import staticconf

from backuppy.blob import compute_diff
from backuppy.io import IOIter
from backuppy.manifest import Manifest
from backuppy.manifest import ManifestEntry
from backuppy.stores.backup_store import BackupStore
from backuppy.stores.backup_store import MANIFEST_PATH
from backuppy.util import compile_exclusions
from backuppy.util import file_walker
from backuppy.util import sha_to_path

logger = colorlog.getLogger(__name__)


def _scan_directory(abs_base_path: str, manifest: Manifest, exclusions: List[Pattern]) -> Tuple[set, set]:
    """ scan a directory looking for changes from the manifest """
    modified_files, marked_files = set(), set()
    for abs_file_name in file_walker(abs_base_path, logger.warning):

        # Skip files that match any of the specified regular expressions
        matched_patterns = [pattern for pattern in exclusions if pattern.search(abs_file_name)]
        if matched_patterns:
            logger.info(f'{abs_file_name} matched exclusion pattern(s) "{matched_patterns}"; skipping')
            continue

        # Mark the file as "seen" so it isn't deleted later
        marked_files.add(abs_file_name)

        if manifest.is_current(abs_file_name):
            logger.info(f'{abs_file_name} is up-to-date; skipping')
            continue

        modified_files.add(abs_file_name)
    return modified_files, marked_files


def _save_copy(abs_file_name: str, backup_store: BackupStore) -> None:
    """ write a complete copy of the file to the backup store """
    sha_fn = sha256()
    with open(abs_file_name, 'rb') as fd_orig, TemporaryFile() as fd_copy:
        for data in IOIter(fd_orig, side_effects=[sha_fn.update]):
            fd_copy.write(data)
        backup_store.save(sha_to_path(sha_fn.hexdigest()), fd_copy)
    entry = ManifestEntry(abs_file_name, sha_fn.hexdigest())
    backup_store.manifest.insert_or_update(abs_file_name, entry, is_diff=False)


def _save_diff(abs_file_name: str, base: ManifestEntry, latest: ManifestEntry, backup_store: BackupStore) -> None:
    """ compute a diff between the new file and the original file, and save the diff to the backup store """

    with TemporaryFile() as fd_orig, open(abs_file_name, 'rb') as fd_new, TemporaryFile() as fd_new_diff:
        backup_store.load(sha_to_path(base.sha), fd_orig)
        sha = compute_diff(fd_orig, fd_new, fd_new_diff)

        # the file could have the same sha but still have changed, for example, if the permissions changed
        if sha != latest.sha:
            backup_store.save(sha_to_path(sha), fd_new_diff)
    entry = ManifestEntry(abs_file_name, sha)
    backup_store.manifest.insert_or_update(
        abs_file_name,
        entry,
        # this is a "real" diff if either the new sha is different from the most-recently-saved sha, or
        # if the most-recently-saved sha is different from the original sha
        is_diff=(entry.sha != latest.sha or base.sha != latest.sha),
    )


def backup(backup_name: str, backup_store: BackupStore, global_exclusions: List[Pattern] = None) -> None:
    """ Back up all files in a backup set to the specified backup store

    :param backup_name: the name of the backup set
    :param backup_store: where to save the backed-up files
    :param global_exclusions: files which match any of these patterns are excluded from the backup
    """
    global_exclusions = global_exclusions or []
    modified_files: set = set()
    marked_files: set = set()
    manifest_changed = False

    # First we scan all of the directories listed in the backup set and find those that have changed
    #
    # This is "safe" in the following senses:
    #   - if a file changes after it's already been scanned, it will get picked up "next time"
    #   - we don't rely on any data from the scan about the files, just whether it's changed or not; thus,
    #     if a file changes further between when we scan and when we back up, we're guaranteed to get the latest version
    for base_path in staticconf.read_list('directories', namespace=backup_name):
        abs_base_path = os.path.abspath(base_path)
        local_exclusions = compile_exclusions(staticconf.read_list('exclusions', [], namespace=backup_name))
        exclusions = global_exclusions + local_exclusions

        modified, marked = _scan_directory(abs_base_path, backup_store.manifest, exclusions)
        modified_files |= (modified)
        marked_files |= (marked)

    # Next, we back up all of the files that we discovered in the step above
    for abs_file_name in modified_files:
        try:
            base, latest = backup_store.manifest.get_diff_pair(abs_file_name)
            if not latest:  # the file hasn't been backed up before, or it's been deleted and re-created
                _save_copy(abs_file_name, backup_store)
            elif base and latest:  # the file has been backed up before, but has either changed contents or metadata
                _save_diff(abs_file_name, base, latest, backup_store)
            logger.info(f'Backed up {abs_file_name}')
        except Exception as e:
            # We never want to hard-fail a backup just because one file crashed; we'd rather back up as much
            # as we can, and log the failures for further investigation
            logger.exception(f'There was a problem backing up {abs_file_name}: {str(e)}; skipping')
            continue
        manifest_changed = True

    # Mark all files that weren't touched in the above loop as "deleted"
    # (we don't actually delete the file, just record that it's no longer present)
    for abs_file_name in backup_store.manifest.files() - marked_files:
        logger.info(f'{abs_file_name} has been deleted')
        backup_store.manifest.delete(abs_file_name)
        manifest_changed = True

    if manifest_changed:
        backup_store.save(MANIFEST_PATH, backup_store.manifest.stream())
        logger.info('Manifest saved')
