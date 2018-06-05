import os
from tempfile import TemporaryFile

import staticconf

from backuppy.blob import compute_diff
from backuppy.io import ReadSha
from backuppy.manifest import ManifestEntry
from backuppy.stores.backup_store import MANIFEST_PATH
from backuppy.util import compile_exclusions
from backuppy.util import file_walker
from backuppy.util import get_color_logger
from backuppy.util import sha_to_path

logger = get_color_logger(__name__)


def _scan_directory(abs_base_path, manifest, exclusions):
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


def _save_copy(abs_file_name, backup_store):
    read_sha = ReadSha(abs_file_name)
    with read_sha as fd_orig, TemporaryFile() as fd_copy:
        while True:
            data = fd_orig.read()
            if not data:
                break
            fd_copy.write(data)
        fd_copy.seek(0)
        backup_store.save(sha_to_path(read_sha.hexdigest), fd_copy)
    entry = ManifestEntry(abs_file_name, read_sha.hexdigest)
    backup_store.manifest.insert_or_update(abs_file_name, entry, is_diff=False)


def _save_diff(abs_file_name, base, latest, backup_store):
    read_sha = ReadSha(abs_file_name)

    with TemporaryFile() as fd_orig, read_sha as fd_new, TemporaryFile() as fd_new_diff:
        backup_store.load(sha_to_path(base.sha), fd_orig)
        compute_diff(fd_orig, fd_new, fd_new_diff)
        if read_sha.hexdigest != latest.sha:
            backup_store.save(sha_to_path(read_sha.hexdigest), fd_new_diff)
    entry = ManifestEntry(abs_file_name, read_sha.hexdigest)
    backup_store.manifest.insert_or_update(
        abs_file_name,
        entry,
        is_diff=(entry.sha != latest.sha or base.sha != latest.sha),
    )


def backup(backup_name, backup_store, global_exclusions=None):
    global_exclusions = global_exclusions or []
    modified_files, marked_files = set(), set()
    manifest_changed = False

    for base_path in staticconf.read_list('directories', namespace=backup_name):
        abs_base_path = os.path.abspath(base_path)
        local_exclusions = compile_exclusions(staticconf.read_list('exclusions', [], namespace=backup_name))
        exclusions = global_exclusions + local_exclusions

        modified, marked = _scan_directory(abs_base_path, backup_store.manifest, exclusions)
        modified_files |= (modified)
        marked_files |= (marked)

    for abs_file_name in modified_files:
        try:
            base, latest = backup_store.manifest.get_diff_pair(abs_file_name)
            if not latest:
                _save_copy(abs_file_name, backup_store)
            else:
                _save_diff(abs_file_name, base, latest, backup_store)
            logger.info(f'Backed up {abs_file_name}')
        except Exception:
            logger.exception(f'There was a problem backing up {abs_file_name}: skipping')
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
