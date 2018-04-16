import os
import re

from backuppy.exceptions import BackupFailedError
from backuppy.manifest import ManifestEntry
from backuppy.util import file_walker
from backuppy.util import get_color_logger

logger = get_color_logger(__name__)


def _compile_exclusions(config):
    return [re.compile(excl) for excl in config.get('exclusions', [])]


def _backup_file(abs_file_name):
    try:
        return ManifestEntry(abs_file_name)
    except OSError as e:
        raise BackupFailedError from e


def backup(manifest, location, config):

    marked_files = set()
    manifest_files = manifest.tracked_files()
    global_exclusions = _compile_exclusions(config)
    for base_path, base_path_config in config['directories'].items():
        abs_base_path = os.path.abspath(base_path)
        local_exclusions = _compile_exclusions(base_path_config) if base_path_config else []
        exclusions = global_exclusions + local_exclusions

        for abs_file_name in file_walker(abs_base_path, logger.warn):

            # Skip files that match any of the specified regular expressions
            matched_patterns = [pattern for pattern in exclusions if pattern.search(abs_file_name)]
            if matched_patterns:
                logger.info(f'{abs_file_name} matched exclusion pattern(s) "{matched_patterns}"; skipping')
                continue

            marked_files.add(abs_file_name)
            if manifest.is_current(abs_file_name):
                logger.info(f'{abs_file_name} is up-to-date; skipping')
                continue

            try:
                manifest_entry = _backup_file(abs_file_name)
            except BackupFailedError as e:
                logger.warn(f'There was a problem backing up {abs_file_name}: {str(e)}; skipping')
                continue

            manifest.insert_or_update(abs_file_name, manifest_entry)

            # Mark the file as "seen" so it isn't deleted later
            logger.info(f'Backed up {abs_file_name}')

    # Mark all files that weren't touched in the above loop as "deleted"
    # (we don't actually delete the file, just record that it's no longer present)
    for name in manifest_files - marked_files:
        logger.info(f'{abs_file_name} has been deleted')
        manifest.delete(name)
