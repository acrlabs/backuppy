import argparse
import os
from typing import List
from typing import Pattern
from typing import Set

import colorlog
import staticconf

from backuppy.args import add_preserve_scratch_arg
from backuppy.args import subparser
from backuppy.stores import get_backup_store
from backuppy.stores.backup_store import BackupStore
from backuppy.util import compile_exclusions
from backuppy.util import file_walker

logger = colorlog.getLogger(__name__)


def _scan_directory(
    abs_base_path: str,
    backup_store: BackupStore,
    exclusions: List[Pattern],
) -> Set[str]:
    """ scan a directory looking for changes from the manifest

    :param abs_base_path: the root of the directory to scan
    :param backup_store: the BackupStore object that should be used to back up the directory
    :param exclusions: a list of files to ignore during backup
    """
    backup_store.manifest.files()
    marked_files = set()
    for abs_file_name in file_walker(abs_base_path, logger.warning):

        # Skip files that match any of the specified regular expressions
        matched_patterns = [
            pattern.pattern for pattern in exclusions if pattern.search(abs_file_name)
        ]
        if matched_patterns:
            logger.info(f'{abs_file_name} matched exclusion(s) "{matched_patterns}"; skipping')
            continue

        # Mark the file as "seen" so it isn't deleted later
        marked_files.add(abs_file_name)

        try:
            backup_store.save_if_new(abs_file_name)
        except Exception as e:
            # We never want to hard-fail a backup just because one file crashed; we'd rather back up
            # as much as we can, and log the failures for further investigation
            logger.exception(f'There was a problem backing up {abs_file_name}: {str(e)}; skipping')
            continue

    # Mark all files that weren't touched in the above loop as "deleted"
    # (we don't actually delete the file, just record that it's no longer present)
    return marked_files


def main(args: argparse.Namespace) -> None:
    """ entry point for the 'backup' subcommand """
    for backup_name, backup_config in staticconf.read('backups').items():
        logger.info(f'Starting backup for {backup_name}')
        backup_store = get_backup_store(backup_name)

        with backup_store.unlock(args.preserve_scratch_dir):
            marked_files: Set[str] = set()
            for base_path in staticconf.read_list('directories', namespace=backup_name):
                abs_base_path = os.path.abspath(base_path)
                exclusions = compile_exclusions(
                    staticconf.read_list('exclusions', [], namespace=backup_name)
                )
                marked_files |= _scan_directory(abs_base_path, backup_store, exclusions)

            for abs_file_name in backup_store.manifest.files() - marked_files:
                logger.info(f'{abs_file_name} has been deleted')
                backup_store.manifest.delete(abs_file_name)
        logger.info(f'Backup for {backup_name} finished')


@subparser('backup', 'perform a backup of all configured locations', main)
def add_backup_parser(subparser) -> None:  # pragma: no cover
    add_preserve_scratch_arg(subparser)
