import argparse
import os
import time
from typing import List
from typing import Optional
from typing import Tuple

import staticconf
from tabulate import tabulate

from backuppy.args import add_preserve_scratch_arg
from backuppy.args import subparser
from backuppy.blob import apply_diff
from backuppy.io import IOIter
from backuppy.manifest import ManifestEntry
from backuppy.stores import get_backup_store
from backuppy.stores.backup_store import BackupStore
from backuppy.util import ask_for_confirmation
from backuppy.util import format_sha
from backuppy.util import format_time
from backuppy.util import parse_time
from backuppy.util import path_join

RESTORE_LIST_HEADERS = ['filename', 'sha', 'backup time']
SHA_LENGTH = 8


def _parse_destination(dest_input: Optional[str], backup_name: str) -> Tuple[str, str]:
    dest_input = dest_input or '.'
    # don't use path_join here because it wipes out the './' prefix
    destination_str = os.path.join(dest_input, backup_name)
    destination = os.path.abspath(destination_str)
    if dest_input != '.' and not os.path.isabs(destination_str):
        destination_str = os.path.join('.', destination_str)
    return destination, destination_str


def _confirm_restore(
    files_to_restore: List[ManifestEntry],
    destination: str,
    destination_str: str,
) -> bool:
    print('')
    if os.path.exists(destination):
        print(
            f'WARNING: {destination_str} already exists.  '
            'Files in this location may be overwritten.'
        )
    print(f'Backuppy will restore the following files to {destination_str}:\n')
    print(tabulate([
        [
            f.abs_file_name,
            format_sha(f.sha, SHA_LENGTH),
            format_time(f.commit_timestamp),
        ]
        for f in files_to_restore],
        headers=RESTORE_LIST_HEADERS,
    ))
    print('')
    return ask_for_confirmation('Continue?')


def _restore(
    files_to_restore: List[ManifestEntry],
    destination: str,
    backup_store: BackupStore,
) -> None:
    print('Beginning restore...')
    os.makedirs(destination, exist_ok=True)
    for f in files_to_restore:
        restore_file_name = path_join(destination, f.abs_file_name[1:])

        with IOIter() as orig_file, \
                IOIter() as diff_file, \
                IOIter(restore_file_name) as restore_file:

            if f.base_sha:
                assert f.base_key_pair  # make mypy happy; this cannot be None here
                backup_store.load(f.base_sha, orig_file, f.base_key_pair)
                backup_store.load(f.sha, diff_file, f.key_pair)
                apply_diff(orig_file, diff_file, restore_file)
            else:
                backup_store.load(f.sha, restore_file, f.key_pair)
    print('Restore complete!\n')


def main(args: argparse.Namespace) -> None:
    destination, destination_str = _parse_destination(args.dest, args.name)
    before_timestamp = parse_time(args.before) if args.before else int(time.time())

    staticconf.YamlConfiguration(args.config, flatten=False)
    backup_set_config = staticconf.read('backups')[args.name]
    staticconf.DictConfiguration(backup_set_config, namespace=args.name)
    backup_store = get_backup_store(args.name)

    with backup_store.unlock(args.preserve_scratch_dir):
        files_to_restore: List[ManifestEntry]
        if args.sha:
            files_to_restore = backup_store.manifest.get_entries_by_sha(args.sha)
            if not files_to_restore:
                raise ValueError(f'Sha {args.sha} does not match anything in the store')

        else:
            search_results = backup_store.manifest.search(
                like=args.like,
                before_timestamp=before_timestamp,
                history_limit=1,
            )
            # Restore the most recent version of all files that haven't been deleted
            files_to_restore = [h[-1] for _, h in search_results if h[-1].sha]

        if _confirm_restore(files_to_restore, destination, destination_str):
            _restore(files_to_restore, destination, backup_store)


@subparser('restore', 'restore files from a backup set', main)
def add_restore_parser(subparser) -> None:  # pragma: no cover
    subparser.add_argument(
        dest='like',
        metavar='QUERY',
        default=None,
        nargs='?',
        help='Query string to search the backup set for',
    )
    subparser.add_argument(
        '--name',
        required=True,
        help='Name of the backup set to examine'
    )
    subparser.add_argument(
        '--before',
        metavar='TIME',
        help='Restore the most recent version of the file backed up before this time',
    )
    subparser.add_argument(
        '--sha',
        help='Restore the file corresponding to this SHA',
    )
    subparser.add_argument(
        '--dest',
        help='Location to restore the file(s) to (default: current directory)'
    )
    add_preserve_scratch_arg(subparser)
