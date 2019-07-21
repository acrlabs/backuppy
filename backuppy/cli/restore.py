import argparse
import os
import time
from typing import Optional
from typing import Tuple

import staticconf
from tabulate import tabulate

from backuppy.args import subparser
from backuppy.blob import apply_diff
from backuppy.io import IOIter
from backuppy.stores import get_backup_store
from backuppy.util import ask_for_confirmation
from backuppy.util import format_time
from backuppy.util import parse_time

RESTORE_LIST_HEADERS = ['filename', 'backup time']


def _parse_destination(dest_input: Optional[str]) -> Tuple[str, str]:
    if dest_input:
        destination = os.path.abspath(dest_input)
        if os.path.isabs(dest_input):
            destination_str = dest_input
        else:
            destination_str = os.path.join('.', os.path.normpath(dest_input))
    else:
        destination = os.path.abspath('.')
        destination_str = 'the current directory'
    return destination, destination_str


def main(args: argparse.Namespace) -> None:
    destination, destination_str = _parse_destination(args.dest)
    before_timestamp = parse_time(args.before) if args.before else int(time.time())

    staticconf.YamlConfiguration(args.config, flatten=False)
    backup_set_config = staticconf.read('backups')[args.name]
    staticconf.DictConfiguration(backup_set_config, namespace=args.name)
    backup_store = get_backup_store(args.name)

    with backup_store.open_manifest():
        if args.sha:
            raise NotImplementedError('not yet working TODO')

        search_results = backup_store.manifest.search(
            like=args.like,
            before_timestamp=before_timestamp,
            history_limit=1,
        )

        print(f'\nBackuppy will restore the following files to {destination_str}:\n')
        print(tabulate([
            [f, format_time(h[-1][1])] for f, h in search_results],
            headers=RESTORE_LIST_HEADERS,
        ))
        print('')
        if not ask_for_confirmation('Continue?'):
            return
        print('Beginning restore...')

        os.makedirs(destination, exist_ok=True)
        for abs_file_name, history in search_results:
            entry = history[-1][0]
            restore_file_name = os.path.join(destination, os.path.basename(abs_file_name))
            with IOIter() as orig_file, \
                    IOIter() as diff_file, \
                    IOIter(restore_file_name) as restore_file:

                if entry.base_sha:
                    backup_store.load(entry.base_sha, orig_file)
                    backup_store.load(entry.sha, diff_file)
                    apply_diff(orig_file, diff_file, restore_file)
                else:
                    backup_store.load(entry.sha, restore_file)

        print('Restore complete!\n')


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
