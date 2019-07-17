import argparse
import os
import stat
import time
from typing import List
from typing import Tuple

import staticconf
from tabulate import tabulate

from backuppy.args import subparser
from backuppy.manifest import QueryResponse
from backuppy.stores import get_backup_store
from backuppy.util import format_time
from backuppy.util import parse_time

DASHES = '-' * 80
SUMMARY_HEADERS: List[str] = ['filename', 'versions', 'last backup time']
DETAILS_HEADERS: List[str] = ['sha', 'uid', 'gid', 'permissions', 'backup time']


def _find_root_prefix(abs_file_name: str, backup_name: str) -> str:
    for directory in staticconf.read_list('directories', namespace=backup_name):
        abs_root = os.path.abspath(directory) + os.path.sep
        if abs_file_name.startswith(abs_root):
            return abs_root
    raise ValueError(f'{abs_file_name} does not start with any directory prefix')


def _print_summary(backup_name: str, search_results: List[QueryResponse]) -> None:
    contents: List[Tuple[str, int, str]] = []
    root_directory = _find_root_prefix(search_results[0][0], backup_name)

    for i, (abs_file_name, history) in enumerate(search_results):
        filename = abs_file_name[len(root_directory):]
        backup_time_str = format_time(history[0][1])
        contents.append((filename, len(history), backup_time_str))
        if i == len(search_results) - 1 or not search_results[i+1][0].startswith(root_directory):
            print(f'\n{DASHES}\n{root_directory}\n{DASHES}')
            print(tabulate(contents, headers=SUMMARY_HEADERS))
            if i != len(search_results) - 1:
                root_directory = _find_root_prefix(search_results[i+1][0], backup_name)
                contents = []
    print('')


def _print_details(backup_name: str, search_results: List[QueryResponse], sha_len: int) -> None:
    for abs_file_name, history in search_results:
        contents = [
            (
                (h.sha[:sha_len] + '...' if h.sha else None),
                h.uid,
                h.gid,
                (stat.filemode(h.mode) if h.mode else '<deleted>'),
                format_time(t),
            )
            for h, t in history
        ]
        print(f'\n{DASHES}\n{abs_file_name}\n{DASHES}')
        print(tabulate(contents, headers=DETAILS_HEADERS))
    print('')


def main(args: argparse.Namespace) -> None:
    after_timestamp = parse_time(args.after) if args.after else 0
    before_timestamp = parse_time(args.before) if args.before else int(time.time())

    staticconf.YamlConfiguration(args.config, flatten=False)
    backup_set_config = staticconf.read('backups')[args.name]
    staticconf.DictConfiguration(backup_set_config, namespace=args.name)
    backup_store = get_backup_store(args.name)

    with backup_store.open_manifest():
        search_results = backup_store.manifest.search(
            like=args.like,
            after_timestamp=after_timestamp,
            before_timestamp=before_timestamp,
            file_limit=args.file_limit,
            history_limit=args.history_limit,
        )
    if not args.details:
        _print_summary(args.name, search_results)
    else:
        _print_details(args.name, search_results, args.sha_length)


@subparser('list', 'list the contents of a backup set', main)
def add_list_parser(subparser) -> None:  # pragma: no cover
    subparser.add_argument(
        dest='like',
        type=str,
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
        '--after',
        metavar='TIME',
        help='Only list files backed up after this time',
    )
    subparser.add_argument(
        '--before',
        metavar='TIME',
        help='Only list files backed up before this time',
    )
    subparser.add_argument(
        '--file-limit',
        type=int,
        help='Show at most this many files',
    )
    subparser.add_argument(
        '--history-limit',
        type=int,
        default=None,
        help="Show at most this many entries of the file's history",
    )
    subparser.add_argument(
        '--details',
        action='store_true',
        help='Print details of the files stored in the backup set',
    )
    subparser.add_argument(
        '--sha-length',
        type=int,
        default=8,
        help='Length of the sha to display in detailed view (no effect in summary view)',
    )
