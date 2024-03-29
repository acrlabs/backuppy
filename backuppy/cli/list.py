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
from backuppy.util import format_sha
from backuppy.util import format_time
from backuppy.util import parse_time

DASHES = '-' * 80
SUMMARY_HEADERS: List[str] = ['filename', 'versions', 'del?', 'last backup time']
DETAILS_HEADERS: List[str] = ['sha', 'uid', 'gid', 'permissions', 'backup time']


def _split_root_prefix(abs_file_name: str, backup_name: str) -> Tuple[str, str]:
    for directory in staticconf.read_list('directories', namespace=backup_name):  # type: ignore[attr-defined]
        abs_root = os.path.abspath(directory) + os.path.sep
        if abs_file_name.startswith(abs_root):
            return abs_root, abs_file_name[len(abs_root):]
    raise ValueError(f'{abs_file_name} does not start with any directory prefix')


def _print_summary(
    backup_name: str,
    search_results: List[QueryResponse],
    deleted_only: bool,
    changed_only: bool,
) -> None:
    contents: List[Tuple[str, int, str, str]] = []

    for i, (abs_file_name, history) in enumerate(search_results):
        root_directory, filename = _split_root_prefix(search_results[i][0], backup_name)
        backup_time_str = format_time(history[0].commit_timestamp)
        deleted_str = '' if history[0].sha else 'y'

        # If the deleted_only flag is present and the SHA is not None, this file _wasn't_
        # deleted, so don't show it.  Similarly, if the changed_only flag is present and the
        # history length is 1, don't show it.  A file has to have a history of at least 2 if
        # it's been deleted so these can be present in the same check.
        if (not (deleted_only and history[0].sha)) and (not changed_only or len(history) > 1):
            contents.append((filename, len(history), deleted_str, backup_time_str))

        if i == len(search_results) - 1 or not search_results[i+1][0].startswith(root_directory):
            print(f'\n{DASHES}\n{root_directory}\n{DASHES}')
            print(tabulate(contents, headers=SUMMARY_HEADERS))
            if i != len(search_results) - 1:
                contents = []
    print('')


def _print_details(
    backup_name: str,
    search_results: List[QueryResponse],
    deleted_only: bool,
    changed_only: bool,
    sha_length: int,
) -> None:
    for abs_file_name, history in search_results:
        if (not (deleted_only and history[0].sha)) and (not changed_only or len(history) > 1):
            contents = [
                (
                    format_sha(h.sha, sha_length),
                    h.uid,
                    h.gid,
                    (stat.filemode(h.mode) if h.mode else '<deleted>'),
                    format_time(h.commit_timestamp),
                )
                for h in history
            ]
            print(f'\n{DASHES}\n{abs_file_name}\n{DASHES}')
            print(tabulate(contents, headers=DETAILS_HEADERS))
    print('')


def main(args: argparse.Namespace) -> None:
    after_timestamp = parse_time(args.after) if args.after else 0
    before_timestamp = parse_time(args.before) if args.before else int(time.time())

    backup_store = get_backup_store(args.name)
    with backup_store.unlock():
        search_results = backup_store.manifest.search(
            after_timestamp=after_timestamp,
            before_timestamp=before_timestamp,
            file_limit=args.file_limit,
            history_limit=args.history_limit,
            like=args.like,
        )
    if not args.details:
        _print_summary(args.name, search_results, args.deleted, args.changed)
    else:
        _print_details(args.name, search_results, args.deleted, args.changed, args.sha_length)


@subparser('list', 'list the contents of a backup set', main)
def add_list_parser(subparser) -> None:  # pragma: no cover
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
        '--deleted',
        action='store_true',
        help='Only show deleted files',
    )
    subparser.add_argument(
        '--changed',
        action='store_true',
        help='Only show files with more than one version',
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
