import argparse
import sys
import zlib
from typing import List

import staticconf

from backuppy.args import add_preserve_scratch_arg
from backuppy.args import subparser
from backuppy.io import compute_sha
from backuppy.io import IOIter
from backuppy.manifest import ManifestEntry
from backuppy.stores import get_backup_store
from backuppy.stores.backup_store import BackupStore
from backuppy.util import ask_for_confirmation

RESTORE_LIST_HEADERS = ['filename', 'sha', 'backup time']
SHA_LENGTH = 8


def _verify(files_to_verify: List[ManifestEntry], backup_store: BackupStore, show_all: bool) -> None:
    print('Beginning verification...')
    for f in files_to_verify:

        check_str = f'Checking {f.abs_file_name}...'
        verified = False
        with IOIter() as orig_file, \
                IOIter() as diff_file, \
                IOIter() as restore_file:

            try:
                backup_store.restore_entry(f, orig_file, diff_file, restore_file)
                sha = compute_sha(restore_file)
                if sha != f.sha:
                    check_str += ' ERROR -- SHAs do not match.\n'
                else:
                    check_str += ' OK!\n'
                    verified = True
            except zlib.error as e:
                check_str += f' ERROR -- could not decompress data.  {str(e)}\n'

        if not verified or show_all:
            sys.stdout.write(check_str)

        if not verified and ask_for_confirmation('Backed up file is corrupt; fix?'):
            backup_store.save_if_new(f.abs_file_name, force_copy=True)

    print('Verification complete!\n')


def main(args: argparse.Namespace) -> None:
    staticconf.DictConfiguration({'yes': args.yes})

    staticconf.YamlConfiguration(args.config, flatten=False)
    backup_set_config = staticconf.read('backups')[args.name]
    staticconf.DictConfiguration(backup_set_config, namespace=args.name)
    backup_store = get_backup_store(args.name)

    with backup_store.unlock(preserve_scratch=args.preserve_scratch_dir):
        files_to_verify: List[ManifestEntry]
        if args.sha:
            files_to_verify = backup_store.manifest.get_entries_by_sha(args.sha)
            if not files_to_verify:
                raise ValueError(f'Sha {args.sha} does not match anything in the store')

        else:
            search_results = backup_store.manifest.search(
                like=args.like,
            )
            # Verify the most recent version of all files that haven't been deleted
            files_to_verify = [h[0] for _, h in search_results if h[0].sha]

        _verify(files_to_verify, backup_store, args.show_all)


@subparser('verify', 'verify file integrity in a backup set', main)
def add_verify_parser(subparser) -> None:  # pragma: no cover
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
        '--sha',
        help='Restore the file corresponding to this SHA',
    )
    subparser.add_argument(
        '--show-all',
        action='store_true',
        help='Show verification status for all files instead of just corrupted files',
    )
    subparser.add_argument(
        '-y', '--yes',
        action='store_true',
        help='Answer yes to all prompts',
    )
    add_preserve_scratch_arg(subparser)
