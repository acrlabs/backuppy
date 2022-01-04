import argparse
import os.path
from collections import defaultdict
from typing import Dict
from typing import List

import colorlog
import staticconf

from backuppy.args import add_preserve_scratch_arg
from backuppy.args import subparser
from backuppy.exceptions import MismatchedSHAError
from backuppy.io import compute_sha
from backuppy.io import IOIter
from backuppy.manifest import ManifestEntry
from backuppy.stores import get_backup_store
from backuppy.stores.backup_store import BackupStore
from backuppy.util import ask_for_confirmation

logger = colorlog.getLogger(__name__)


def _check_entry(entry: ManifestEntry, backup_store: BackupStore):
    with IOIter() as orig_file, \
            IOIter() as diff_file, \
            IOIter() as restore_file:

        backup_store.restore_entry(entry, orig_file, diff_file, restore_file)
        sha = compute_sha(restore_file)
        if sha != entry.sha:
            raise MismatchedSHAError(f'SHAs for {entry.abs_file_name} do not match')


def _fix_duplicate_entries(backup_store: BackupStore):
    print('Checking for duplicate entries...')
    entries = backup_store.manifest.find_duplicate_entries()
    grouped_entries = defaultdict(list)
    for e in entries:
        grouped_entries[(e.abs_file_name, e.sha, e.uid, e.gid, e.mode)].append(e)

    for (filename, sha, _, _, _), entries in grouped_entries.items():
        print(
            f'ERROR: Found {len(entries)} duplicate entries for ({filename}, {sha}), '
            'trying to clean up...'
        )
        found_good_entry = False
        for entry in sorted(entries, key=lambda e: e.commit_timestamp, reverse=True):
            if not found_good_entry:
                try:
                    _check_entry(entry, backup_store)
                    found_good_entry = True
                    print(f'Entry backed up at {entry.commit_timestamp} seems good.')
                    continue
                except Exception as e:
                    print(f'ERROR: entry backed up at {entry.commit_timestamp} is corrupt: {str(e)}')

            if ask_for_confirmation(f'Delete entry backed up at {entry.commit_timestamp}?'):
                backup_store.manifest.delete_entry(entry)

        if not found_good_entry and \
                ask_for_confirmation(f'No valid entries for {filename}; save new version?'):
            logger.warning(f'Saving new version of {filename}')
            if os.path.exists(entry.abs_file_name):
                backup_store.save_if_new(entry.abs_file_name, force_copy=True)
            else:
                backup_store.manifest.delete(entry.abs_file_name)
    print('Duplicate entry checks complete!\n')


def _fix_shas_with_multiple_key_pairs(backup_store: BackupStore):
    print('Checking for SHAs with multiple key-pairs...')
    shas_with_multiple_key_pairs = backup_store.manifest.find_shas_with_multiple_key_pairs()
    grouped_entries = defaultdict(list)
    for e in shas_with_multiple_key_pairs:
        grouped_entries[e.sha].append(e)

    for sha, entries in grouped_entries.items():
        print(
            f'ERROR: Found {len(entries)} entries for {sha} with different key_pairs, '
            'trying to clean up...'
        )
        good_key_pair = None
        for entry in entries:
            try:
                _check_entry(entry, backup_store)
                good_key_pair = entry.key_pair
                print(f'Entry backed up at {entry.commit_timestamp} seems good.')
                break
            except Exception as e:
                print(f'ERROR: entry backed up at {entry.commit_timestamp} is corrupt: {str(e)}')

        if good_key_pair is not None and ask_for_confirmation('Fix all entries?'):
            logger.warning(f'Updating all entries with sha {sha}')
            for entry in entries:
                entry.key_pair = good_key_pair
                backup_store.manifest.insert_or_update(entry)

        elif good_key_pair is None and \
                ask_for_confirmation(f'No valid entries for {sha}; save new version?'):
            logger.warning(f'Saving new version of {entries[0].abs_file_name}')
            backup_store.save_if_new(entries[0].abs_file_name, force_copy=True)
    print('Multiple key-pair check complete!\n')


def _verify(entries: List[ManifestEntry], backup_store: BackupStore, show_all: bool) -> None:
    print('Beginning verification...')

    # Because we might be fixing things as we go, we need to keep track of what
    # we've fixed so we don't needlessly overwrite data
    fixed_shas: Dict[str, bytes] = dict()
    for entry in entries:

        check_str = f'Checking {entry.abs_file_name}...'
        if entry.sha in fixed_shas:
            entry.key_pair = fixed_shas[entry.sha]
        if entry.base_sha in fixed_shas:
            entry.base_key_pair = fixed_shas[entry.base_sha]

        verified = False
        try:
            _check_entry(entry, backup_store)
            verified = True
            check_str += ' OK!'
        except Exception as e:
            check_str += f' ERROR: {str(e)}'

        if not verified or show_all:
            print(check_str)

        if not verified and ask_for_confirmation('Backed up file is corrupt; fix?'):
            new_entry = backup_store.save_if_new(entry.abs_file_name, force_copy=True)
            if new_entry:
                fixed_shas[new_entry.sha] = new_entry.key_pair

    print('Verification complete!\n')


def _fast_verify(backup_store: BackupStore) -> None:
    print('Beginning fast verification...\n')
    _fix_shas_with_multiple_key_pairs(backup_store)
    _fix_duplicate_entries(backup_store)
    print('Verification complete!\n')


def main(args: argparse.Namespace) -> None:
    staticconf.DictConfiguration({'yes': args.yes})

    backup_store = get_backup_store(args.name)

    with backup_store.unlock(preserve_scratch=args.preserve_scratch_dir):
        files_to_verify: List[ManifestEntry]
        if args.fast:
            _fast_verify(backup_store)
            return

        elif args.sha:
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
    subparser.add_argument(
        '--fast',
        action='store_true',
        help='Quick verification (just check manifest for consistency)'
    )
    add_preserve_scratch_arg(subparser)
