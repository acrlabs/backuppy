import argparse

import staticconf

from backuppy.args import add_name_arg
from backuppy.args import subparser
from backuppy.crypto import decrypt_and_unpack
from backuppy.io import IOIter
from backuppy.manifest import get_manifest_keypair
from backuppy.manifest import MANIFEST_PREFIX
from backuppy.stores import get_backup_store
from backuppy.stores.backup_store import BackupStore
from backuppy.util import sha_to_path

ACTIONS = ['fetch', 'decrypt', 'unpack']


def _get(
    filename: str,
    key_pair: bytes,
    backup_store: BackupStore,
    action,
) -> None:
    print(f'Fetching {filename}...')
    with IOIter() as encrypted_local_file, IOIter(filename) as local_file:
        if action == 'fetch':
            options = {'use_encryption': False, 'use_compression': False}
        elif action == 'decrypt':
            options = {'use_compression': False}
        else:
            options = {}
        to_fetch = filename if filename.startswith(MANIFEST_PREFIX) else sha_to_path(filename)
        backup_store._load(to_fetch, encrypted_local_file)
        decrypt_and_unpack(
            encrypted_local_file,
            local_file,
            key_pair,
            {**backup_store.options, **options},  # type: ignore
        )
    print('Done!\n')


def main(args: argparse.Namespace) -> None:
    if args.sha is not None and args.manifest is not None:
        raise ValueError('Cannot specify both SHA and manifest')

    staticconf.YamlConfiguration(args.config, flatten=False)
    backup_set_config = staticconf.read('backups')[args.name]
    staticconf.DictConfiguration(backup_set_config, namespace=args.name)
    backup_store = get_backup_store(args.name)

    with backup_store.unlock():
        if args.sha:
            entries = backup_store.manifest.get_entries_by_sha(args.sha)
            if not entries:
                raise ValueError(f'Sha {args.sha} does not match anything in the store')

            # All the entries corresponding to this sha should be the same, so just use
            # the first one
            filename, key_pair = entries[0].sha, entries[0].key_pair
        else:
            filename = sorted(backup_store._query(MANIFEST_PREFIX), reverse=True)[args.manifest][1:]
            private_key_filename = backup_store.config.read('private_key_filename', default='')
            key_pair = get_manifest_keypair(filename, private_key_filename, backup_store._load)
        _get(filename, key_pair, backup_store, args.action)


HELP_TEXT = '''
WARNING: this command is considered "plumbing" and should be used for debugging or
exceptional cases only.  You can render your backup store inaccessible if it is used
incorrectly.  Use at your own risk!
'''


@subparser('get', HELP_TEXT, main)
def add_get_parser(subparser) -> None:  # pragma: no cover
    add_name_arg(subparser)
    subparser.add_argument(
        '--sha',
        help='Restore the file corresponding to this SHA',
    )
    subparser.add_argument(
        '--manifest',
        type=int,
        help='Retrieve a manifest from the backup store, index from 0 (most recent) to n (oldest)',
    )
    subparser.add_argument(
        '--action',
        choices=ACTIONS,
        default='unpack',
        help='Actions to take on the retrieved file',
    )
