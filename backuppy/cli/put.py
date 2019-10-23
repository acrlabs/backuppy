import argparse

import staticconf

from backuppy.args import add_name_arg
from backuppy.args import subparser
from backuppy.manifest import lock_manifest
from backuppy.manifest import Manifest
from backuppy.stores import get_backup_store


def main(args: argparse.Namespace) -> None:
    staticconf.YamlConfiguration(args.config, flatten=False)
    backup_set_config = staticconf.read('backups')[args.name]
    staticconf.DictConfiguration(backup_set_config, namespace=args.name)
    backup_store = get_backup_store(args.name)

    if args.manifest:
        manifest = Manifest(args.filename)
        private_key_filename = backup_store.config.read('private_key_filename', default='')
        lock_manifest(
            manifest,
            private_key_filename,
            backup_store._save,
            backup_store._load,
            backup_store.options,
        )
    else:
        with backup_store.unlock():
            backup_store.save_if_new(args.filename)


HELP_TEXT = '''
WARNING: this command is considered "plumbing" and should be used for debugging or
exceptional cases only.  You can render your backup store inaccessible if it is used
incorrectly.  Use at your own risk!
'''


@subparser('put', HELP_TEXT, main)
def add_put_parser(subparser) -> None:  # pragma: no cover
    add_name_arg(subparser)
    subparser.add_argument(
        dest='filename',
        help='File to store in the backup'
    )
    subparser.add_argument(
        '--manifest',
        action='store_true',
        help='Save the file as manifest in the backup store (THIS CAN RENDER YOUR BACKUP UNUSABLE)',
    )
