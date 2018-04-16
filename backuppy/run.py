import argparse
import os

import yaml

from backuppy.backup import backup
from backuppy.manifest import Manifest


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--config',
        default='backuppy.conf',
        help='Config file to load specifying what to back up',
    )
    parser.add_argument(
        '--backup-location',
        help='root directory to save backed-up files in',
    )
    parser.add_argument(
        '--restore-location',
        help='root directory to restore files to',
    )
    return parser.parse_args()


def restore(manifest, location, timestamp):
    pass


def main(args):
    if args.backup_location:
        with open(args.config) as f:
            config = yaml.load(f)

        manifest_file = os.path.join(args.backup_location, 'manifest')
        if os.path.isfile(manifest_file):
            manifest = Manifest.load(manifest_file)
        else:
            manifest = Manifest()

        backup(manifest, args.backup_location, config)
        manifest.save(manifest_file)

    elif args.restore_location:
        restore(args.restore_location)


if __name__ == '__main__':
    args = parse_args()
    main(args)
