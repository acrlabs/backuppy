import argparse
import os

import yaml

from backuppy.manifest import Manifest


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--manifest',
        help='Backup manifest to read',
    )
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


def backup(manifest, location, config):
    if args.manifest:
        manifest = Manifest.load(args.manifest)
    else:
        manifest = Manifest(config['directories'].keys())

    exclusions = {
        os.path.abspath(path): pathconf.get('exclusions', [])
        for path, pathconf in config['directories'].items()
        if pathconf
    }
    manifest.update(exclusions)
    manifest.save(os.path.join(args.backup_location, 'manifest'))


def restore(manifest, location, timestamp):
    pass


def main(args):
    if args.backup_location:
        with open(args.config) as f:
            config = yaml.load(f)
        backup(args.manifest, args.backup_location, config)

    elif args.restore_location:
        restore(args.restore_location)


if __name__ == '__main__':
    args = parse_args()
    main(args)
