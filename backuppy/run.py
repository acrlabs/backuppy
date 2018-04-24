import argparse

import staticconf

from backuppy.backup import backup
from backuppy.stores import get_backup_store
from backuppy.util import compile_exclusions
from backuppy.util import get_color_logger

logger = get_color_logger(__name__)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--config',
        default='backuppy.conf',
        help='Config file to load specifying what to back up',
    )
    parser.add_argument(
        '--mode',
        choices=['backup', 'restore'],
        default='backup',
        help='Mode of operation',
    )
    return parser.parse_args()


def restore(manifest, location, timestamp):
    pass


def main(args):
    if args.mode == 'backup':
        staticconf.YamlConfiguration(args.config, flatten=False)
        global_exclusions = compile_exclusions(staticconf.read_list('exclusions', []))
        for backup_name, backup_config in staticconf.read('backups').items():
            staticconf.DictConfiguration(backup_config, namespace=backup_name)
            logger.info(f'Starting backup for {backup_name}')
            backup_store = get_backup_store(backup_name)
            backup(backup_name, backup_store, global_exclusions)
            logger.info(f'Backup for {backup_name} finished')

    elif args.mode == 'restore':
        restore(args.restore_location)


if __name__ == '__main__':
    args = parse_args()
    main(args)
