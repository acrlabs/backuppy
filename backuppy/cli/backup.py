import argparse

import colorlog
import staticconf

from backuppy.args import subparser
from backuppy.backup import backup
from backuppy.stores import get_backup_store
from backuppy.util import compile_exclusions

logger = colorlog.getLogger(__name__)


def main(args: argparse.Namespace):
    staticconf.YamlConfiguration(args.config, flatten=False)
    global_exclusions = compile_exclusions(staticconf.read_list('exclusions', []))
    for backup_name, backup_config in staticconf.read('backups').items():
        staticconf.DictConfiguration(backup_config, namespace=backup_name)
        logger.info(f'Starting backup for {backup_name}')
        backup_store = get_backup_store(backup_name)
        backup(backup_name, backup_store, global_exclusions)
        logger.info(f'Backup for {backup_name} finished')


@subparser('backup', 'perform a backup of all configuration locations', main)
def add_backup_parser(subparser, required_named_args, optional_named_args) -> None:
    pass
