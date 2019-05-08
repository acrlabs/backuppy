import logging

import colorlog
import staticconf

from backuppy.args import parse_args

logger = colorlog.getLogger(__name__)


def setup_logging(log_level_str: str = 'info') -> None:
    handler = colorlog.StreamHandler()
    handler.setFormatter(colorlog.ColoredFormatter('%(log_color)s%(levelname)s:%(name)s:%(message)s'))
    logger = colorlog.getLogger()
    logger.addHandler(handler)

    log_level = getattr(logging, log_level_str.upper())
    logging.getLogger().setLevel(log_level)


def main():
    args = parse_args("BackupPY - an open-source backup tool")
    staticconf.DictConfiguration({
        'use_encryption': not args.disable_encryption,
        'use_compression': not args.disable_compression,
    })
    setup_logging(args.log_level)
    args.entrypoint(args)


if __name__ == '__main__':
    main()
