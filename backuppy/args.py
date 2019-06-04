import argparse
import sys
from typing import Callable

import colorlog

from backuppy import __version__

logger = colorlog.getLogger(__name__)


class CustomHelpFormatter(argparse.ArgumentDefaultsHelpFormatter):  # pragma: no cover
    def __init__(self, prog):
        super().__init__(prog, max_help_position=35, width=100)


def subparser(command: str, help: str, entrypoint: Callable) -> Callable:  # pragma: no cover
    """ Function decorator to simplify adding arguments to subcommands

    :param command: name of the subcommand to add
    :param help: help string for the subcommand
    :param entrypoint: the 'main' function for the subcommand to execute
    """
    def decorator(add_args):
        def wrapper(subparser):
            subparser = subparser.add_parser(command, formatter_class=CustomHelpFormatter, add_help=False)
            add_args(subparser)
            subparser.add_argument('-h', '--help', action='help', help='show this message and exit')
            subparser.set_defaults(entrypoint=entrypoint)
        return wrapper
    return decorator


def parse_args(description: str) -> argparse.Namespace:  # pragma: no cover
    """Set up parser for the CLI tool and any subcommands

    :param description: a string descripting the tool
    :returns: a tuple of the parsed command-line options with their values
    """

    root_parser = argparse.ArgumentParser(
        prog='backuppy',
        description=description,
        formatter_class=CustomHelpFormatter,
    )
    root_parser.add_argument(
        '--log-level',
        default='warning',
        choices=['debug', 'debug2', 'info', 'warning', 'error', 'critical'],
    )
    root_parser.add_argument(
        '-v', '--version',
        action='version',
        version='backuppy' + __version__
    )
    root_parser.add_argument(
        '--disable-compression',
        action='store_true',
        help='Turn off GZIP\'ed backup blobs',
    )
    root_parser.add_argument(
        '--disable-encryption',
        action='store_true',
        help='Turn off encrypted backup blobs',
    )

    subparser = root_parser.add_subparsers(help='accepted commands')
    subparser.dest = 'subcommand'

    from backuppy.cli.backup import add_backup_parser
    add_backup_parser(subparser)

    args = root_parser.parse_args()

    if args.subcommand is None:
        logger.error('missing subcommand')
        root_parser.print_help()
        sys.exit(1)

    if not hasattr(args, 'entrypoint'):
        logger.critical(f'error: missing entrypoint for {args.subcommand}')
        sys.exit(1)

    return args
