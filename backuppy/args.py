import argparse
import sys
from typing import Callable
from typing import List
from typing import Optional

import colorlog

from backuppy import __version__

logger = colorlog.getLogger(__name__)


class CustomHelpFormatter(argparse.ArgumentDefaultsHelpFormatter):  # pragma: no cover
    def __init__(self, prog):
        super().__init__(prog, max_help_position=35, width=100)


def subparser(command: str, description: str, entrypoint: Callable) -> Callable:  # pragma: no cover
    """ Function decorator to simplify adding arguments to subcommands

    :param command: name of the subcommand to add
    :param help: help string for the subcommand
    :param entrypoint: the 'main' function for the subcommand to execute
    """
    def decorator(add_args):
        def wrapper(subparser):
            subparser = subparser.add_parser(
                command,
                formatter_class=CustomHelpFormatter,
                description=description,
                add_help=False,
            )
            add_args(subparser)
            subparser.add_argument('-h', '--help', action='help', help='show this message and exit')
            subparser.set_defaults(entrypoint=entrypoint)
        return wrapper
    return decorator


def add_name_arg(parser: argparse.ArgumentParser) -> None:  # pragma: no cover
    parser.add_argument(
        '--name',
        required=True,
        help='Name of the backup set to examine'
    )


def add_preserve_scratch_arg(parser: argparse.ArgumentParser) -> None:  # pragma: no cover
    parser.add_argument(
        '--preserve-scratch-dir',
        action='store_true',
        help=(
            'Keep the configured "scratch directory" around after job completes '
            '(WARNING: may take up a lot of disk space)'
        )
    )


def parse_args(
    description: str,
    arg_list: Optional[List[str]],
) -> argparse.Namespace:  # pragma: no cover
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
        '--log-file',
        default=None,
        help='Create a rotating file log in addition to stdout'
    )
    root_parser.add_argument(
        '--log-file-level',
        default='info',
        choices=['debug', 'debug2', 'info', 'warning', 'error', 'critical'],
        help='Log level for the rotated log file; has no effect if no log file is specified'
    )
    root_parser.add_argument(
        '-v', '--version',
        action='version',
        version='backuppy' + __version__
    )
    root_parser.add_argument(
        '--config',
        default='backuppy.conf',
        metavar='filename',
        help='Config file to load specifying what to back up',
    )

    subparser = root_parser.add_subparsers(help='accepted commands')
    subparser.dest = 'subcommand'

    from backuppy.cli.backup import add_backup_parser
    from backuppy.cli.list import add_list_parser
    from backuppy.cli.restore import add_restore_parser
    from backuppy.cli.verify import add_verify_parser
    from backuppy.cli.get import add_get_parser
    from backuppy.cli.put import add_put_parser
    add_backup_parser(subparser)
    add_list_parser(subparser)
    add_restore_parser(subparser)
    add_verify_parser(subparser)
    add_get_parser(subparser)
    add_put_parser(subparser)

    args = root_parser.parse_args(args=(arg_list or sys.argv[1:]))

    if args.subcommand is None:
        logger.error('missing subcommand')
        root_parser.print_help()
        sys.exit(1)

    if not hasattr(args, 'entrypoint'):
        logger.critical(f'error: missing entrypoint for {args.subcommand}')
        sys.exit(1)

    return args
