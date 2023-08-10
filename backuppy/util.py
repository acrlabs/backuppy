import os
import re
import sys
from datetime import datetime
from random import shuffle
from tempfile import gettempdir
from typing import Callable
from typing import Generator
from typing import List
from typing import Optional
from typing import Pattern

import colorlog
import dateparser
import staticconf
from iteration_utilities import deepflatten

from backuppy.exceptions import InputParseError

logger = colorlog.getLogger(__name__)


def ask_for_confirmation(prompt: str, default: str = 'y'):
    yes = 'Y' if default.lower() == 'y' else 'y'
    no = 'n' if default.lower() == 'y' else 'N'

    while True:
        sys.stdout.write(f'{prompt} [{yes}/{no}] ')
        sys.stdout.flush()
        if staticconf.read_bool('yes', default=False):  # type: ignore[attr-defined]
            return True

        inp = sys.stdin.readline().strip()
        if inp.lower() in {'y', 'yes'}:
            return True
        elif inp.lower() in {'n', 'no'}:
            return False
        elif inp == '':
            return default == 'y'
        else:
            print('Unrecognized response; please enter "yes" or "no"')


def compile_exclusions(exclusions: List[str]) -> List[Pattern]:
    # deep-flattening the exclusions list makes it nicer to use YAML anchors
    return [re.compile(excl) for excl in deepflatten(exclusions, ignore=str)]


def file_walker(
    path,
    on_error: Optional[Callable] = None,
    exclusions: Optional[List[Pattern]] = None,
) -> Generator[str, None, None]:
    """ Walk through all the files in a path and yield their names one-at-a-time,
    relative to the "path" value passed in.  The ordering of the returned files
    is randomized so we don't always back up the same files in the same order.

    :param path: root path to start walking
    :param on_error: function to call if something goes wrong in os.walk
    :param exclusions: list of regexes to skip; if you want the regex to _only_
        match directories, you must end the pattern with os.sep.  If you want it
        to _only_ match files, it must end with $.  Otherwise, the pattern will
        match both directories and files
    :returns: a generator of all of the files in the path and its subdirectories
        that don't match anything in exclusions
    """
    exclusions = exclusions or []
    for root, dirs, files in os.walk(path, onerror=on_error):

        # Skip files and directories that match any of the specified regular expressions
        new_dirs = []
        for d in dirs:
            abs_dir_name = path_join(root, d) + os.sep
            matched_patterns = [excl.pattern for excl in exclusions if excl.search(abs_dir_name)]
            if matched_patterns:
                logger.info(f'{abs_dir_name} matched exclusion(s) "{matched_patterns}"; skipping')
            else:
                new_dirs.append(d)  # don't need the abs name here

        # os.walk allows you to modify the dirs in-place to control the order in which
        # things are visited; we do that here to ensure that we're not always starting
        # our backup in the same place and going through in the same order, which could
        # result in the later things never getting backed up if there is some systemic crash
        shuffle(new_dirs)
        dirs[:] = new_dirs
        shuffle(files)
        for f in files:
            abs_file_name = path_join(root, f)
            matched_patterns = [excl.pattern for excl in exclusions if excl.search(abs_file_name)]
            if matched_patterns:
                logger.info(f'{abs_file_name} matched exclusion(s) "{matched_patterns}"; skipping')
            else:
                yield path_join(root, f)


def format_sha(sha: str, sha_length: int) -> Optional[str]:
    return sha[:sha_length] + '...' if sha else None


def format_time(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')


def get_scratch_dir() -> str:
    return os.path.join(gettempdir(), 'backuppy')


def regex_search_list(needle: str, haystack: List[str]):
    for pattern in haystack:
        if re.search(pattern, needle):
            return True
    return False


def parse_time(input_str: str) -> int:
    dt = dateparser.parse(input_str)
    if not dt:
        raise InputParseError(f'Could not parse time "{input_str}"')
    return int(dt.timestamp())


def path_join(*args):
    return os.path.normpath(os.sep.join(args))


def sha_to_path(sha: str) -> str:
    return os.path.join(sha[:2], sha[2:4], sha[4:])
