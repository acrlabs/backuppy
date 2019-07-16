import os
import re
from typing import Generator
from typing import List
from typing import Pattern

import dateparser

from backuppy.exceptions import InputParseError


def compile_exclusions(exclusions: str) -> List[Pattern]:  # pragma: no cover
    return [re.compile(excl) for excl in exclusions]


def file_walker(path, on_error=None) -> Generator[str, None, None]:  # pragma: no cover
    """ Walk through all the files in a path and yield their names one-at-a-time,
    relative to the "path" value passed in.
    """
    for root, dirs, files in os.walk(path, onerror=on_error):
        for f in files:
            yield os.path.join(root, f)


def parse_time(input_str: str) -> int:
    dt = dateparser.parse(input_str)
    if not dt:
        raise InputParseError(f'Could not parse time "{input_str}"')
    return int(dt.timestamp)


def path_join(*args):
    return os.path.normpath('/'.join(args))


def sha_to_path(sha: str) -> str:
    return os.path.join(sha[:2], sha[2:4], sha[4:])
