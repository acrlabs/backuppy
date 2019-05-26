import os
import re
from typing import List
from typing import Pattern


class EqualityMixin:  # pragma: no cover
    def __eq__(self, other):
        return other and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


def compile_exclusions(exclusions: str) -> List[Pattern]:  # pragma: no cover
    return [re.compile(excl) for excl in exclusions]


def file_walker(path, on_error=None):  # pragma: no cover
    """ Walk through all the files in a path and yield their names one-at-a-time,
    relative to the "path" value passed in.
    """
    for root, dirs, files in os.walk(path, onerror=on_error):
        for f in files:
            yield os.path.join(root, f)


def path_join(*args):
    return os.path.normpath('/'.join(args))


def sha_to_path(sha: str) -> str:
    return os.path.join(sha[:2], sha[2:4], sha[4:])
