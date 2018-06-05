import os
import re

import colorlog

CHUNK_SIZE = 2


class EqualityMixin:
    def __eq__(self, other):
        return other and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


def compile_exclusions(exclusions):
    return [re.compile(excl) for excl in exclusions]


def file_contents_stream(abs_file_name):
    with open(abs_file_name, 'rb') as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            yield chunk


def file_walker(path, on_error=None):
    """ Walk through all the files in a path and yield their names one-at-a-time,
    relative to the "path" value passed in.
    """
    for root, dirs, files in os.walk(path, onerror=on_error):
        for f in files:
            yield os.path.join(root, f)


def get_color_logger(name):
    handler = colorlog.StreamHandler()
    handler.setFormatter(colorlog.ColoredFormatter('%(log_color)s%(levelname)s:%(name)s:%(message)s'))
    logger = colorlog.getLogger(name)
    logger.addHandler(handler)
    return logger


def sha_to_path(sha):
    return (sha[:2], sha[2:4], sha[4:])
