import os

import colorlog


class EqualityMixin:
    def __eq__(self, other):
        return other and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


def file_walker(path, on_error=None):
    for root, dirs, files in os.walk(path, onerror=on_error):
        for f in files:
            yield os.path.join(root, f)


def get_color_logger(name):
    handler = colorlog.StreamHandler()
    handler.setFormatter(colorlog.ColoredFormatter('%(log_color)s%(levelname)s:%(name)s:%(message)s'))
    logger = colorlog.getLogger(name)
    logger.addHandler(handler)
    return logger
