import logging
from typing import List
from typing import Optional

import colorlog

from backuppy.args import parse_args
from backuppy.config import setup_config

logger = colorlog.getLogger(__name__)
DEBUG2 = logging.DEBUG - 5


def _log_fns_for_level(log_level):
    def _log_fn(self, message, *args, **kwargs):
        if self.isEnabledFor(log_level):
            self._log(log_level, message, args, **kwargs)

    def _root_log_fn(message, *args, **kwargs):
        logging.log(log_level, message, *args, **kwargs)
    return _log_fn, _root_log_fn


def setup_logging(log_level_str: str = 'info') -> None:
    global logger
    if not len(logger.handlers):
        logging.addLevelName(DEBUG2, 'DEBUG2')
        setattr(logging, 'DEBUG2', DEBUG2)
        log_fn, root_log_fn = _log_fns_for_level(DEBUG2)
        setattr(logging.getLoggerClass(), 'debug2', log_fn)
        setattr(logging, 'debug2', root_log_fn)

        handler = colorlog.StreamHandler()
        handler.setFormatter(colorlog.ColoredFormatter(
            '%(log_color)s%(asctime)s %(levelname)s %(name)s(%(lineno)d) -- %(message)s'))
        logger = colorlog.getLogger()
        logger.addHandler(handler)

        log_level = getattr(logging, log_level_str.upper())
        logging.getLogger().setLevel(log_level)


def main(arg_list: Optional[List[str]] = None) -> None:
    args = parse_args("BackupPY - an open-source backup tool", arg_list)
    setup_logging(args.log_level)
    setup_config(args.config)
    args.entrypoint(args)


if __name__ == '__main__':
    main()
