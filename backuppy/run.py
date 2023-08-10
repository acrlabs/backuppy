import logging
import logging.handlers
from typing import List
from typing import Optional

import colorlog

from backuppy.args import parse_args
from backuppy.config import setup_config

logger = colorlog.getLogger(__name__)
DEBUG2 = logging.DEBUG - 5
ROTATING_LOG_FILE_COUNT = 10
ROTATING_LOG_FILE_MAX_BYTES = 2 ** 27  # approx 100 MB


def _log_fns_for_level(log_level):
    def _log_fn(self, message, *args, **kwargs):
        if self.isEnabledFor(log_level):
            self._log(log_level, message, args, **kwargs)

    def _root_log_fn(message, *args, **kwargs):
        logging.log(log_level, message, *args, **kwargs)
    return _log_fn, _root_log_fn


def setup_logging(
    log_level_str: str = 'info',
    log_file: Optional[str] = None,
    log_file_level_str: Optional[str] = None,
) -> None:
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
        log_level = getattr(logging, log_level_str.upper())
        handler.setLevel(log_level)
        colorlog.getLogger().addHandler(handler)

        min_log_level = log_level

        if log_file:
            log_file_handler = logging.handlers.RotatingFileHandler(
                log_file,
                maxBytes=ROTATING_LOG_FILE_MAX_BYTES,
                backupCount=ROTATING_LOG_FILE_COUNT,
            )
            log_file_level_str = log_file_level_str or log_level_str
            log_file_level = getattr(logging, log_file_level_str.upper())
            log_file_handler.setLevel(log_file_level)
            log_file_handler.setFormatter(logging.Formatter(
                '%(asctime)s %(levelname)s %(name)s(%(lineno)d) -- %(message)s'))

            if log_file_level < min_log_level:
                min_log_level = log_file_level
            logging.getLogger().addHandler(log_file_handler)

        logging.getLogger().setLevel(min_log_level)

        # these logs are super noisy, turn them off
        logging.getLogger('botocore').setLevel(max(logging.INFO, log_level))
        logging.getLogger('boto3').setLevel(max(logging.INFO, log_level))


def main(arg_list: Optional[List[str]] = None) -> None:
    args = parse_args("BackupPY - an open-source backup tool", arg_list)
    setup_logging(args.log_level, args.log_file, args.log_file_level)
    setup_config(args.config)
    args.entrypoint(args)


if __name__ == '__main__':
    main()
