class BackuppyException(Exception):
    pass


class BackupFailedError(BackuppyException):
    pass


class DiffParseError(BackuppyException):
    pass
