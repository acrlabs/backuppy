class BackuppyException(Exception):
    pass


class BackupReadFailedException(Exception):
    pass


class DiffParseError(BackuppyException):
    pass


class UnknownProtocolError(BackuppyException):
    pass
