class BackuppyException(Exception):
    pass


class BackupCorruptedError(BackuppyException):
    pass


class BackupReadFailedException(BackuppyException):
    pass


class DiffParseError(BackuppyException):
    pass


class DoubleBufferError(BackuppyException):
    pass


class FileChangedException(BackuppyException):
    pass


class InputParseError(BackuppyException):
    pass


class ManifestLockedException(BackuppyException):
    pass


class UnknownProtocolError(BackuppyException):
    pass
