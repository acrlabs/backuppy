from abc import ABCMeta
from abc import abstractmethod

import staticconf


class BackupStore(metaclass=ABCMeta):
    def __init__(self, backup_name):
        self.config = staticconf.NamespaceReaders(backup_name)

    @abstractmethod
    def write(self, stored_path, tmpfile):
        pass

    @abstractmethod
    def read(self, names):
        pass
