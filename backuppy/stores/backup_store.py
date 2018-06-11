from abc import ABCMeta
from abc import abstractmethod
from tempfile import TemporaryFile

import staticconf
import yaml

from backuppy.manifest import Manifest
from backuppy.util import get_color_logger

MANIFEST_PATH = 'manifest'
logger = get_color_logger(__name__)


class BackupStore(metaclass=ABCMeta):
    def __init__(self, backup_name):
        self.backup_name = backup_name
        self.config = staticconf.NamespaceReaders(backup_name)
        self._manifest = None

    @abstractmethod
    def save(self, stored_path, tmpfile):
        pass

    @abstractmethod
    def load(self, name, fd_out):
        pass

    @property
    def manifest(self):
        if not self._manifest:
            try:
                with TemporaryFile() as fd_manifest:
                    self.load(MANIFEST_PATH, fd_manifest)
                    self._manifest = yaml.load(fd_manifest.read())
            except Exception as e:
                logger.warn(f'Manifest could not be loaded for {self.backup_name} ({str(e)}); starting new manifest')
                self._manifest = Manifest()
        return self._manifest
