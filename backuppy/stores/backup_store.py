from abc import ABCMeta
from abc import abstractmethod
from tempfile import TemporaryFile
from typing import cast
from typing import IO
from typing import Optional
from typing import Tuple
from typing import Union

import staticconf
import yaml

from backuppy.manifest import Manifest
import colorlog

MANIFEST_PATH = 'manifest'
logger = colorlog.getLogger(__name__)


class BackupStore(metaclass=ABCMeta):
    backup_name: str
    _manifest: Optional[Manifest]

    def __init__(self, backup_name: str) -> None:
        self.backup_name = backup_name
        self.config = staticconf.NamespaceReaders(backup_name)
        self._manifest = None

    @abstractmethod
    def save(self, stored_path: Union[str, Tuple[str, ...]], tmpfile: IO[bytes]) -> None:  # pragma: no cover
        pass

    @abstractmethod
    def load(self, name: Union[str, Tuple[str, ...]], fd_out: IO[bytes]) -> None:  # pragma: no cover
        pass

    @property
    def manifest(self) -> Manifest:
        if not self._manifest:
            try:
                with TemporaryFile() as fd_manifest:
                    self.load(MANIFEST_PATH, fd_manifest)
                    self._manifest = yaml.load(fd_manifest.read())
            except Exception as e:
                logger.warning(f'Manifest could not be loaded for {self.backup_name} ({str(e)}); starting new manifest')
                self._manifest = Manifest()

        return cast(Manifest, self._manifest)  # OK to cast here because we'll always have a Manifest at this point
