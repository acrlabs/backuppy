import os

import yaml

from backuppy.crypto import compress_and_encrypt
from backuppy.crypto import compute_hash
from backuppy.crypto import decrypt_and_unpack
from backuppy.util import EqualityMixin
from backuppy.util import get_color_logger

logger = get_color_logger(__name__)


class ManifestEntry(yaml.YAMLObject, EqualityMixin):
    yaml_tag = u'!entry'

    def __init__(self, filename):
        file_stat = os.stat(filename)
        self.sha = compute_hash(filename)
        self.mtime = int(file_stat.st_mtime)
        self.uid = file_stat.st_uid
        self.gid = file_stat.st_gid
        self.mode = file_stat.st_mode

    def __repr__(self):
        return f'<{self.sha}, {self.mtime}, {self.uid}, {self.gid}, {self.mode}>'


class Manifest(EqualityMixin):

    def __init__(self, paths):
        self.contents = {os.path.abspath(path): dict() for path in paths}

    def save(self, location):
        with open(location, 'wb') as f:
            f.write(compress_and_encrypt(yaml.dump(self)))

    @staticmethod
    def load(location):
        with open(location, 'rb') as f:
            return yaml.load(decrypt_and_unpack(f.read()))

    def update(self):
        for path, entries in self.contents.items():
            for root, dirs, files in os.walk(path, onerror=logger.warn):
                for f in files:
                    abs_file_name = os.path.join(root, f)
                    try:
                        entry = ManifestEntry(abs_file_name)
                    except OSError as err:
                        logger.warn(f'Could not read {abs_file_name} -- skipping: {err}')
                        continue

                    if abs_file_name not in entries or entry != entries[abs_file_name][-1]:
                        entries.setdefault(abs_file_name, []).append(entry)
