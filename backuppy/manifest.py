import os
from collections import defaultdict
from collections import namedtuple

import yaml

from backuppy.crypto import compress_and_encrypt
from backuppy.crypto import compute_hash
from backuppy.crypto import decrypt_and_unpack

ManifestEntry = namedtuple('ManifestEntry', ['sha', 'mtime'])


class Manifest:

    def __init__(self, path=None):
        if path:
            self._data = {
                'metadata': {
                    'path': os.path.abspath(path),
                },
                'contents': defaultdict(list),
            }

    def save(self, location):
        with open(location, 'wb') as f:
            f.write(compress_and_encrypt(yaml.dump(self._data)))

    @classmethod
    def load(cls, location):
        manifest = cls()
        with open(location, 'rb') as f:
            manifest._data = yaml.load(decrypt_and_unpack(f.read()))
        return manifest

    def update(self):
        for root, dirs, files in os.walk(self.path):
            for f in files:
                abs_file_name = os.path.abspath(os.path.join(root, f))
                sha = compute_hash(abs_file_name)

                if abs_file_name not in self.contents or sha != self.contents[abs_file_name][-1].sha:
                    self.contents[abs_file_name].append(ManifestEntry(
                        sha, int(os.stat(abs_file_name).st_mtime),
                    ))

    @property
    def path(self):
        return self._data['metadata']['path']

    @property
    def contents(self):
        return self._data['contents']
