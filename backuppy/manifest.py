import os
import re
import time

import yaml

from backuppy.crypto import compress_and_encrypt
from backuppy.crypto import compute_hash
from backuppy.crypto import decrypt_and_unpack
from backuppy.util import EqualityMixin
from backuppy.util import file_walker
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


class Manifest:

    def __init__(self, paths):
        self.contents = {os.path.abspath(path): dict() for path in paths}

    def save(self, location):
        with open(location, 'wb') as f:
            f.write(compress_and_encrypt(yaml.dump(self)))

    @staticmethod
    def load(location):
        with open(location, 'rb') as f:
            return yaml.load(decrypt_and_unpack(f.read()))

    def update(self, exclusions=None):
        commit_time = time.time()
        exclusions = exclusions or {}

        for path, records in self.contents.items():
            unmarked_files = set(records.keys())
            for abs_file_name in file_walker(path, logger.warn):

                # Skip files that match any of the specified regular expressions
                if any([re.search(pattern, abs_file_name) for pattern in exclusions.get(path, [])]):
                    continue

                try:
                    entry = ManifestEntry(abs_file_name)
                except OSError as err:
                    logger.warn(f'Could not read {abs_file_name} -- skipping: {err}')
                    continue

                # Mark the file as "seen" so it isn't deleted later
                unmarked_files.discard(abs_file_name)

                # Update the entry if it's not present or the metadata is different
                try:
                    last_entry = records[abs_file_name][-1][1]
                except KeyError:
                    last_entry = None

                if entry != last_entry:
                    records.setdefault(abs_file_name, []).append((commit_time, entry))

            # Mark all files that weren't touched in the above loop as "deleted"
            # (we don't actually delete the file, just record that it's no longer present)
            for deleted_file in unmarked_files:
                records[deleted_file].append((commit_time, None))

    def snapshot(self, timestamp):
        manifest_snapshot = {}
        for path, records in self.contents.items():
            for abs_file_name, entries in records.items():
                try:
                    commit_time, entry = max(((ct, e) for ct, e in entries if ct <= timestamp))
                except ValueError:
                    logger.info(f'{abs_file_name} has not been created yet')
                    continue

                # Only include the path if the file hasn't been deleted
                if entry:
                    manifest_snapshot[abs_file_name] = entry
        return manifest_snapshot
