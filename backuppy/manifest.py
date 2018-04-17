import os
import time

import yaml

from backuppy.crypto import compress_and_encrypt
from backuppy.crypto import compute_hash
from backuppy.crypto import decrypt_and_unpack
from backuppy.util import EqualityMixin
from backuppy.util import get_color_logger

logger = get_color_logger(__name__)


class ManifestEntry(yaml.YAMLObject, EqualityMixin):
    yaml_tag = u'!entry'

    def __init__(self, abs_file_name):
        file_stat = os.stat(abs_file_name)
        self.sha = compute_hash(abs_file_name)
        self.mtime = int(file_stat.st_mtime)
        self.uid = file_stat.st_uid
        self.gid = file_stat.st_gid
        self.mode = file_stat.st_mode

    def __repr__(self):
        return f'<{self.sha}, {self.mtime}, {self.uid}, {self.gid}, {self.mode}>'


class Manifest:
    """ A manifest listing all of the files tracked in the backup

    The manifest stores all of the information in a "contents" dictionary, which
    has the following format:

        /full/path/to/file1
            - (timestamp1, ManifestEntry)
            - (timestamp3, ManifestEntry)
        /full/path/to/file2:
            - (timestamp2, ManifestEntry)
            ...
        ...

    The list entries for each file are tuples indicating the time that the file
    was backed up as well as the relevant file metadata.  This list is maintained
    in sorted order, so the last entry in the list shows the most recent version of
    the file.  Thus, it is possible to get a snapshot of the recorded history at any
    time T by iterating through the manifest and taking the most recent entry for each
    file with time less than T.

    ::note: empty directories are ignored by the manifest
    """

    def __init__(self):
        """ Create an empty manifest; we only do this if we're starting a new backup """
        self.contents = {}

    def save(self, location):
        """ Write the manifest to an encrypted YAML file """
        with open(location, 'wb') as f:
            f.write(compress_and_encrypt(yaml.dump(self)))

    @staticmethod
    def load(location):
        """ Read the manifest from an encrypted YAML file; PyYAML records the Python class that
        we're writing here, so when we call "load" it will return a Manifest object.
        """
        with open(location, 'rb') as f:
            return yaml.load(decrypt_and_unpack(f.read()))

    def get_last_entry(self, abs_file_name):
        try:
            return self.contents[abs_file_name][-1][1]
        except KeyError:
            return None

    def is_current(self, abs_file_name):
        return (self.get_last_entry(abs_file_name) == ManifestEntry(abs_file_name))

    def insert_or_update(self, abs_file_name, entry):
        commit_time = int(time.time())
        self.contents.setdefault(abs_file_name, []).append([commit_time, entry])

    def delete(self, abs_file_name):
        commit_time = int(time.time())
        try:
            self.contents[abs_file_name].append([commit_time, None])
        except KeyError:
            logger.warn(f'Tried to delete unknown file {abs_file_name}')

    def tracked_files(self):
        return set(self.contents.keys())

    def snapshot(self, timestamp):
        manifest_snapshot = {}
        for abs_file_name, entries in self.contents.items():
            # if the earliest entry is greater than our timestamp, skip it
            if entries[0][0] > timestamp:
                continue
            commit_time, entry = max(((ct, e) for ct, e in entries if ct <= timestamp))

            # Only include the path if the file hasn't been deleted
            if entry:
                manifest_snapshot[abs_file_name] = entry
        return manifest_snapshot
