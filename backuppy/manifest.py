import os
import time
from hashlib import sha256

import yaml

from backuppy.util import EqualityMixin
from backuppy.util import file_contents_stream
from backuppy.util import get_color_logger

logger = get_color_logger(__name__)


class ManifestEntry(yaml.YAMLObject, EqualityMixin):
    yaml_tag = u'!entry'

    def __init__(self, abs_file_name, sha=None):
        file_stat = os.stat(abs_file_name)
        self.sha = sha
        if not self.sha:
            hash_function = sha256()
            for chunk in file_contents_stream(abs_file_name):
                hash_function.update(chunk)
            self.sha = hash_function.hexdigest()
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

    def stream(self):
        return [yaml.dump(self).encode()]

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
