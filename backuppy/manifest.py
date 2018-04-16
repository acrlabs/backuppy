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

    def __init__(self, *abs_path):
        abs_file_name = os.path.join(*abs_path)
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

        /full/path/to/backup_location_1:
            relative/path/to/file1:
                - (timestamp1, ManifestEntry)
                - (timestamp3, ManifestEntry)
            file2:
                - (timestamp2, ManifestEntry)
                ...
            ...

        /different/path/to/backup_location_2:
            ...

    The absolute path for any file can be reconstructed by concatenating the
    top-level index with the file name.

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

    def get_last_entry(self, abs_base_path, rel_name):
        try:
            return self.contents[abs_base_path][rel_name][-1][1]
        except KeyError:
            return None

    def is_current(self, *abs_path):
        return (self.get_last_entry(*abs_path) == ManifestEntry(*abs_path))

    def insert_or_update(self, entry, abs_base_path, rel_name):
        commit_time = int(time.time())
        self.contents.setdefault(abs_base_path, {}).setdefault(rel_name, []).append([commit_time, entry])

    def delete(self, abs_base_path, rel_name):
        commit_time = int(time.time())
        try:
            self.contents[abs_base_path][rel_name].append([commit_time, None])
        except KeyError:
            logger.warn(f'Tried to delete unknown file {rel_name} in {abs_base_path}')

    def tracked_files(self, abs_base_path):
        return set(self.contents.get(abs_base_path, {}).keys())

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
