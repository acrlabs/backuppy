import os
import re
import time
from collections import defaultdict

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

    def __init__(self, paths):
        """ Create an empty manifest; we only do this if we're starting a new backup """
        self.contents = {os.path.abspath(path): defaultdict(list) for path in paths}

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

    def update(self, exclusions=None):
        """ Iterate through all the locations tracked by the Manifest and update with
        any new, changed, or deleted files

        :param exclusions: a dictionary of lists of regex strings matching files to ignore;
            the exclusions dict is indexed by "root backup path" to allow for different exclusion
            patterns in different locations (this isn't strictly necessary, but is provided as
            a convenience)
        """
        commit_time = int(time.time())
        exclusions = {
            path: [re.compile(x) for x in excl_list]
            for path, excl_list in exclusions.items()
        } if exclusions else defaultdict(list)

        for path, records in self.contents.items():
            unmarked_files = set(records.keys())
            for rel_name in file_walker(path, logger.warn):
                abs_file_name = os.path.join(path, rel_name)

                # Skip files that match any of the specified regular expressions
                if any([excl.search(rel_name) for excl in exclusions[path]]):
                    continue

                try:
                    entry = ManifestEntry(abs_file_name)
                except OSError as err:
                    logger.warn(f'Could not read {abs_file_name} -- skipping: {err}')
                    continue

                # Mark the file as "seen" so it isn't deleted later
                unmarked_files.discard(rel_name)

                # Update the entry if it's not present or the metadata is different
                try:
                    last_entry = records[rel_name][-1][1]
                except IndexError:
                    last_entry = None

                if entry != last_entry:
                    records[rel_name].append([commit_time, entry])

            # Mark all files that weren't touched in the above loop as "deleted"
            # (we don't actually delete the file, just record that it's no longer present)
            for deleted_file in unmarked_files:
                records[deleted_file].append([commit_time, None])

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
