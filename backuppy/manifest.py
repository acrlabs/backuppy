import io
import os
import time
from hashlib import sha256
from typing import Dict
from typing import IO
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple

import colorlog
import yaml

from backuppy.io import IOIter
from backuppy.util import EqualityMixin

logger = colorlog.getLogger(__name__)


class ManifestEntry(yaml.YAMLObject, EqualityMixin):
    yaml_tag = u'!entry'

    def __init__(self, abs_file_name: str, sha: str = '') -> None:
        file_stat = os.stat(abs_file_name)
        self.sha = sha
        if not self.sha:
            sha_fn = sha256()
            with open(abs_file_name, 'rb') as f:
                for data in IOIter(f, side_effects=[sha_fn.update]):
                    pass  # don't care about the data here
                self.sha = sha_fn.hexdigest()
        self.mtime = int(file_stat.st_mtime)
        self.uid = file_stat.st_uid
        self.gid = file_stat.st_gid
        self.mode = file_stat.st_mode

    def __repr__(self):  # pragma: no cover
        return f'<{self.sha}, {self.mtime}, {self.uid}, {self.gid}, {self.mode}>'


DiffPair = Tuple[Optional[ManifestEntry], Optional[ManifestEntry]]
ManifestContents = Dict[str, List[Tuple[float, Optional[ManifestEntry], bool]]]


class Manifest:
    """ A manifest listing all of the files tracked in the backup

    The manifest stores all of the information in a "contents" dictionary, which
    has the following format:

        /full/path/to/file1
            - (timestamp1, ManifestEntry, False)
            - (timestamp3, ManifestEntry, True)
        /full/path/to/file2:
            - (timestamp2, ManifestEntry, False)
            ...
        ...

    The list entries for each file are tuples indicating the time that the file
    was backed up, the relevant file metadata, and whether the entry should be intepreted
    as a diff or as the actual file contents.  This list is maintained in sorted order,
    so the last entry in the list shows the most recent version of the file.  Thus, it is
    possible to get a snapshot of the recorded history at any time T by iterating through
    the manifest and taking the most recent entry for each file with time less than T.

    ::note: empty directories are ignored by the manifest
    """

    def __init__(self):
        """ Create an empty manifest; we only do this if we're starting a new backup """
        self.contents: ManifestContents = {}

    def stream(self) -> IO[bytes]:
        """ Return a byte-stream containing the contents of the manifest for writing """
        return io.BytesIO(yaml.dump(self).encode())

    def get_diff_pair(self, abs_file_name: str, timestamp: Optional[float] = None) -> DiffPair:
        """ Return a (base file, diff) pair which can be used to reconstruct the specified file

        :param abs_file_name: the name of the file to reconstruct
        :param timestamp: the point in time for which we want to reconstruct the file (TODO)
        :returns: a DiffPair object
        """

        # if the file doesn't exist in the manifest, return an empty DiffPair
        try:
            history = self.contents[abs_file_name]
        except KeyError:
            return None, None

        # we need to find the index of the base file; this is the most recent file in the
        # Manifest which is not a diff
        base_index = max(
            i for
            i, (commit_time, entry, is_diff) in enumerate(history)
            if not is_diff  # deleted files are not diffs, so this will return None in that case
        )

        base = history[base_index][1]
        latest = history[-1][1]

        return base, latest

    def is_current(self, abs_file_name: str) -> bool:
        """ Check to see if the specified file name is up-to-date in the manifest """
        _, latest_entry = self.get_diff_pair(abs_file_name)
        return (latest_entry == ManifestEntry(abs_file_name))

    def insert_or_update(self, abs_file_name: str, entry: ManifestEntry, is_diff: bool) -> None:
        """ Insert a new entry into the manifest

        :param abs_file_name: the name of the file
        :param entry: the saved file metadata (we have to pass this in instead of re-creating
            it because the contents of the file may have changed since backing up)
        :param is_diff: indicates whether the backed-up file is a diff or a complete copy of the original file
        """
        commit_time = int(time.time())
        self.contents.setdefault(abs_file_name, []).append((commit_time, entry, is_diff))

    def delete(self, abs_file_name: str) -> None:
        """ Mark that a file has been deleted

        Note that we don't actually remove the file from the manifest in case we want to restore it
        later, we just insert a record with an empty ManifestEntry field.

        :param abs_file_name: the name of the file
        """

        commit_time = int(time.time())
        try:
            self.contents[abs_file_name].append((commit_time, None, False))
        except KeyError:
            logger.warn(f'Tried to delete unknown file {abs_file_name}')

    def files(self) -> Set[str]:
        """ Return all of the (currently-existing) files in the manifest """
        return set(
            filename
            for filename, entries in self.contents.items()
            if entries[-1][1]  # don't return files that have been deleted
        )

    # def snapshot(self, timestamp: float) -> Dict[str, ManifestEntry]:
    #     manifest_snapshot = {}
    #     for abs_file_name, entries in self.contents.items():
    #         # if the earliest entry is greater than our timestamp, skip it
    #         if entries[0][0] > timestamp:
    #             continue
    #         commit_time, entry = max(((ct, e) for ct, e in entries if ct <= timestamp))

    #         # Only include the path if the file hasn't been deleted
    #         if entry:
    #             manifest_snapshot[abs_file_name] = entry
    #     return manifest_snapshot
