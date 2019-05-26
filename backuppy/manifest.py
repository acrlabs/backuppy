import os
import sqlite3
import time
from typing import Optional
from typing import Set
from typing import Tuple

import colorlog

from backuppy.util import EqualityMixin

logger = colorlog.getLogger(__name__)


class ManifestEntry(EqualityMixin):
    def __init__(
        self,
        abs_file_name: str,
        sha: str,
        base_sha: Optional[str],
        mtime: int,
        uid: int,
        gid: int,
        mode: int,
    ) -> None:
        self.abs_file_name = abs_file_name
        self.sha = sha
        self.base_sha = base_sha
        self.mtime = mtime
        self.uid = uid
        self.gid = gid
        self.mode = mode

    @classmethod
    def from_stat(
        cls,
        abs_file_name: str,
        sha: str,
        base_sha: Optional[str],
        file_stat: os.stat_result,
    ) -> 'ManifestEntry':
        return cls(
            abs_file_name,
            sha,
            base_sha,
            int(file_stat.st_mtime),
            file_stat.st_uid,
            file_stat.st_gid,
            file_stat.st_mode,
        )

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> 'ManifestEntry':
        return cls(row['abs_file_name'], row['sha'], row['base_sha'], row['mtime'], row['uid'], row['gid'], row['mode'])


DiffPair = Tuple[Optional[ManifestEntry], Optional[ManifestEntry]]


class Manifest:
    """ A sqlite3 manifest listing all of the files tracked in the backup

    ::note: empty directories are ignored by the manifest
    """

    def __init__(self, manifest_filename: str, start_new_manifest: bool):
        """ Connect to a manifest file and optionally initialize a new database """
        self._conn = sqlite3.connect(manifest_filename)
        self._conn.set_trace_callback(logger.debug2)
        self._conn.row_factory = sqlite3.Row
        self._cursor = self._conn.cursor()
        if start_new_manifest:
            self._cursor.execute(
                '''
                create table manifest (
                    abs_file_name text not null,
                    sha text,
                    mtime integer,
                    uid integer,
                    gid integer,
                    mode integer,
                    commit_timestamp integer not null
                )
                '''
            )
            self._cursor.execute(
                '''
                create table diff_pairs (
                    sha text not null unique,
                    base_sha text not null,
                    foreign key(sha) references manifest(sha)
                )
                '''
            )
            self._cursor.execute('create index manifest_idx on manifest(abs_file_name, commit_timestamp)')

    def get_entry(self, abs_file_name: str, timestamp: Optional[int] = None) -> Optional[ManifestEntry]:
        """ Return a (base file, diff) pair which can be used to reconstruct the specified file

        :param abs_file_name: the name of the file to reconstruct
        :param timestamp: the point in time for which we want to reconstruct the file
        :returns: a DiffPair object
        """

        timestamp = timestamp or int(time.time())
        self._cursor.execute(
            '''
            select * from manifest natural left join diff_pairs
            where abs_file_name=? and commit_timestamp<=? order by commit_timestamp
            ''',
            (abs_file_name, timestamp),
        )
        rows = self._cursor.fetchall()
        if not rows:
            return None

        latest_row = rows[-1]
        return ManifestEntry.from_row(latest_row)

    def insert_or_update(self, entry: ManifestEntry) -> None:
        """ Insert a new entry into the manifest

        :param abs_file_name: the name of the file
        :param entry: the saved file metadata (we have to pass this in instead of re-creating
            it because the contents of the file may have changed since backing up)
        """
        commit_timestamp = int(time.time())
        self._cursor.execute(
            '''
            insert into manifest
            (abs_file_name, sha, mtime, uid, gid, mode, commit_timestamp)
            values (?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                entry.abs_file_name,
                entry.sha,
                entry.mtime,
                entry.uid,
                entry.gid,
                entry.mode,
                commit_timestamp,
            ),
        )
        if entry.base_sha:
            self._cursor.execute(
                'insert or ignore into diff_pairs (sha, base_sha) values (?, ?)',
                (entry.sha, entry.base_sha),
            )
        self._conn.commit()

    def delete(self, abs_file_name: str) -> None:
        """ Mark that a file has been deleted

        Note that we don't actually remove the file from the manifest in case we want to restore it
        later, we just insert a record with an empty ManifestEntry field.

        :param abs_file_name: the name of the file
        """

        if not self.get_entry(abs_file_name):
            logger.warn('Trying to delete untracked file; nothing written to datastore')
            return

        commit_timestamp = int(time.time())
        self._cursor.execute(
            'insert into manifest (abs_file_name, commit_timestamp) values (?, ?)',
            (abs_file_name, commit_timestamp),
        )
        self._conn.commit()

    def files(self, timestamp: Optional[int] = None) -> Set[str]:
        """ Return all of the (currently-existing) files in the manifest """
        timestamp = timestamp or int(time.time())
        self._cursor.execute(
            '''
            select abs_file_name, max(commit_timestamp) from manifest
            where commit_timestamp <=?
            group by abs_file_name having sha not null
            ''',
            (timestamp,),
        )
        return set(row['abs_file_name'] for row in self._cursor.fetchall())
