import sqlite3
import time
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple

import colorlog

logger = colorlog.getLogger(__name__)
ManifestEntryHistory = List[Tuple['ManifestEntry', int]]
QueryResponse = Tuple[str, ManifestEntryHistory]


class ManifestEntry:
    def __init__(
        self,
        abs_file_name: str,
        sha: str,
        base_sha: Optional[str],
        uid: int,
        gid: int,
        mode: int,
    ) -> None:
        self.abs_file_name = abs_file_name
        self.sha = sha
        self.base_sha = base_sha
        self.uid = uid
        self.gid = gid
        self.mode = mode

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> 'ManifestEntry':
        """ Helper function to construct a ManifestEntry from the database """
        return cls(
            row['abs_file_name'],
            row['sha'],
            row['base_sha'],
            row['uid'],
            row['gid'],
            row['mode'],
        )


DiffPair = Tuple[Optional[ManifestEntry], Optional[ManifestEntry]]


class Manifest:
    """ A sqlite3 manifest listing all of the files tracked in the backup

    ::note: empty directories are ignored by the manifest
    """

    def __init__(self, manifest_filename: str):
        """ Connect to a manifest file and optionally initialize a new database """
        self._conn = sqlite3.connect(manifest_filename)
        self._conn.set_trace_callback(logger.debug2)
        self._conn.row_factory = sqlite3.Row
        self._cursor = self._conn.cursor()
        self.changed = False

        self._cursor.execute(
            '''
            select name from sqlite_master
            where type ='table' and name not like 'sqlite_%'
            '''
        )
        rows = self._cursor.fetchall()
        if {r['name'] for r in rows} != {'manifest', 'diff_pairs'}:
            logger.info('This looks like a new manifest; initializing')
            self._create_manifest_tables()

    def get_entry(
        self,
        abs_file_name: str,
        timestamp: Optional[int] = None,
    ) -> Optional[ManifestEntry]:
        """ Get the contents of the manifest for the most recent version of a file

        :param abs_file_name: the name of the file to reconstruct
        :param timestamp: the point in time for which we want to reconstruct the file
        :returns: the contents of the database corresponding to the requested filename at the
            specified time
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

    def search(
        self,
        like: str = '',
        before_timestamp: Optional[int] = None,
        after_timestamp: Optional[int] = None,
        file_limit: Optional[int] = None,
        history_limit: Optional[int] = None,
    ) -> List[QueryResponse]:

        if file_limit == 0 or history_limit == 0:
            return []

        like_query = f"%{like or ''}%"
        before_timestamp = before_timestamp or int(time.time())
        after_timestamp = after_timestamp or 0
        self._cursor.execute(
            '''
            select * from manifest natural left join diff_pairs
            where abs_file_name like ? and commit_timestamp between ? and ?
            order by abs_file_name, commit_timestamp desc
            ''',
            (like_query, after_timestamp, before_timestamp)
        )

        results: List[QueryResponse] = []
        rows, i, file_count = self._cursor.fetchall(), 0, 0
        while i < len(rows):
            abs_file_name, history, j = rows[i]['abs_file_name'], [], 0
            while i + j < len(rows) and rows[i + j]['abs_file_name'] == abs_file_name:
                if not history_limit or j < history_limit:
                    history.append((
                        ManifestEntry.from_row(rows[i + j]),
                        rows[i + j]['commit_timestamp'],
                    ))
                j += 1

            results.append((abs_file_name, history))
            file_count += 1
            i += j

        return results

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
            (abs_file_name, sha, uid, gid, mode, commit_timestamp)
            values (?, ?, ?, ?, ?, ?)
            ''',
            (
                entry.abs_file_name,
                entry.sha,
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
        self._commit()

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
        self._commit()

    def files(self, timestamp: Optional[int] = None) -> Set[str]:
        """ Return all of the (currently-existing) files in the manifest at or before the
        specified time

        :param timestamp: the most recent commit timestamp to consider in the manifest
        :returns: all of the absolute filenames contained in the manifest matching the criteria
        """
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

    def _commit(self):
        """ Commit the data to the database, and mark that the database has changed """
        self._conn.commit()
        self.changed = True

    def _create_manifest_tables(self):
        """ Initialize a new manifest """
        self._cursor.execute(
            '''
            create table manifest (
                abs_file_name text not null,
                sha text,
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
        self._cursor.execute(
            'create index manifest_idx on manifest(abs_file_name, commit_timestamp)')
        self._commit()
