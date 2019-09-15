import sqlite3
import time
from typing import Callable
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple

import colorlog

from backuppy.crypto import compress_and_encrypt
from backuppy.crypto import decrypt_and_unpack
from backuppy.crypto import decrypt_and_verify
from backuppy.crypto import encrypt_and_sign
from backuppy.crypto import generate_key_pair
from backuppy.exceptions import BackupCorruptedError
from backuppy.io import IOIter
from backuppy.options import OptionsDict
from backuppy.util import get_scratch_dir
from backuppy.util import path_join

logger = colorlog.getLogger(__name__)
MANIFEST_PREFIX = 'manifest.'
MANIFEST_KEY_PREFIX = 'manifest-key.'
MANIFEST_FILE = MANIFEST_PREFIX + '{ts}'
MANIFEST_KEY_FILE = MANIFEST_KEY_PREFIX + '{ts}'
_MANIFEST_TABLES = {'manifest', 'base_shas'}
QueryResponse = Tuple[str, List['ManifestEntry']]


class ManifestEntry:
    def __init__(
        self,
        abs_file_name: str,
        sha: str,
        base_sha: Optional[str],
        uid: int,
        gid: int,
        mode: int,
        key_pair: bytes,
        base_key_pair: Optional[bytes],
        commit_timestamp: int = 0,  # provide a dummy value to be filled in at commit time
    ) -> None:
        self.abs_file_name = abs_file_name
        self.sha = sha
        self.base_sha = base_sha
        self.uid = uid
        self.gid = gid
        self.mode = mode
        self.key_pair = key_pair
        self.base_key_pair = base_key_pair
        self.commit_timestamp = commit_timestamp

        # if a base sha is provided, a key_pair MUST also exist for that sha
        assert bool(base_sha) == bool(base_key_pair)

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
            row['key_pair'],
            row['base_key_pair'],
            row['commit_timestamp'],
        )


class Manifest:
    """ A sqlite3 manifest listing all of the files tracked in the backup

    ::note: empty directories are ignored by the manifest
    """

    def __init__(self, manifest_filename: str):
        """ Connect to a manifest file and optionally initialize a new database """
        self.filename = manifest_filename
        self._conn = sqlite3.connect(self.filename)
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
        tables = {r['name'] for r in rows}
        if tables and not (tables == _MANIFEST_TABLES):
            raise BackupCorruptedError(f'The manifest does not have the right tables: {tables}')
        elif not tables:
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
            select * from manifest natural left join base_shas
            where abs_file_name=? and commit_timestamp<=? order by commit_timestamp
            ''',
            (abs_file_name, timestamp),
        )
        rows = self._cursor.fetchall()
        if not rows:
            return None

        latest_row = rows[-1]
        return ManifestEntry.from_row(latest_row)

    def get_entries_by_sha(self, sha: str) -> List[ManifestEntry]:
        self._cursor.execute(
            'select * from manifest natural left join base_shas where sha like ?',
            (f'{sha}%',),
        )
        rows = self._cursor.fetchall()
        return [ManifestEntry.from_row(row) for row in rows]

    def search(
        self,
        like: str = '',
        before_timestamp: Optional[int] = None,
        after_timestamp: Optional[int] = None,
        file_limit: Optional[int] = None,
        history_limit: Optional[int] = None,
    ) -> List[QueryResponse]:
        """
        Search the manifest for files matching a particular pattern; if no values are given, return
        all files in the manifest

        :param like: the pattern to search for
        :param before_timestamp: only return results before this time
        :param after_timestamp: only return results after this time
        :param file_limit: return no more than this number of files
        :param history_limit: only return this number of changes for a particular file
        :returns: list of ManifestEntries that match the search
        """

        if file_limit == 0 or history_limit == 0:
            return []

        like_query = f"%{like or ''}%"
        before_timestamp = before_timestamp or int(time.time())
        after_timestamp = after_timestamp or 0
        self._cursor.execute(
            '''
            select * from manifest natural left join base_shas
            where abs_file_name like ? and commit_timestamp between ? and ?
            order by abs_file_name, commit_timestamp desc
            ''',
            (like_query, after_timestamp, before_timestamp)
        )

        results: List[QueryResponse] = []
        rows, i, file_count = self._cursor.fetchall(), 0, 0
        while i < len(rows):
            if file_limit and file_count >= file_limit:
                break

            abs_file_name, history, j = rows[i]['abs_file_name'], [], 0
            while i + j < len(rows) and rows[i + j]['abs_file_name'] == abs_file_name:
                if not history_limit or j < history_limit:
                    history.append(ManifestEntry.from_row(rows[i + j]))
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
            (abs_file_name, sha, uid, gid, mode, key_pair, commit_timestamp)
            values (?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                entry.abs_file_name,
                entry.sha,
                entry.uid,
                entry.gid,
                entry.mode,
                entry.key_pair,
                commit_timestamp,
            ),
        )
        if entry.base_sha:
            self._cursor.execute(
                'insert or replace into base_shas (sha, base_sha, base_key_pair) values (?, ?, ?)',
                (entry.sha, entry.base_sha, entry.base_key_pair),
            )
        else:
            self._cursor.execute('delete from base_shas where sha=?', (entry.sha,))
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
                key_pair blob,
                commit_timestamp integer not null
            )
            '''
        )
        self._cursor.execute(
            '''
            create table base_shas (
                sha text not null unique,
                base_sha text not null,
                base_key_pair blob not null unique,
                foreign key(sha) references manifest(sha)
            )
            '''
        )
        self._cursor.execute('create index mfst_idx on manifest(abs_file_name, commit_timestamp)')
        self._cursor.execute('create index sha_idx on manifest(sha)')

        self._commit()


def unlock_manifest(
    manifest_filename: str,
    private_key_filename: str,
    load: Callable[[str, IOIter], IOIter],
    options: OptionsDict,
) -> Manifest:
    """ Load a manifest into local storage and unencrypt it

    :param manifest_filename: the name of the manifest to unlock
    :param private_key_filename: the private key file in PEM format used to encrypt the
        manifest's keypair
    :param load: the _load function from the backup store
    :param options: backup store options
    :returns: the requested Manifest
    """
    local_manifest_filename = path_join(get_scratch_dir(), manifest_filename)
    logger.debug(f'Unlocking manifest at {local_manifest_filename}')

    # First use the private key to read the AES key and nonce used to encrypt the manifest
    key_pair = b''
    if options['use_encryption']:
        with IOIter() as manifest_key:
            ts = manifest_filename.split('.', 1)[1]
            load(MANIFEST_KEY_FILE.format(ts=ts), manifest_key)
            # the key is not large enough to worry about chunked reads, so just do it all at once
            manifest_key.fd.seek(0)
            encrypted_key_pair = manifest_key.fd.read()
        key_pair = decrypt_and_verify(encrypted_key_pair, private_key_filename)

    # Now use the key and nonce to decrypt the manifest
    with IOIter() as encrypted_local_manifest, \
            IOIter(local_manifest_filename, check_mtime=False) as local_manifest:
        load(manifest_filename, encrypted_local_manifest)
        decrypt_and_unpack(encrypted_local_manifest, local_manifest, key_pair, options)

    return Manifest(local_manifest_filename)


def lock_manifest(
    manifest: Manifest,
    private_key_filename: str,
    save: Callable[[IOIter, str], None],
    options: OptionsDict,
) -> None:
    """ Save a manifest from local storage to the backup store

    :param manifest: the manifest object to save
    :param private_key_filename: the private key file in PEM format used to encrypt the
        manifest's keypair
    :param load: the _save function from the backup store
    :param options: backup store options
    :returns: the requested Manifest
    """

    timestamp = time.time()
    local_manifest_filename = manifest.filename
    logger.debug(f'Locking manifest at {local_manifest_filename}')

    # First generate a new key and nonce to encrypt the manifest
    key_pair = b''
    if options['use_encryption']:
        key_pair = generate_key_pair()

    # Next, use that key and nonce to encrypt and save the manifest
    with IOIter(local_manifest_filename) as local_manifest, \
            IOIter(local_manifest_filename + '.enc') as encrypted_manifest:
        signature = compress_and_encrypt(local_manifest, encrypted_manifest, key_pair, options)
        save(encrypted_manifest, MANIFEST_FILE.format(ts=timestamp))

    # Finally, save the manifest key/nonce along with its HMAC using the user's private key
    if options['use_encryption']:
        with IOIter(local_manifest_filename + '.key') as new_manifest_key:
            new_manifest_key.fd.write(encrypt_and_sign(key_pair + signature, private_key_filename))
            new_manifest_key.fd.seek(0)
            save(new_manifest_key, MANIFEST_KEY_FILE.format(ts=timestamp))
