import mock
import pytest

from backuppy.exceptions import BackupCorruptedError
from backuppy.manifest import lock_manifest
from backuppy.manifest import Manifest
from backuppy.manifest import ManifestEntry
from backuppy.manifest import unlock_manifest

INITIAL_FILES = ['/file1', '/file2', '/file3']


@pytest.fixture(autouse=True)
def mock_time():
    with mock.patch('backuppy.manifest.time.time', return_value=1000):
        yield


@pytest.fixture
def mock_stat():
    return mock.Mock(
        st_uid=1000,
        st_gid=2000,
        st_mode=34622,
    )


@pytest.fixture
def mock_manifest():
    m = Manifest(':memory:')
    m._cursor.execute(
        '''
        insert into manifest (abs_file_name, sha, uid, gid, mode, key_pair, commit_timestamp)
        values
        ('/foo', '12345678', 1000, 2000, 34622, '1234', 50),
        ('/foo', '12345679', 1000, 2000, 34622, '1235', 100),
        ('/bar', 'abcdef78', 1000, 2000, 34622, '1236', 55),
        ('/bar', '123def78', 1000, 2000, 34622, '1237', 200),
        ('/baz', 'fdecba21', 1000, 2000, 34622, '1238', 50),
        ('/baz', null, null, null, null, null, 100)
        '''
    )
    m._cursor.execute(
        '''
        insert into base_shas (sha, base_sha, base_key_pair)
        values ('123def78', 'abcdef78', 'abcd')
        '''
    )
    return m


@pytest.mark.parametrize('existing_tables', [[], [{'name': 'manifest'}, {'name': 'base_shas'}]])
def test_create_manifest_object(existing_tables):
    with mock.patch('backuppy.manifest.sqlite3') as mock_sqlite, \
            mock.patch('backuppy.manifest.Manifest._create_manifest_tables') as mock_create_tables:
        mock_sqlite.connect.return_value.cursor.return_value.fetchall.return_value = existing_tables
        Manifest('my_manifest.sqlite')
        assert mock_create_tables.call_count == 1 - bool(len(existing_tables))


@pytest.mark.parametrize('existing_tables', [
    [{'name': 'manifest'}],
    [{'name': 'manifest'}, {'name': 'foo'}],
])
def test_corrupted_manifest(existing_tables):
    with mock.patch('backuppy.manifest.sqlite3') as mock_sqlite, \
            mock.patch('backuppy.manifest.Manifest._create_manifest_tables') as mock_create_tables:
        mock_sqlite.connect.return_value.cursor.return_value.fetchall.return_value = existing_tables
        with pytest.raises(BackupCorruptedError):
            Manifest('my_manifest.sqlite')
        assert mock_create_tables.call_count == 0


def test_get_entry_no_entry(mock_manifest):
    assert not mock_manifest.get_entry('/does_not_exist')


@pytest.mark.parametrize('commit_timestamp', [75, 200])
def test_get_entry_no_diff(mock_manifest, commit_timestamp):
    entry = mock_manifest.get_entry('/foo', commit_timestamp)
    assert entry.abs_file_name == '/foo'
    assert entry.sha == ('12345678' if commit_timestamp == 75 else '12345679')
    assert entry.uid == 1000
    assert entry.gid == 2000
    assert entry.mode == 34622


def test_get_entry_with_diff(mock_manifest):
    entry = mock_manifest.get_entry('/bar')
    assert entry.abs_file_name == '/bar'
    assert entry.sha == '123def78'
    assert entry.base_sha == 'abcdef78'
    assert entry.uid == 1000
    assert entry.gid == 2000
    assert entry.mode == 34622


def test_get_entry_file_deleted(mock_manifest):
    entry = mock_manifest.get_entry('/baz', 110)
    assert entry.abs_file_name == '/baz'
    assert not entry.sha
    assert not entry.uid
    assert not entry.gid
    assert not entry.mode


def test_get_entries_by_sha(mock_manifest):
    entries = mock_manifest.get_entries_by_sha('1')
    assert len(entries) == 3


def test_search(mock_manifest):
    results = mock_manifest.search()
    assert len(results) == 3
    for path, history in results:
        assert len(history) == 2
        assert history[0].commit_timestamp > history[1].commit_timestamp


def test_search_with_query(mock_manifest):
    results = mock_manifest.search(like='ba')
    assert len(results) == 2
    assert results[0][0] == '/bar'
    assert results[1][0] == '/baz'


def test_search_time_window(mock_manifest):
    results = mock_manifest.search(after_timestamp=60, before_timestamp=150)
    assert len(results) == 2
    assert results[0][0] == '/baz'
    assert results[1][0] == '/foo'
    for path, history in results:
        assert 60 < history[0].commit_timestamp < 150


@pytest.mark.parametrize('limit', [0, 1])
def test_search_file_limit(mock_manifest, limit):
    results = mock_manifest.search(file_limit=limit)
    assert len(results) == limit
    for path, history in results:
        assert len(history) == 2


@pytest.mark.parametrize('limit', [0, 1])
def test_search_history_limit(mock_manifest, limit):
    results = mock_manifest.search(history_limit=limit)
    assert len(results) == (0 if limit == 0 else 3)
    for path, history in results:
        assert len(history) == 1


@pytest.mark.parametrize('base_sha,base_key_pair', [(None, None), ('f33b', b'2222')])
def test_insert_new_file(mock_manifest, mock_stat, base_sha, base_key_pair):
    new_file = '/not/backed/up'
    uid, gid, mode = mock_stat.st_uid, mock_stat.st_gid, mock_stat.st_mode
    new_entry = ManifestEntry(new_file, 'b33f', base_sha, uid, gid, mode, b'1111', base_key_pair)
    mock_manifest.insert_or_update(new_entry)
    mock_manifest._cursor.execute(
        '''
        select * from manifest left natural join base_shas
        where abs_file_name = '/not/backed/up'
        order by commit_timestamp
        '''
    )
    rows = mock_manifest._cursor.fetchall()
    assert len(rows) == 1
    assert rows[0]['abs_file_name'] == new_file
    assert rows[0]['sha'] == 'b33f'
    assert rows[0]['base_sha'] == base_sha
    assert rows[0]['uid'] == 1000
    assert rows[0]['gid'] == 2000
    assert rows[0]['mode'] == 34622
    assert rows[0]['key_pair'] == b'1111'
    assert rows[0]['base_key_pair'] == base_key_pair
    assert rows[0]['commit_timestamp'] == 1000


@pytest.mark.parametrize('base_sha,base_key_pair', [(None, None), ('f33b', b'2222')])
def test_update(mock_manifest, mock_stat, base_sha, base_key_pair):
    new_file = '/foo'
    uid, gid, mode = mock_stat.st_uid, mock_stat.st_gid, mock_stat.st_mode
    new_entry = ManifestEntry(new_file, 'b33f2', base_sha, uid, gid, mode, b'1111', base_key_pair)
    mock_manifest.insert_or_update(new_entry)
    mock_manifest._cursor.execute(
        '''
        select * from manifest left natural join base_shas
        where abs_file_name = '/foo'
        order by commit_timestamp
        '''
    )
    rows = mock_manifest._cursor.fetchall()
    assert len(rows) == 3
    assert rows[-1]['abs_file_name'] == new_file
    assert rows[-1]['sha'] == 'b33f2'
    assert rows[-1]['base_sha'] == base_sha
    assert rows[-1]['uid'] == 1000
    assert rows[-1]['gid'] == 2000
    assert rows[-1]['mode'] == 34622
    assert rows[-1]['key_pair'] == b'1111'
    assert rows[-1]['base_key_pair'] == base_key_pair
    assert rows[-1]['commit_timestamp'] == 1000


def test_insert_duplicate(mock_manifest, mock_stat):
    same_file = '/foo'
    uid, gid, mode = mock_stat.st_uid, mock_stat.st_gid, mock_stat.st_mode
    new_entry = ManifestEntry(same_file, '12345678', None, uid, gid, mode, b'1111', None)
    mock_manifest.insert_or_update(new_entry)
    mock_manifest._cursor.execute(
        '''
        select * from manifest
        where abs_file_name = '/foo'
        order by commit_timestamp
        '''
    )
    rows = mock_manifest._cursor.fetchall()
    assert len(rows) == 2
    assert rows[-1]['abs_file_name'] == same_file
    assert rows[0]['sha'] == '12345679'
    assert rows[-1]['sha'] == '12345678'
    assert rows[-1]['uid'] == 1000
    assert rows[-1]['gid'] == 2000
    assert rows[-1]['mode'] == 34622
    assert rows[-1]['key_pair'] == b'1111'
    assert rows[-1]['commit_timestamp'] == 1000


def test_insert_diff_key_pair_for_sha(mock_manifest, mock_stat):
    new_file = '/somebody_new'
    uid, gid, mode = mock_stat.st_uid, mock_stat.st_gid, mock_stat.st_mode
    new_entry = ManifestEntry(new_file, '12345678', None, uid, gid, mode, b'2222', None)
    mock_manifest.insert_or_update(new_entry)
    mock_manifest._cursor.execute(
        '''
        select * from manifest where sha = '12345678'
        '''
    )
    rows = mock_manifest._cursor.fetchall()
    assert len(rows) == 2
    assert all([r['key_pair'] == b'2222' for r in rows])


def test_delete(mock_manifest):
    deleted_file = '/foo'
    mock_manifest.delete(deleted_file)
    mock_manifest._cursor.execute(
        '''
        select * from manifest left natural join base_shas
        where abs_file_name = '/foo'
        order by commit_timestamp
        '''
    )
    rows = mock_manifest._cursor.fetchall()
    assert len(rows) == 3
    assert rows[-1]['abs_file_name'] == deleted_file
    assert not rows[-1]['sha']
    assert not rows[-1]['base_sha']
    assert not rows[-1]['uid']
    assert not rows[-1]['gid']
    assert not rows[-1]['mode']
    assert not rows[-1]['key_pair']
    assert not rows[-1]['base_key_pair']
    assert rows[-1]['commit_timestamp'] == 1000


def test_delete_unknown(mock_manifest, caplog):
    with mock.patch('backuppy.manifest.logger') as mock_logger:
        mock_manifest.delete('/not/backed/up')
        mock_manifest._cursor.execute(
            '''
            select * from manifest left natural join base_shas
            where abs_file_name = '/not/backed/up'
            '''
        )
        rows = mock_manifest._cursor.fetchall()
        assert mock_logger.warn.call_count == 1
        assert not rows


@pytest.mark.parametrize('timestamp', [60, 1000])
def test_tracked_files(mock_manifest, timestamp):
    expected = set(['/foo', '/bar'])
    if timestamp < 100:
        expected.add('/baz')
    assert mock_manifest.files(timestamp) == expected


def test_find_duplicate_entries(mock_manifest):
    mock_manifest._cursor.execute('drop index mfst_unique_idx')
    mock_manifest._cursor.execute(
        '''
        insert into manifest (abs_file_name, sha, uid, gid, mode, key_pair, commit_timestamp)
        values
        ('/foo', '12345678', 1000, 2000, 34622, '5678', 1000),
        ('/foo', '12345678', 1000, 3000, 34622, '5678', 1000)
        '''
    )
    assert len(mock_manifest.find_duplicate_entries()) == 2


def test_find_shas_with_multiple_key_pairs(mock_manifest):
    mock_manifest._cursor.execute(
        '''
        insert into manifest (abs_file_name, sha, uid, gid, mode, key_pair, commit_timestamp)
        values
        ('/bar', '12345678', 1000, 3000, 34622, '5678', 1000)
        '''
    )
    assert len(mock_manifest.find_shas_with_multiple_key_pairs()) == 2


def test_delete_entry(mock_manifest):
    mock_manifest.delete_entry(
        ManifestEntry('/foo', '12345678', None, 1000, 2000, 34622, b'1234', None, 50),
    )
    mock_manifest._cursor.execute(
        '''
        select * from manifest where abs_file_name = '/foo'
        '''
    )
    rows = mock_manifest._cursor.fetchall()
    assert len(rows) == 1


@pytest.mark.parametrize('use_encryption', [True, False])
def test_unlock_manifest(use_encryption):
    mock_load = mock.Mock()
    with mock.patch('backuppy.manifest.IOIter'), \
            mock.patch('backuppy.manifest.decrypt_and_verify') as mock_decrypt_pub_key, \
            mock.patch('backuppy.manifest.decrypt_and_unpack'), \
            mock.patch('backuppy.manifest.Manifest'):
        unlock_manifest(
            'my_manifest.sqlite',
            '/path/to/private/key',
            mock_load,
            options={'use_encryption': use_encryption},
        )
    assert mock_load.call_count == 1 + use_encryption
    assert mock_decrypt_pub_key.call_count == use_encryption


@pytest.mark.parametrize('use_encryption', [True, False])
def test_lock_manifest(mock_manifest, use_encryption):
    mock_save = mock.Mock()
    with mock.patch('backuppy.manifest.IOIter'), \
            mock.patch('backuppy.manifest.encrypt_and_sign') as mock_decrypt_pub_key, \
            mock.patch('backuppy.manifest.compress_and_encrypt'), \
            mock.patch('backuppy.manifest.unlock_manifest'):
        lock_manifest(
            mock_manifest,
            '/path/to/private/key',
            mock_save,
            mock.Mock(),
            options={'use_encryption': use_encryption},
        )
    assert mock_save.call_count == 1 + use_encryption
    assert mock_decrypt_pub_key.call_count == use_encryption


def test_lock_manifest_error(mock_manifest, caplog):
    with mock.patch('backuppy.manifest.IOIter'), \
            mock.patch('backuppy.manifest.encrypt_and_sign'), \
            mock.patch('backuppy.manifest.compress_and_encrypt'), \
            mock.patch('backuppy.manifest.unlock_manifest', side_effect=Exception), \
            pytest.raises(Exception):
        lock_manifest(
            mock_manifest,
            '/path/to/private/key',
            mock.Mock(),
            mock.Mock(),
            options={'use_encryption': True},
        )

    assert 'saved manifest could not be decrypted' in caplog.records[-1].message
