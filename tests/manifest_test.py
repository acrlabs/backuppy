import mock
import pytest

from backuppy.manifest import Manifest
from backuppy.manifest import ManifestEntry

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
        insert into manifest (abs_file_name, sha, uid, gid, mode, commit_timestamp)
        values
        ('/foo', '12345678', 1000, 2000, 34622, 50),
        ('/foo', '12345679', 1000, 2000, 34622, 100),
        ('/bar', 'abcdef78', 1000, 2000, 34622, 55),
        ('/bar', '123def78', 1000, 2000, 34622, 200),
        ('/baz', 'fdecba21', 1000, 2000, 34622, 50),
        ('/baz', null, null, null, null, 100)
        '''
    )
    m._cursor.execute('insert into diff_pairs (sha, base_sha) values ("123def78", "abcdef78")')
    return m


@pytest.mark.parametrize('existing_tables', [[], [{'name': 'manifest'}, {'name': 'diff_pairs'}]])
def test_create_manifest_object(existing_tables):
    with mock.patch('backuppy.manifest.sqlite3') as mock_sqlite, \
            mock.patch('backuppy.manifest.Manifest._create_manifest_tables') as mock_create_tables:
        mock_sqlite.connect.return_value.cursor.return_value.fetchall.return_value = existing_tables
        Manifest('my_manifest.sqlite')
        assert mock_create_tables.call_count == 1 - bool(len(existing_tables))


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


def test_search(mock_manifest):
    results = mock_manifest.search()
    assert len(results) == 3
    for path, history in results:
        assert len(history) == 2
        assert history[0][1] > history[1][1]


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
        assert 60 < history[0][1] < 150


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


@pytest.mark.parametrize('base_sha', [None, 'f33b'])
def test_insert_new_file(mock_manifest, mock_stat, base_sha):
    new_file = '/not/backed/up'
    uid, gid, mode = mock_stat.st_uid, mock_stat.st_gid, mock_stat.st_mode
    new_entry = ManifestEntry(new_file, 'b33f', base_sha, uid, gid, mode)
    mock_manifest.insert_or_update(new_entry)
    mock_manifest._cursor.execute(
        '''
        select * from manifest left natural join diff_pairs
        where abs_file_name = '/not/backed/up'
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
    assert rows[0]['commit_timestamp'] == 1000


@pytest.mark.parametrize('base_sha', [None, 'f33b2'])
def test_update(mock_manifest, mock_stat, base_sha):
    new_file = '/foo'
    uid, gid, mode = mock_stat.st_uid, mock_stat.st_gid, mock_stat.st_mode
    new_entry = ManifestEntry(new_file, 'b33f2', base_sha, uid, gid, mode)
    mock_manifest.insert_or_update(new_entry)
    mock_manifest._cursor.execute(
        '''
        select * from manifest left natural join diff_pairs
        where abs_file_name = '/foo'
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
    assert rows[-1]['commit_timestamp'] == 1000


def test_delete(mock_manifest):
    deleted_file = '/foo'
    mock_manifest.delete(deleted_file)
    mock_manifest._cursor.execute(
        '''
        select * from manifest left natural join diff_pairs
        where abs_file_name = '/foo'
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
    assert rows[-1]['commit_timestamp'] == 1000


def test_delete_unknown(mock_manifest, caplog):
    with mock.patch('backuppy.manifest.logger') as mock_logger:
        mock_manifest.delete('/not/backed/up')
        mock_manifest._cursor.execute(
            '''
            select * from manifest left natural join diff_pairs
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
