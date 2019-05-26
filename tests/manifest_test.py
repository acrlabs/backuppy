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
        st_mtime=100,
        st_uid=1000,
        st_gid=2000,
        st_mode=34622,
    )


@pytest.fixture
def mock_manifest():
    m = Manifest(':memory:', start_new_manifest=True)
    m._cursor.execute(
        '''
        insert into manifest (abs_file_name, sha, mtime, uid, gid, mode, commit_timestamp)
        values
        ('/foo', '12345678', 1, 1000, 2000, 34622, 50),
        ('/foo', '12345679', 1, 1000, 2000, 34622, 100),
        ('/bar', 'abcdef78', 4, 1000, 2000, 34622, 55),
        ('/bar', '123def78', 110, 1000, 2000, 34622, 200),
        ('/baz', 'fdecba21', 18, 1000, 2000, 34622, 50),
        ('/baz', null, null, null, null, null, 100)
        '''
    )
    m._cursor.execute('insert into diff_pairs (sha, base_sha) values ("123def78", "abcdef78")')
    return m


def test_load_new_manifest(mock_manifest):
    mock_manifest._cursor.execute(
        '''
        select name from sqlite_master
        where type ='table' and name not like 'sqlite_%'
        '''
    )
    rows = mock_manifest._cursor.fetchall()
    assert {r['name'] for r in rows} == {'manifest', 'diff_pairs'}


def test_load_existing_manifest():
    m = Manifest(':memory:', start_new_manifest=False)
    m._cursor.execute(
        '''
        select name from sqlite_master
        where type ='table' and name not like 'sqlite_%'
        '''
    )
    assert not m._cursor.fetchall()


def test_get_entry_no_entry(mock_manifest):
    assert not mock_manifest.get_entry('/does_not_exist')


@pytest.mark.parametrize('commit_timestamp', [75, 200])
def test_get_entry_no_diff(mock_manifest, commit_timestamp):
    entry = mock_manifest.get_entry('/foo', commit_timestamp)
    assert entry.abs_file_name == '/foo'
    assert entry.sha == ('12345678' if commit_timestamp == 75 else '12345679')
    assert entry.mtime == 1
    assert entry.uid == 1000
    assert entry.gid == 2000
    assert entry.mode == 34622


def test_get_entry_with_diff(mock_manifest):
    entry = mock_manifest.get_entry('/bar')
    assert entry.abs_file_name == '/bar'
    assert entry.sha == '123def78'
    assert entry.base_sha == 'abcdef78'
    assert entry.mtime == 110
    assert entry.uid == 1000
    assert entry.gid == 2000
    assert entry.mode == 34622


def test_get_entry_file_deleted(mock_manifest):
    entry = mock_manifest.get_entry('/baz', 110)
    assert entry.abs_file_name == '/baz'
    assert not entry.sha
    assert not entry.mtime
    assert not entry.uid
    assert not entry.gid
    assert not entry.mode


@pytest.mark.parametrize('base_sha', [None, 'f33b'])
def test_insert_new_file(mock_manifest, mock_stat, base_sha):
    new_file = '/not/backed/up'
    new_entry = ManifestEntry.from_stat(new_file, sha='b33f', base_sha=base_sha, file_stat=mock_stat)
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
    assert rows[0]['mtime'] == 100
    assert rows[0]['uid'] == 1000
    assert rows[0]['gid'] == 2000
    assert rows[0]['mode'] == 34622
    assert rows[0]['commit_timestamp'] == 1000


@pytest.mark.parametrize('base_sha', [None, 'f33b2'])
def test_update(mock_manifest, mock_stat, base_sha):
    new_file = '/foo'
    new_entry = ManifestEntry.from_stat(new_file, sha='b33f2', base_sha=base_sha, file_stat=mock_stat)
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
    assert rows[-1]['mtime'] == 100
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
    assert not rows[-1]['mtime']
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
