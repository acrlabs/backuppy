import os
from hashlib import sha256

import mock
import pytest

from backuppy.manifest import Manifest
from backuppy.manifest import ManifestEntry


def sha(string):
    return sha256(string.encode()).hexdigest()


@pytest.fixture
def fake_filesystem(fs):
    fs.CreateFile('/a/dummy/file1', contents='foo')
    fs.CreateFile('/a/dummy/file2', contents='bar')
    fs.CreateFile('/b/dummy/file1', contents='baz')
    fs.CreateFile('/c/not/backed/up', contents='whatever')


@pytest.fixture
def mock_manifest(fake_filesystem):
    return Manifest(['/a', '/b'])


def test_manifest_init(mock_manifest):
    assert set(mock_manifest.contents.keys()) == set(['/a', '/b'])


def test_manifest_update(mock_manifest, fs):
    mock_manifest.update()

    assert len(mock_manifest.contents['/a']) == 2
    assert len(mock_manifest.contents['/b']) == 1
    assert '/c' not in mock_manifest.contents
    assert '/a/dummy/file1' in mock_manifest.contents['/a']
    assert '/a/dummy/file2' in mock_manifest.contents['/a']
    assert '/b/dummy/file1' in mock_manifest.contents['/b']
    assert len(mock_manifest.contents['/a']['/a/dummy/file1']) == 1
    assert len(mock_manifest.contents['/a']['/a/dummy/file2']) == 1
    assert len(mock_manifest.contents['/b']['/b/dummy/file1']) == 1
    assert mock_manifest.contents['/a']['/a/dummy/file1'][0][1].sha == sha('foo')
    assert mock_manifest.contents['/a']['/a/dummy/file2'][0][1].sha == sha('bar')
    assert mock_manifest.contents['/b']['/b/dummy/file1'][0][1].sha == sha('baz')

    new_file = fs.CreateFile('/a/new/file', contents='hello, world!')
    mock_manifest.update()

    assert len(mock_manifest.contents['/a']) == 3
    assert len(mock_manifest.contents['/b']) == 1
    assert '/a/dummy/file1' in mock_manifest.contents['/a']
    assert '/a/dummy/file2' in mock_manifest.contents['/a']
    assert '/a/new/file' in mock_manifest.contents['/a']
    assert '/b/dummy/file1' in mock_manifest.contents['/b']
    assert len(mock_manifest.contents['/a']['/a/dummy/file1']) == 1
    assert len(mock_manifest.contents['/a']['/a/dummy/file2']) == 1
    assert len(mock_manifest.contents['/a']['/a/new/file']) == 1
    assert len(mock_manifest.contents['/b']['/b/dummy/file1']) == 1
    assert mock_manifest.contents['/a']['/a/new/file'][0][1].sha == sha('hello, world!')

    new_file.SetContents('hello, everyone!')
    mock_manifest.update()

    assert len(mock_manifest.contents['/a']) == 3
    assert len(mock_manifest.contents['/b']) == 1
    assert '/a/dummy/file1' in mock_manifest.contents['/a']
    assert '/a/dummy/file2' in mock_manifest.contents['/a']
    assert '/a/new/file' in mock_manifest.contents['/a']
    assert '/b/dummy/file1' in mock_manifest.contents['/b']
    assert len(mock_manifest.contents['/a']['/a/dummy/file1']) == 1
    assert len(mock_manifest.contents['/a']['/a/dummy/file2']) == 1
    assert len(mock_manifest.contents['/a']['/a/new/file']) == 2
    assert len(mock_manifest.contents['/b']['/b/dummy/file1']) == 1
    assert mock_manifest.contents['/a']['/a/new/file'][0][1].sha == sha('hello, world!')
    assert mock_manifest.contents['/a']['/a/new/file'][1][1].sha == sha('hello, everyone!')

    os.chown('/a/new/file', 1000, 1001)
    mock_manifest.update()

    assert len(mock_manifest.contents['/a']) == 3
    assert len(mock_manifest.contents['/b']) == 1
    assert '/a/dummy/file1' in mock_manifest.contents['/a']
    assert '/a/dummy/file2' in mock_manifest.contents['/a']
    assert '/a/new/file' in mock_manifest.contents['/a']
    assert '/b/dummy/file1' in mock_manifest.contents['/b']
    assert len(mock_manifest.contents['/a']['/a/dummy/file1']) == 1
    assert len(mock_manifest.contents['/a']['/a/dummy/file2']) == 1
    assert len(mock_manifest.contents['/a']['/a/new/file']) == 3
    assert len(mock_manifest.contents['/b']['/b/dummy/file1']) == 1
    assert mock_manifest.contents['/a']['/a/new/file'][0][1].sha == sha('hello, world!')
    assert mock_manifest.contents['/a']['/a/new/file'][1][1].sha == sha('hello, everyone!')
    assert mock_manifest.contents['/a']['/a/new/file'][2][1].sha == sha('hello, everyone!')
    assert mock_manifest.contents['/a']['/a/new/file'][2][1].uid == 1000
    assert mock_manifest.contents['/a']['/a/new/file'][2][1].gid == 1001

    os.remove('/a/dummy/file1')
    mock_manifest.update()

    assert len(mock_manifest.contents['/a']) == 3
    assert len(mock_manifest.contents['/b']) == 1
    assert '/a/dummy/file1' in mock_manifest.contents['/a']
    assert '/a/dummy/file2' in mock_manifest.contents['/a']
    assert '/a/new/file' in mock_manifest.contents['/a']
    assert '/b/dummy/file1' in mock_manifest.contents['/b']
    assert len(mock_manifest.contents['/a']['/a/dummy/file1']) == 2
    assert len(mock_manifest.contents['/a']['/a/dummy/file2']) == 1
    assert len(mock_manifest.contents['/a']['/a/new/file']) == 3
    assert len(mock_manifest.contents['/b']['/b/dummy/file1']) == 1
    assert mock_manifest.contents['/a']['/a/dummy/file1'][0][1].sha == sha('foo')
    assert mock_manifest.contents['/a']['/a/dummy/file1'][1][1] is None

    fs.CreateFile('/a/dummy/file1', contents='recreated')
    mock_manifest.update()

    assert len(mock_manifest.contents['/a']) == 3
    assert len(mock_manifest.contents['/b']) == 1
    assert '/a/dummy/file1' in mock_manifest.contents['/a']
    assert '/a/dummy/file2' in mock_manifest.contents['/a']
    assert '/a/new/file' in mock_manifest.contents['/a']
    assert '/b/dummy/file1' in mock_manifest.contents['/b']
    assert len(mock_manifest.contents['/a']['/a/dummy/file1']) == 3
    assert len(mock_manifest.contents['/a']['/a/dummy/file2']) == 1
    assert len(mock_manifest.contents['/a']['/a/new/file']) == 3
    assert len(mock_manifest.contents['/b']['/b/dummy/file1']) == 1
    assert mock_manifest.contents['/a']['/a/dummy/file1'][0][1].sha == sha('foo')
    assert mock_manifest.contents['/a']['/a/dummy/file1'][1][1] is None
    assert mock_manifest.contents['/a']['/a/dummy/file1'][2][1].sha == sha('recreated')


def test_manifest_save_load(mock_manifest):
    mock_manifest.save('/manifest')
    m = Manifest.load('/manifest')
    assert m.contents == mock_manifest.contents


@mock.patch('backuppy.manifest.logger')
def test_manifest_invalid_entry(mock_logger, mock_manifest):
    os.chmod('/a/dummy/file1', 0)
    mock_manifest.update()

    assert mock_logger.warn.call_count == 1
    assert len(mock_manifest.contents['/a']) == 1
    assert len(mock_manifest.contents['/b']) == 1
    assert '/a/dummy/file2' in mock_manifest.contents['/a']
    assert '/b/dummy/file1' in mock_manifest.contents['/b']
    assert len(mock_manifest.contents['/a']['/a/dummy/file2']) == 1
    assert len(mock_manifest.contents['/b']['/b/dummy/file1']) == 1
    assert mock_manifest.contents['/a']['/a/dummy/file2'][0][1].sha == sha('bar')
    assert mock_manifest.contents['/b']['/b/dummy/file1'][0][1].sha == sha('baz')


def test_manifest_with_exclusions(mock_manifest):
    mock_manifest.update({'/a': ['file']})
    assert len(mock_manifest.contents['/a']) == 0
    assert len(mock_manifest.contents['/b']) == 1
    assert '/b/dummy/file1' in mock_manifest.contents['/b']
    assert len(mock_manifest.contents['/b']['/b/dummy/file1']) == 1
    assert mock_manifest.contents['/b']['/b/dummy/file1'][0][1].sha == sha('baz')

    mock_manifest.update({'/b': ['.*']})
    assert len(mock_manifest.contents['/a']) == 2
    assert len(mock_manifest.contents['/b']) == 1
    assert '/a/dummy/file1' in mock_manifest.contents['/a']
    assert '/a/dummy/file2' in mock_manifest.contents['/a']
    assert '/b/dummy/file1' in mock_manifest.contents['/b']
    assert len(mock_manifest.contents['/a']['/a/dummy/file1']) == 1
    assert len(mock_manifest.contents['/a']['/a/dummy/file2']) == 1
    assert len(mock_manifest.contents['/b']['/b/dummy/file1']) == 2
    assert mock_manifest.contents['/b']['/b/dummy/file1'][0][1].sha == sha('baz')
    assert mock_manifest.contents['/b']['/b/dummy/file1'][1][1] is None


@mock.patch('backuppy.manifest.time')
def test_manifest_snapshot(mock_time, mock_manifest, fs):
    mock_time.time.side_effect = [1, 10, 50, 60, 100]

    adummyfile1 = ManifestEntry('/a/dummy/file1')
    adummyfile2 = ManifestEntry('/a/dummy/file2')
    bdummyfile1 = ManifestEntry('/b/dummy/file1')
    mock_manifest.update()

    f = fs.GetObject('/a/dummy/file1')
    f.SetContents('lorem ipsum')

    adummyfile1changed = ManifestEntry('/a/dummy/file1')
    mock_manifest.update()

    f = fs.GetObject('/b/dummy/file1')
    f.SetContents('hello, world!')
    fs.CreateFile('/a/new/file', contents='i am a new file')

    bdummyfile1changed = ManifestEntry('/b/dummy/file1')
    anewfile = ManifestEntry('/a/new/file')
    mock_manifest.update()

    os.remove('/a/dummy/file2')
    mock_manifest.update()

    fs.CreateFile('/a/dummy/file2', contents='bar')
    mock_manifest.update()

    assert mock_manifest.snapshot(0) == {}
    assert mock_manifest.snapshot(5) == {
        '/a/dummy/file1': adummyfile1,
        '/a/dummy/file2': adummyfile2,
        '/b/dummy/file1': bdummyfile1,
    }
    assert mock_manifest.snapshot(30) == {
        '/a/dummy/file1': adummyfile1changed,
        '/a/dummy/file2': adummyfile2,
        '/b/dummy/file1': bdummyfile1,
    }
    assert mock_manifest.snapshot(55) == {
        '/a/dummy/file1': adummyfile1changed,
        '/a/dummy/file2': adummyfile2,
        '/a/new/file': anewfile,
        '/b/dummy/file1': bdummyfile1changed,
    }
    assert mock_manifest.snapshot(75) == {
        '/a/dummy/file1': adummyfile1changed,
        '/a/new/file': anewfile,
        '/b/dummy/file1': bdummyfile1changed,
    }
    assert mock_manifest.snapshot(125) == {
        '/a/dummy/file1': adummyfile1changed,
        '/a/dummy/file2': adummyfile2,
        '/a/new/file': anewfile,
        '/b/dummy/file1': bdummyfile1changed,
    }
