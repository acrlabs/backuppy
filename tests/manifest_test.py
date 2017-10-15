import os
from hashlib import sha256

import mock
import pytest

from backuppy.manifest import Manifest


@pytest.fixture
def fake_filesystem(fs):
    fs.CreateFile('/a/dummy/file1', contents='foo')
    fs.CreateFile('/a/dummy/file2', contents='bar')
    fs.CreateFile('/b/dummy/file1', contents='baz')


@pytest.fixture
def mock_manifest(fake_filesystem):
    return Manifest(['/a', '/b'])


def test_manifest_init(mock_manifest):
    assert set(mock_manifest.contents.keys()) == set(['/a', '/b'])


def test_manifest_update(mock_manifest, fs):
    mock_manifest.update()

    assert len(mock_manifest.contents['/a']) == 2
    assert len(mock_manifest.contents['/b']) == 1
    assert '/a/dummy/file1' in mock_manifest.contents['/a']
    assert '/a/dummy/file2' in mock_manifest.contents['/a']
    assert '/b/dummy/file1' in mock_manifest.contents['/b']
    assert len(mock_manifest.contents['/a']['/a/dummy/file1']) == 1
    assert len(mock_manifest.contents['/a']['/a/dummy/file2']) == 1
    assert len(mock_manifest.contents['/b']['/b/dummy/file1']) == 1
    assert mock_manifest.contents['/a']['/a/dummy/file1'][0].sha == sha256('foo'.encode()).hexdigest()
    assert mock_manifest.contents['/a']['/a/dummy/file2'][0].sha == sha256('bar'.encode()).hexdigest()
    assert mock_manifest.contents['/b']['/b/dummy/file1'][0].sha == sha256('baz'.encode()).hexdigest()

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
    assert mock_manifest.contents['/a']['/a/new/file'][0].sha == sha256('hello, world!'.encode()).hexdigest()

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
    assert mock_manifest.contents['/a']['/a/new/file'][0].sha == sha256('hello, world!'.encode()).hexdigest()
    assert mock_manifest.contents['/a']['/a/new/file'][1].sha == sha256('hello, everyone!'.encode()).hexdigest()

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
    assert mock_manifest.contents['/a']['/a/new/file'][0].sha == sha256('hello, world!'.encode()).hexdigest()
    assert mock_manifest.contents['/a']['/a/new/file'][1].sha == sha256('hello, everyone!'.encode()).hexdigest()
    assert mock_manifest.contents['/a']['/a/new/file'][2].sha == sha256('hello, everyone!'.encode()).hexdigest()
    assert mock_manifest.contents['/a']['/a/new/file'][2].uid == 1000
    assert mock_manifest.contents['/a']['/a/new/file'][2].gid == 1001


def test_manifest_save_load(mock_manifest):
    mock_manifest.save('/manifest')
    m = Manifest.load('/manifest')
    assert m == mock_manifest


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
    assert mock_manifest.contents['/a']['/a/dummy/file2'][0].sha == sha256('bar'.encode()).hexdigest()
    assert mock_manifest.contents['/b']['/b/dummy/file1'][0].sha == sha256('baz'.encode()).hexdigest()
