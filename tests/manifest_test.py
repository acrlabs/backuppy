from copy import deepcopy

import mock
import pytest

from backuppy.manifest import Manifest
from backuppy.manifest import ManifestEntry

INITIAL_FILES = ['/file1', '/file2', '/file3']


@pytest.fixture
def mock_time():
    with mock.patch('backuppy.manifest.time') as mock_time:
        mock_time.time.return_value = 1
        yield mock_time


@pytest.fixture
def new_entry():
    with mock.patch('backuppy.manifest.os.stat'):
        return ManifestEntry('/file1', sha=f'def1234')


@pytest.fixture
def manifest():
    m = Manifest()
    with mock.patch('backuppy.manifest.os.stat'):
        m.contents = {name: [(0, ManifestEntry(name, sha=f'abcd{i}'), False)] for i, name in enumerate(INITIAL_FILES)}
    return m


def test_get_diff_pair_no_entry(manifest):
    assert manifest.get_diff_pair('/foo') == (None, None)


def test_get_diff_pair_only_base(manifest):
    for name in INITIAL_FILES:
        entry = manifest.contents[name][0][1]
        assert manifest.get_diff_pair(name) == (entry, entry)


def test_get_diff_pair_changed(manifest, new_entry):
    base, latest = manifest.contents['/file1'][0][1], new_entry
    manifest.contents['/file1'].append([1, latest, True])
    assert manifest.get_diff_pair('/file1') == (base, latest)


def test_get_diff_pair_file_deleted(manifest):
    manifest.contents['/file1'].append([1, None, False])
    assert manifest.get_diff_pair('/file1') == (None, None)


def test_get_diff_pair_file_deleted_and_restored(manifest, new_entry):
    base, latest = manifest.contents['/file1'][0][1], new_entry
    manifest.contents['/file1'].append([1, None, False])
    manifest.contents['/file1'].append([2, base, False])
    manifest.contents['/file1'].append([3, latest, True])
    assert manifest.get_diff_pair('/file1') == (base, latest)


def test_get_diff_pair_timestamp(manifest):
    pass


def test_is_current(manifest):
    with mock.patch('backuppy.manifest.ManifestEntry') as mock_entry:
        for name in INITIAL_FILES:
            mock_entry.return_value = manifest.contents[name][0][1]
            assert manifest.is_current(name)

            mock_entry.return_value = mock.Mock()
            assert not manifest.is_current(name)


def test_insert(mock_time, manifest):
    mock_time.return_value = 1
    new_file = '/not/backed/up'
    with mock.patch('backuppy.manifest.os.stat'):
        new_entry = ManifestEntry(new_file, sha='b33f')
    manifest.insert_or_update(new_file, new_entry, True)
    assert set(manifest.contents.keys()) == set(INITIAL_FILES + [new_file])
    assert manifest.contents[new_file] == [(1, new_entry, True)]
    for entries in manifest.contents.values():
        assert len(entries) == 1


def test_update(mock_time, manifest, new_entry):
    mock_time.return_value = 1
    manifest.insert_or_update('/file1', new_entry, True)
    assert set(manifest.contents.keys()) == set(INITIAL_FILES)
    assert manifest.contents[INITIAL_FILES[0]][-1] == (1, new_entry, True)
    assert len(manifest.contents[INITIAL_FILES[0]]) == 2
    assert len(manifest.contents[INITIAL_FILES[1]]) == 1
    assert len(manifest.contents[INITIAL_FILES[2]]) == 1


def test_delete(mock_time, manifest):
    manifest.delete(INITIAL_FILES[0])
    assert set(manifest.contents.keys()) == set(INITIAL_FILES)
    assert manifest.contents[INITIAL_FILES[0]][-1] == (1, None, False)
    assert len(manifest.contents[INITIAL_FILES[0]]) == 2
    assert len(manifest.contents[INITIAL_FILES[1]]) == 1
    assert len(manifest.contents[INITIAL_FILES[2]]) == 1


def test_delete_unknown(mock_time, manifest):
    old_contents = deepcopy(manifest.contents)
    with mock.patch('backuppy.manifest.logger') as mock_logger:
        manifest.delete('foo')
        assert mock_logger.warn.call_count == 1
        assert manifest.contents == old_contents


def test_tracked_files(manifest):
    assert manifest.files() == set(INITIAL_FILES)
