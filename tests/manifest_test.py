from copy import deepcopy
from hashlib import sha256

import mock
import pytest

from backuppy.manifest import Manifest
from backuppy.manifest import ManifestEntry
from tests.conftest import INITIAL_FILES


def sha(string):
    return sha256(string.encode()).hexdigest()


def overwrite_file(name=INITIAL_FILES[0], contents='foo'):
    with open(name, 'w') as f:
        f.write(contents)


@pytest.fixture
def manifest(fake_filesystem):
    m = Manifest()
    m.contents = {name: [[0, ManifestEntry(name)]] for name in INITIAL_FILES}
    return m


def test_manifest_save_load(manifest):
    manifest.save('/manifest')
    m = Manifest.load('/manifest')
    assert m.contents == manifest.contents


def test_last_entry(manifest):
    for name in INITIAL_FILES:
        assert manifest.get_last_entry(name) == ManifestEntry(name)

    overwrite_file()
    new_entry = ManifestEntry(INITIAL_FILES[0])
    manifest.contents[INITIAL_FILES[0]].append([1, new_entry])
    assert manifest.get_last_entry(INITIAL_FILES[0]) == new_entry


def test_last_entry_not_present(manifest):
    assert manifest.get_last_entry('/file/not/present') is None


def test_is_current(manifest):
    for name in INITIAL_FILES:
        assert manifest.is_current(name)
    overwrite_file()
    assert not manifest.is_current(INITIAL_FILES[0])


def test_is_current_not_present(manifest):
    assert not manifest.is_current('/c/not/backed/up')


@mock.patch('backuppy.manifest.time')
class TestInsertUpdateDelete:
    def test_insert(self, mock_time, manifest):
        mock_time.return_value = 1
        new_file = '/c/not/backed/up'
        new_entry = ManifestEntry(new_file)
        manifest.insert_or_update(new_file, new_entry)
        assert set(manifest.contents.keys()) == set(INITIAL_FILES + [new_file])
        assert manifest.contents[new_file] == [[1, new_entry]]
        for entries in manifest.contents.values():
            assert len(entries) == 1

    def test_update(self, mock_time, manifest):
        mock_time.return_value = 1
        overwrite_file()
        new_entry = ManifestEntry(INITIAL_FILES[0])
        manifest.insert_or_update(INITIAL_FILES[0], new_entry)
        assert set(manifest.contents.keys()) == set(INITIAL_FILES)
        assert manifest.contents[INITIAL_FILES[0]][-1] == [1, new_entry]
        assert len(manifest.contents[INITIAL_FILES[0]]) == 2
        assert len(manifest.contents[INITIAL_FILES[1]]) == 1
        assert len(manifest.contents[INITIAL_FILES[2]]) == 1

    def test_delete(self, mock_time, manifest):
        manifest.delete(INITIAL_FILES[0])
        assert set(manifest.contents.keys()) == set(INITIAL_FILES)
        assert manifest.contents[INITIAL_FILES[0]][-1] == [1, None]
        assert len(manifest.contents[INITIAL_FILES[0]]) == 2
        assert len(manifest.contents[INITIAL_FILES[1]]) == 1
        assert len(manifest.contents[INITIAL_FILES[2]]) == 1

    def test_delete_unknown(self, mock_time, manifest):
        old_contents = deepcopy(manifest.contents)
        with mock.patch('backuppy.manifest.logger') as mock_logger:
            manifest.delete('foo')
            assert mock_logger.warn.call_count == 1
            assert manifest.contents == old_contents


def test_tracked_files(manifest):
    assert manifest.tracked_files() == set(INITIAL_FILES)


def test_manifest_snapshot(manifest):
    overwrite_file(INITIAL_FILES[0], 'foo')
    overwrite_file(INITIAL_FILES[2], 'asdf')
    manifest.contents[INITIAL_FILES[0]].append([10, ManifestEntry(INITIAL_FILES[0])])
    manifest.contents[INITIAL_FILES[1]].append([12, None])
    manifest.contents[INITIAL_FILES[2]].append([20, ManifestEntry(INITIAL_FILES[2])])

    zeroth_snapshot = manifest.snapshot(-1)
    assert not zeroth_snapshot

    first_snapshot = manifest.snapshot(0)
    assert set(first_snapshot.keys()) == set(INITIAL_FILES)
    assert {entry.sha for entry in first_snapshot.values()} == {sha(name) for name in INITIAL_FILES}

    second_snapshot = manifest.snapshot(15)
    assert set(second_snapshot.keys()) == {INITIAL_FILES[0], INITIAL_FILES[2]}
    assert second_snapshot[INITIAL_FILES[0]].sha == sha('foo')
    assert second_snapshot[INITIAL_FILES[2]].sha == sha(INITIAL_FILES[2])

    third_snapshot = manifest.snapshot(25)
    assert set(second_snapshot.keys()) == {INITIAL_FILES[0], INITIAL_FILES[2]}
    assert third_snapshot[INITIAL_FILES[0]].sha == sha('foo')
    assert third_snapshot[INITIAL_FILES[2]].sha == sha('asdf')
