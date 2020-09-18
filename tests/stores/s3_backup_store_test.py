import boto3
import mock
import pytest
import staticconf
from moto import mock_s3

from backuppy.io import IOIter
from backuppy.stores.s3_backup_store import DEEP_ARCHIVE_SIZE
from backuppy.stores.s3_backup_store import GLACIER_SIZE
from backuppy.stores.s3_backup_store import ONEZONE_IA_SIZE
from backuppy.stores.s3_backup_store import S3BackupStore
from backuppy.stores.s3_backup_store import STANDARD_IA_SIZE


@pytest.fixture
def s3_client():
    mock_s3_obj = mock_s3()
    mock_s3_obj.start()
    client = boto3.client('s3')
    client.create_bucket(Bucket='test_bucket')
    client.put_object(Bucket='test_bucket', Key='/foo', Body='old boring content')
    client.put_object(Bucket='test_bucket', Key='/biz/baz', Body='old boring content 2')
    client.put_object(Bucket='test_bucket', Key='/fuzz/buzz', Body='old boring content 3')
    yield client
    mock_s3_obj.stop()


@pytest.fixture
def mock_backup_store():
    backup_name = 'fake_backup'
    with mock.patch('backuppy.stores.s3_backup_store.BackupStore'), \
            staticconf.testing.PatchConfiguration({
                'protocol': {
                    'type': 's3',
                    'aws_access_key_id': 'ACCESS_KEY',
                    'aws_secret_access_key': 'SECRET_ACCESS_KEY',
                    'aws_region': 'us-west-2',
                    'bucket': 'test_bucket',
                }
            },
            namespace=backup_name,
    ):
        yield S3BackupStore(backup_name)


def test_save(s3_client, mock_backup_store):
    with IOIter('/scratch/foo') as input1, IOIter('/scratch/asdf/bar') as input2:
        mock_backup_store._save(input1, '/foo')
        mock_backup_store._save(input2, '/asdf/bar')
    assert s3_client.get_object(
        Bucket='test_bucket',
        Key='/foo'
    )['Body'].read() == b"i'm a copy of foo"
    assert s3_client.get_object(
        Bucket='test_bucket',
        Key='/asdf/bar'
    )['Body'].read() == b"i'm a copy of bar"


def test_load(s3_client, mock_backup_store):
    with IOIter('/restored_file') as output:
        mock_backup_store._load('/foo', output)

    with open('/restored_file') as f:
        assert f.read() == 'old boring content'


def test_query(s3_client, mock_backup_store):
    assert set(mock_backup_store._query('')) == {'/foo', '/biz/baz', '/fuzz/buzz'}


def test_query_2(s3_client, mock_backup_store):
    assert set(mock_backup_store._query('/f')) == {'/foo', '/fuzz/buzz'}


def test_query_no_results(s3_client, mock_backup_store):
    assert mock_backup_store._query('not_here') == []


def test_delete(s3_client, mock_backup_store):
    mock_backup_store._delete('/biz/baz')
    with pytest.raises(Exception) as e:
        s3_client.get_object(
            Bucket='test_bucket',
            Key='/biz/baz',
        )
        assert e.__class__ == 'NoSuchKey'


@pytest.mark.parametrize('sc', ['STANDARD', 'INTELLIGENT_TIERING'])
def test_compute_object_storage_class_size_1(mock_backup_store, sc):
    with staticconf.testing.PatchConfiguration(
            {'protocol': {'storage_class': sc}},
            namespace='fake_backup'
    ):
        assert mock_backup_store._compute_object_storage_class(mock.Mock(filename='foo')) == sc


@pytest.mark.parametrize('size', [STANDARD_IA_SIZE, STANDARD_IA_SIZE - 1])
def test_compute_object_storage_class_size_2(mock_backup_store, size):
    with staticconf.testing.PatchConfiguration(
            {'protocol': {'storage_class': 'STANDARD_IA'}},
            namespace='fake_backup'
    ):
        assert mock_backup_store._compute_object_storage_class(
            mock.Mock(filename='foo', size=size)
        ) == ('STANDARD_IA' if size == STANDARD_IA_SIZE else 'STANDARD')


@pytest.mark.parametrize('size', [ONEZONE_IA_SIZE, ONEZONE_IA_SIZE - 1])
def test_compute_object_storage_class_size_3(mock_backup_store, size):
    with staticconf.testing.PatchConfiguration(
            {'protocol': {'storage_class': 'ONEZONE_IA'}},
            namespace='fake_backup'
    ):
        assert mock_backup_store._compute_object_storage_class(
            mock.Mock(filename='foo', size=size)
        ) == ('ONEZONE_IA' if size == ONEZONE_IA_SIZE else 'STANDARD')


@pytest.mark.parametrize('size', [GLACIER_SIZE, GLACIER_SIZE - 1])
def test_compute_object_storage_class_size_4(mock_backup_store, size):
    with staticconf.testing.PatchConfiguration(
            {'protocol': {'storage_class': 'GLACIER'}},
            namespace='fake_backup'
    ):
        assert mock_backup_store._compute_object_storage_class(
            mock.Mock(filename='foo', size=size)
        ) == ('GLACIER' if size == GLACIER_SIZE else 'STANDARD')


@pytest.mark.parametrize('size', [DEEP_ARCHIVE_SIZE, DEEP_ARCHIVE_SIZE - 1])
def test_compute_object_storage_class_size_5(mock_backup_store, size):
    with staticconf.testing.PatchConfiguration(
            {'protocol': {'storage_class': 'DEEP_ARCHIVE'}},
            namespace='fake_backup'
    ):
        assert mock_backup_store._compute_object_storage_class(
            mock.Mock(filename='foo', size=size)
        ) == ('DEEP_ARCHIVE' if size == DEEP_ARCHIVE_SIZE else 'STANDARD')


def test_compute_object_storage_manifest(mock_backup_store):
    with staticconf.testing.PatchConfiguration(
            {'protocol': {'storage_class': 'DEEP_ARCHIVE'}},
            namespace='fake_backup'
    ):
        assert mock_backup_store._compute_object_storage_class(
            mock.Mock(filename='/tmp/foo/bar/manifest.12345566', size=100000000000)
        ) == 'STANDARD'
        assert mock_backup_store._compute_object_storage_class(
            mock.Mock(filename='/tmp/foo/baz/manifest-key.12345566', size=100000000000)
        ) == 'STANDARD'
