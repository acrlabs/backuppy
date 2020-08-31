import boto3
import mock
import pytest
import staticconf
from moto import mock_s3

from backuppy.io import IOIter
from backuppy.stores.s3_backup_store import S3BackupStore


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
