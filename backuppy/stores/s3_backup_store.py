from typing import List

import boto3
import colorlog

from backuppy.io import BLOCK_SIZE
from backuppy.io import IOIter
from backuppy.stores.backup_store import BackupStore


logger = colorlog.getLogger(__name__)


class S3BackupStore(BackupStore):
    """ Back up files to a local (on-disk) location """

    def __init__(self, backup_name):
        super().__init__(backup_name)
        session = boto3.session.Session(
            aws_access_key_id=self.config.read_string('protocol.awsAccessKeyId'),
            aws_secret_access_key=self.config.read_string('protocol.awsSecretAccessKey'),
            region_name=self.config.read_string('protocol.awsRegion'),
        )
        self._bucket = self.config.read_string('protocol.bucket')
        self._client = session.client('s3')

    def _save(self, src: IOIter, dest: str) -> None:
        assert src.filename  # can't have a tmpfile here
        if self._client.list_objects_v2(Bucket=self._bucket, Prefix=dest)['KeyCount'] > 0:
            logger.warning(
                f'{src.filename} already exists in {self._bucket}; overwriting with new data',
            )

        logger.info(f'Writing {src.filename} to {self._bucket}')
        # writing objects to S3 is guaranteed to be atomic, so no need for checks here
        src.fd.seek(0)
        self._client.put_object(Bucket=self._bucket, Body=src.fd, Key=dest)

    def _load(self, path: str, output_file: IOIter) -> IOIter:
        logger.info(f'Reading {path} from {self._bucket}')
        response = self._client.get_object(Bucket=self._bucket, Key=path)
        writer = output_file.writer(); next(writer)
        for data in response['Body'].iter_chunks(BLOCK_SIZE):
            writer.send(data)
        return output_file

    def _query(self, prefix: str) -> List[str]:
        results = []
        paginator = self._client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            results.extend([item['Key'] for item in page.get('Contents', [])])
        return results

    def _delete(self, filename: str) -> None:
        self._client.delete_object(Bucket=self._bucket, Key=filename)
