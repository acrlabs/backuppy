import math

import boto3
import colorlog

from backuppy.io import BLOCK_SIZE
from backuppy.io import IOIter
from backuppy.stores.backup_store import BackupStore

logger = colorlog.getLogger(__name__)
REGULAR_STORAGE_CLASSES = {"STANDARD", "INTELLIGENT_TIERING"}
IA_MIN_SIZE = 128 * 1024
GLACIER_MIN_SIZE = 40 * 1024

# prices taken from AWS to figure out when its more economical
# to use standard versus a different storage class
STANDARD_IA_SIZE = math.ceil(0.023 / 0.0125 * IA_MIN_SIZE)
ONEZONE_IA_SIZE = math.ceil(0.023 / 0.01 * IA_MIN_SIZE)
GLACIER_SIZE = math.ceil(0.023 / 0.004 * GLACIER_MIN_SIZE)
DEEP_ARCHIVE_SIZE = math.ceil(0.023 / 0.00099 * GLACIER_MIN_SIZE)

RESTORE_DAYS = 7
RESTORE_REQUEST_TIER = "Bulk"


class S3BackupStore(BackupStore):
    """Back up files to a local (on-disk) location"""

    def __init__(self, backup_name):
        super().__init__(backup_name)
        session = boto3.session.Session(
            aws_access_key_id=self.config.read_string("protocol.aws_access_key_id"),
            aws_secret_access_key=self.config.read_string("protocol.aws_secret_access_key"),
            region_name=self.config.read_string("protocol.aws_region"),
        )
        self._bucket = self.config.read_string("protocol.bucket")
        self._client = session.client("s3")

    def _save(self, src: IOIter, dest: str) -> None:
        assert src.filename  # can't have a tmpfile here
        dest = dest.replace("\\", "/")  # convert Windows separators to S3 separators
        if self._client.list_objects_v2(Bucket=self._bucket, Prefix=dest)["KeyCount"] > 0:
            logger.warning(
                f"{dest} already exists in {self._bucket}; overwriting with new data",
            )

        logger.info(f"Writing {src.filename} to s3://{self._bucket}/{dest}")
        # writing objects to S3 is guaranteed to be atomic, so no need for checks here
        src.fd.seek(0)
        self._client.upload_fileobj(
            src.fd,
            self._bucket,
            dest,
            ExtraArgs={"StorageClass": self._compute_object_storage_class(src)},
        )

    def _load(self, path: str, output_file: IOIter) -> IOIter | None:
        path = path.replace("\\", "/")

        s3_path = f"s3://{self._bucket}/{path}"
        logger.info(f"Reading {s3_path} into {output_file.filename}")

        head = self._client.head_object(Bucket=self._bucket, Key=path)
        storage_class = head.get("StorageClass", "STANDARD")
        restore_header = head.get("Restore")

        if storage_class != "STANDARD":
            if restore_header is None:
                logger.warning(f"{s3_path} is in {storage_class} and has not been restored, requesting now")
                self._client.restore_object(
                    Bucket=self._bucket,
                    Key=path,
                    RestoreRequest={
                        "Days": RESTORE_DAYS,
                        "GlacierJobParameters": {"Tier": RESTORE_REQUEST_TIER},
                    },
                )
                return None
            elif 'ongoing-request="true"' in restore_header:
                logger.warning(f"{s3_path} is in {storage_class}; restore is in progress, please try again later")
                return None

        response = self._client.get_object(Bucket=self._bucket, Key=path)
        writer = output_file.writer()
        next(writer)
        for data in response["Body"].iter_chunks(BLOCK_SIZE):
            writer.send(data)
        return output_file

    def _query(self, prefix: str) -> list[str]:
        results = []
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            results.extend([item["Key"] for item in page.get("Contents", [])])
        return results

    def _delete(self, filename: str) -> None:
        self._client.delete_object(Bucket=self._bucket, Key=filename)

    def _compute_object_storage_class(self, obj: IOIter) -> str:
        """For low-frequency access storage classes, AWS charges objects below a certain size
        as though they were larger; if the object is less than half of that minimum size, it's
        cheaper to store them in STANDARD"""
        # never stick the manifest in low-access storage
        assert obj.filename
        if "manifest" in obj.filename:
            return "STANDARD"

        storage_class = self.config.read_string("protocol.storage_class", default="STANDARD")
        if (
            storage_class in REGULAR_STORAGE_CLASSES
            or (storage_class == "STANDARD_IA" and obj.size >= STANDARD_IA_SIZE)
            or (storage_class == "ONEZONE_IA" and obj.size >= ONEZONE_IA_SIZE)
            or (storage_class == "GLACIER" and obj.size >= GLACIER_SIZE)
            or (storage_class == "DEEP_ARCHIVE" and obj.size >= DEEP_ARCHIVE_SIZE)
        ):
            return storage_class
        else:
            return "STANDARD"
