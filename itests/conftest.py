import os
from hashlib import sha256
from shutil import rmtree

import pytest

from backuppy.run import setup_logging
from backuppy.stores.backup_store import MANIFEST_PATH

ITEST_ROOT = 'itests'
DATA_DIR = os.path.join(ITEST_ROOT, 'data')
BACKUP_DIR = os.path.join(ITEST_ROOT, 'backup')
ITEST_MANIFEST_PATH = os.path.join(BACKUP_DIR, MANIFEST_PATH)


def compute_sha(string):
    sha_fn = sha256()
    sha_fn.update(string)
    return sha_fn.hexdigest()


@pytest.fixture(autouse=True, scope='module')
def initialize():
    try:
        rmtree(DATA_DIR)
    except FileNotFoundError:
        pass
    try:
        rmtree(BACKUP_DIR)
    except FileNotFoundError:
        pass

    os.makedirs(DATA_DIR)
    os.makedirs(BACKUP_DIR)
    setup_logging('debug')
