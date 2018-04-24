import os
from hashlib import sha256
from tempfile import NamedTemporaryFile

import staticconf
import yaml

from backuppy.crypto import compress_and_encrypt
from backuppy.crypto import decrypt_and_unpack
from backuppy.exceptions import BackupFailedError
from backuppy.manifest import Manifest
from backuppy.manifest import ManifestEntry
from backuppy.util import compile_exclusions
from backuppy.util import file_contents_stream
from backuppy.util import file_walker
from backuppy.util import get_color_logger

logger = get_color_logger(__name__)

MANIFEST_PATH = 'manifest'


def _get_manifest(backup_store):
    try:
        with open(backup_store.read(MANIFEST_PATH), 'rb') as f:
            return yaml.load(decrypt_and_unpack(f.read()))
    except FileNotFoundError:
        return Manifest()


def _backup_data_stream(data_stream, backup_store, stored_path=None):
    hash_function = sha256()
    with NamedTemporaryFile(suffix='.tbkpy', delete=False) as f:
        for chunk in data_stream:
            hash_function.update(chunk)
            f.write(compress_and_encrypt(chunk))
        tmpfile = f.name
    sha = hash_function.hexdigest()
    stored_path = stored_path or (sha[:2], sha[2:4], sha[4:])
    backup_store.write(stored_path, tmpfile)

    if os.path.isfile(tmpfile):
        os.remove(tmpfile)

    return sha


def backup(backup_name, backup_store, global_exclusions=None):
    global_exclusions = global_exclusions or []
    manifest = _get_manifest(backup_store)
    marked_files, manifest_files = set(), manifest.tracked_files()
    for base_path in staticconf.read_list('directories', namespace=backup_name):
        abs_base_path = os.path.abspath(base_path)
        local_exclusions = compile_exclusions(staticconf.read_list('exclusions', [], namespace=backup_name))
        exclusions = global_exclusions + local_exclusions

        for abs_file_name in file_walker(abs_base_path, logger.warn):

            # Skip files that match any of the specified regular expressions
            matched_patterns = [pattern for pattern in exclusions if pattern.search(abs_file_name)]
            if matched_patterns:
                logger.info(f'{abs_file_name} matched exclusion pattern(s) "{matched_patterns}"; skipping')
                continue

            # Mark the file as "seen" so it isn't deleted later
            marked_files.add(abs_file_name)

            if manifest.is_current(abs_file_name):
                logger.info(f'{abs_file_name} is up-to-date; skipping')
                continue

            try:
                sha = _backup_data_stream(file_contents_stream(abs_file_name), backup_store)
            except BackupFailedError as e:
                logger.warn(f'There was a problem backing up {abs_file_name}: {str(e)}; skipping')
                continue

            manifest.insert_or_update(abs_file_name, ManifestEntry(abs_file_name, sha))
            logger.info(f'Backed up {abs_file_name}')

    # Mark all files that weren't touched in the above loop as "deleted"
    # (we don't actually delete the file, just record that it's no longer present)
    for name in manifest_files - marked_files:
        logger.info(f'{abs_file_name} has been deleted')
        manifest.delete(name)

    _backup_data_stream(manifest.stream(), backup_store, stored_path=MANIFEST_PATH)
