from itertools import zip_longest
from typing import Optional

import bsdiff4
import colorlog

from backuppy.exceptions import DiffParseError
from backuppy.exceptions import DiffTooLargeException
from backuppy.io import IOIter

# This file will read and write diff file for backuppy.

# The expected diff format is '(<len>\|<byte-stream>)*'
#
# The meaning of these tokens is:
#  - len: the number of bytes in the diff
#  - byte-stream: a bytes diff returned by bsdiff4


logger = colorlog.getLogger(__name__)
SEPARATOR = b'|'


def apply_diff(orig_file: IOIter, diff_file: IOIter, new_file: IOIter) -> None:
    """ Given an original file and a diff file, write out a new file with the diff applied

    :param orig_file: an IOIter object whose contents are the "original" data
    :param diff_file: an IOIter object whose contents are the diff to be applied
    :param new_file: an IOIter object where the new file data will be written
    """

    # The outer loop reads a chunk of data at a time; the inner loop parses
    # the read chunk one step at a time and applies it
    diff = b''
    new_writer = new_file.writer(); next(new_writer)
    orig_reader = orig_file.reader()
    logger.debug2('applying diff')
    for diff_chunk in diff_file.reader():
        diff += diff_chunk
        while diff:
            # try to parse the next chunk; if we can't, break out of the loop to get more data
            try:
                diff_len_str, remainder = diff.split(SEPARATOR, 1)
            except ValueError:
                break

            diff_len = int(diff_len_str)
            if len(remainder) < diff_len:
                break

            try:
                orig_block = next(orig_reader)
            except StopIteration:
                orig_block = b''
            new_writer.send(bsdiff4.patch(orig_block, remainder[:diff_len]))
            diff = remainder[diff_len:]

    if diff:
        raise DiffParseError(f'Un-parseable diff: {diff}')


def compute_diff(
    orig_file: IOIter,
    new_file: IOIter,
    diff_file: IOIter,
    discard_diff_percentage: Optional[float] = None,
) -> IOIter:
    """ Given an open original file and a new file, compute the diff between the two

    :param orig_file: an IOIter object whose contents are the "original" data
    :param new_file: an IOIter object whose contents are the "new" data
    :param diff_file: an IOIter object where the diff data will be written
    """

    total_written = 0

    writer = diff_file.writer(); next(writer)
    logger.debug2('beginning diff computation')
    for orig_bytes, new_bytes in zip_longest(orig_file.reader(), new_file.reader(), fillvalue=b''):
        diff = bsdiff4.diff(orig_bytes, new_bytes)
        diff_str = str(len(diff)).encode() + SEPARATOR + diff
        total_written += len(diff_str)
        if discard_diff_percentage and total_written > orig_file.size * discard_diff_percentage:
            raise DiffTooLargeException
        writer.send(diff_str)

    return diff_file
