import os
import re
from hashlib import sha256
from itertools import zip_longest
from typing import IO
from typing import Optional

import edlib

from backuppy.exceptions import DiffParseError
from backuppy.io import BLOCK_SIZE
from backuppy.io import IOIter

DEL = b'D'
INS = b'I'
REPL = b'X'
SEP = b'|'


def _copy(fd_orig: IO[bytes], fd_new: IO[bytes], to_pos: Optional[int] = None, offset: int = 0) -> None:
    """ copy data from fd_orig to fd_new up to to_pos - offset

    :param fd_orig: an open IO stream in 'rb' mode
    :param fd_new: an open IO stream in 'wb' mode
    :param to_pos: the seek position in fd_orig to copy to, or None to copy the rest of the file
    :param offset: how much to offset the copy position by
    """
    to_pos = to_pos - offset if to_pos else os.stat(fd_orig.fileno()).st_size
    orig_bytes = b''
    while True:
        orig_pos = fd_orig.tell()
        if orig_pos >= to_pos:
            break
        requested_read_size = min(BLOCK_SIZE, to_pos - orig_pos)
        orig_bytes = fd_orig.read(requested_read_size)
        if not orig_bytes:
            raise DiffParseError('No more data in source file')
        fd_new.write(orig_bytes)


def apply_diff(fd_orig: IO[bytes], fd_diff: IO[bytes], fd_new: IO[bytes]) -> None:
    """ Given an open original file and a diff, write out the new file

    :param fd_orig: an open IO stream in 'rb' mode
    :param fd_diff: an open IO stream in 'rb' mode
    :param fd_new: an open IO stream in 'wb' mode
    """

    # Make sure we're at the beginning
    fd_orig.seek(0)
    fd_diff.seek(0)
    fd_new.seek(0)

    # The outer loop reads a chunk of data at a time; the inner loop parses
    # the read chunk one step at a time and applies it
    diff, offset = b'', 0
    for data in IOIter(fd_diff):
        diff += data
        while diff:
            # try to parse the next chunk; if we can't, break out of the loop to get more data
            try:
                position, action_len, remainder = diff.split(SEP, 2)
            except ValueError:
                break

            contents_pos = int(position[1:])
            _copy(fd_orig, fd_new, contents_pos, offset)
            action = action_len[0:1]
            contents_len = int(action_len[1:])

            # If the remainder of the chunk doesn't have enough bytes and we need to insert
            # or replace data, get the next chunk first so we have all the needed data
            if len(remainder) < contents_len and action != DEL:
                break
            contents = b'' if action == DEL else remainder[:contents_len]
            diff = remainder if action == DEL else remainder[contents_len + 1:]
            if action == DEL:
                fd_orig.seek(contents_len, 1)
            elif action == INS:
                fd_new.write(contents)
                offset += len(contents)
            elif action == REPL:
                fd_new.write(contents)
                fd_orig.seek(contents_len, 1)
            else:
                raise DiffParseError(f'Expected an action, found {action}')

    # Use slices to get bytes objects instead of integers
    if diff:
        raise DiffParseError(f'Un-parseable diff: {diff}')

    # If we get here and there's still data in the original file, it must be equal to
    # what was in the new file, so just copy any remaining data from the original file to the new file
    _copy(fd_orig, fd_new, offset=offset)


def compute_diff(fd_orig: IO[bytes], fd_new: IO[bytes], fd_diff: IO[bytes]) -> str:
    """ Given an open original file and a new file, compute the diff

    :param fd_orig: an open IO stream in 'rb' mode
    :param fd_new: an open IO stream in 'rb' mode
    :param fd_diff: an open IO stream in 'wb' mode
    """

    # Make sure we're at the beginning
    fd_diff.seek(0)

    sha_fn, pos = sha256(), 0
    for orig_bytes, new_bytes in zip_longest(IOIter(fd_orig), IOIter(fd_new, side_effects=[sha_fn.update])):
        if not orig_bytes:
            steps = [(len(new_bytes), INS)]
        elif not new_bytes:
            steps = [(len(orig_bytes), DEL)]
        else:
            # Reverse the order of new_contents and old_contents since edlib outputs the cigar w.r.t. the 2nd arg
            result = edlib.align(new_bytes, orig_bytes, task='path')
            steps = [(int(n), c.encode('utf-8')) for n, c in re.findall(r'(\d+)([=DIX])', result['cigar'])]

        local_pos, diff = 0, b''
        for num, action in steps:
            local_pos += num
            if action == b'=':
                continue

            diff += f'@{pos+local_pos-num}'.encode('utf-8') + SEP + action
            if action == DEL:  # if new_bytes is None we're guaranteed to hit this case
                contents = b''
                local_pos -= num
            else:  # can only hit this case if new_bytes is not None
                contents = new_bytes[local_pos - num:local_pos]
            diff += f'{num}'.encode('utf-8') + SEP + contents + b'\n'
        fd_diff.write(diff)
        pos += BLOCK_SIZE

    return sha_fn.hexdigest()
