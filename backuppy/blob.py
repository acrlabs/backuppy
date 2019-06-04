import re
from itertools import zip_longest
from typing import Tuple

import edlib

from backuppy.exceptions import DiffParseError
from backuppy.io import IOIter

DEL = b'D'
INS = b'I'
REPL = b'X'
SEP = b'|'


def apply_diff(orig_file: IOIter, diff_file: IOIter, new_file: IOIter) -> None:
    """ Given an open original file and a diff, write out the new file

    :param orig_file: an open IO stream in 'rb' mode
    :param diff_file: an open IO stream in 'rb' mode
    :param new_file: an open IO stream in 'wb' mode
    """

    # The outer loop reads a chunk of data at a time; the inner loop parses
    # the read chunk one step at a time and applies it
    diff, offset = b'', 0
    writer = new_file.writer(); next(writer)
    for diff_chunk in diff_file.reader():
        diff += diff_chunk
        while diff:
            # try to parse the next chunk; if we can't, break out of the loop to get more data
            try:
                position, action_len, remainder = diff.split(SEP, 2)
            except ValueError:
                break

            contents_pos = int(position[1:])
            for data in orig_file.reader(end=contents_pos - offset, reset_pos=False):
                writer.send(data)
            action = action_len[0:1]
            contents_len = int(action_len[1:])

            # If the remainder of the chunk doesn't have enough bytes and we need to insert
            # or replace data, get the next chunk first so we have all the needed data
            if len(remainder) < contents_len and action != DEL:
                break
            contents = b'' if action == DEL else remainder[:contents_len]
            diff = remainder if action == DEL else remainder[contents_len:]
            if action == DEL:
                orig_file.fd.seek(contents_len, 1)
                offset -= contents_len
            elif action == INS:
                writer.send(contents)
                offset += contents_len
            elif action == REPL:
                writer.send(contents)
                orig_file.fd.seek(contents_len, 1)
            else:
                raise DiffParseError(f'Expected an action, found {action}')

    # Use slices to get bytes objects instead of integers
    if diff:
        raise DiffParseError(f'Un-parseable diff: {diff}')

    # If we get here and there's still data in the original file, it must be equal to
    # what was in the new file, so just copy any remaining data from the original file to the new file
    for data in orig_file.reader(end=orig_file.stat().st_size, reset_pos=False):
        writer.send(data)


def compute_sha_and_diff(orig_file: IOIter, new_file: IOIter, diff_file: IOIter) -> Tuple[str, IOIter]:
    """ Given an open original file and a new file, compute the diff

    :param orig_file: an open IO stream in 'rb' mode
    :param new_file: an open IO stream in 'rb' mode
    :param diff_file: an open IO stream in 'wb' mode
    """

    pos = 0
    writer = diff_file.writer(); next(writer)
    for orig_bytes, new_bytes in zip_longest(orig_file.reader(), new_file.reader()):
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
            diff += f'{num}'.encode('utf-8') + SEP + contents
        writer.send(diff)
        pos += orig_file.block_size

    return new_file.sha(), diff_file
