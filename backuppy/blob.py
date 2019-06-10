import re
from itertools import zip_longest
from typing import Tuple

import edlib

from backuppy.exceptions import DiffParseError
from backuppy.io import IOIter

# This file will read and write diff file for backuppy.

# The expected diff format is '(@<pos>|<action><contents-length>|<optional-contents>)*'
#
# The meaning of these tokens is:
#  - pos: the location in the original file to which the diff should be applied (given any previous
#      diffs that have already been applied).
#  - action: one of 'D' (delete), 'I' (insert), 'X' (replace)
#  - contents-length: how many bytes of data to delete, insert, or replace
#  - optional-contents: if the action was insert or replace, the data to insert or replace; this
#      field is omitted (but the preceding separator | is not) for delete actions
#
# For example, if the original file is 'asdf' and the new file is 'adz foobar' the diff would be
# be '@1|D1|@2|I2|z @5|I5|oobar'.  Note that the position of the second chunk (2) is _after_ the
# first diff chunk has been applied, and similarly for the third diff chunk.


class Token:
    DEL = b'D'
    INS = b'I'
    REPL = b'X'
    SEP = b'|'


def apply_diff(orig_file: IOIter, diff_file: IOIter, new_file: IOIter) -> None:
    """ Given an original file and a diff file, write out a new file with the diff applied

    :param orig_file: an IOIter object whose contents are the "original" data
    :param diff_file: an IOIter object whose contents are the diff to be applied
    :param new_file: an IOIter object where the new file data will be written
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
                position, action_len, remainder = diff.split(Token.SEP, 2)
            except ValueError:
                break

            # Use slices to get bytes objects instead of integers
            contents_pos = int(position[1:])  # skip the initial '@'
            action = action_len[0:1]
            contents_len = int(action_len[1:])

            # scan in the original file until we get to the start position for this diff
            for data in orig_file.reader(end=contents_pos - offset, reset_pos=False):
                writer.send(data)

            # If the remainder of the chunk doesn't have enough bytes and we need to insert
            # or replace data, get the next chunk first so we have all the needed data
            if len(remainder) < contents_len and action != Token.DEL:
                break
            contents = b'' if action == Token.DEL else remainder[:contents_len]
            diff = remainder if action == Token.DEL else remainder[contents_len:]
            if action == Token.DEL:
                orig_file.fd.seek(contents_len, 1)  # seek from the current position
                offset -= contents_len  # adjust the orig_file position based on the amount deleted
            elif action == Token.INS:
                writer.send(contents)
                offset += contents_len  # adjust the orig_file position based on the amount inserted
            elif action == Token.REPL:
                writer.send(contents)
                orig_file.fd.seek(contents_len, 1)  # seek from the current position
            else:
                raise DiffParseError(f'Expected an action, found {action}')

    if diff:
        raise DiffParseError(f'Un-parseable diff: {diff}')

    # If we get here and there's still data in the original file, it must be equal to
    # what was in the new file, so just copy any remaining data from the original file to the new file
    for data in orig_file.reader(end=orig_file.stat().st_size, reset_pos=False):
        writer.send(data)


def compute_sha_and_diff(orig_file: IOIter, new_file: IOIter, diff_file: IOIter) -> Tuple[str, IOIter]:
    """ Given an open original file and a new file, compute the diff between the two

    :param orig_file: an IOIter object whose contents are the "original" data
    :param new_file: an IOIter object whose contents are the "new" data
    :param diff_file: an IOIter object where the diff data will be written
    """

    pos = 0
    writer = diff_file.writer(); next(writer)
    for orig_bytes, new_bytes in zip_longest(orig_file.reader(), new_file.reader()):
        if not orig_bytes:
            steps = [(len(new_bytes), Token.INS)]
        elif not new_bytes:
            steps = [(len(orig_bytes), Token.DEL)]
        else:
            # Reverse the order of new_contents and old_contents since edlib outputs the cigar w.r.t. the 2nd arg
            result = edlib.align(new_bytes, orig_bytes, task='path')
            steps = [(int(n), c.encode('utf-8')) for n, c in re.findall(r'(\d+)([=DIX])', result['cigar'])]

        local_pos, diff = 0, b''
        for num, action in steps:
            local_pos += num
            if action == b'=':
                continue

            diff += f'@{pos+local_pos-num}'.encode('utf-8') + Token.SEP + action
            if action == Token.DEL:  # if new_bytes is None we're guaranteed to hit this case
                contents = b''
                local_pos -= num
            else:  # can only hit this case if new_bytes is not None
                contents = new_bytes[local_pos - num:local_pos]
            diff += f'{num}'.encode('utf-8') + Token.SEP + contents
        writer.send(diff)
        pos += orig_file.block_size

    return new_file.sha(), diff_file
