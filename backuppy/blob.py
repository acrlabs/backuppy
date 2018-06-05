import os
import re

import edlib

from backuppy.exceptions import DiffParseError
from backuppy.io import BLOCK_SIZE

DEL = b'D'
INS = b'I'
REPL = b'X'
SEP = b'|'


def _copy(fd_orig, fd_new, to_pos=None, offset=0):
    to_pos = to_pos or os.stat(fd_orig.fileno()).st_size
    orig_bytes = b''
    while True:
        orig_pos = fd_orig.tell() + offset
        if orig_pos >= to_pos:
            break
        requested_read_size = min(BLOCK_SIZE, to_pos - orig_pos)
        orig_bytes += fd_orig.read(requested_read_size)
        if not orig_bytes:
            raise DiffParseError('No more data in source file')
        fd_new.write(orig_bytes)


def apply_diff(fd_orig, fd_diff, fd_new):
    diff, offset = b'', 0
    while True:
        diff += fd_diff.read(BLOCK_SIZE)
        if not diff:
            break
        while diff:
            try:
                position, action_len, remainder = diff.split(SEP, 2)
            except ValueError:
                pass

            if position[0:1] != b'@':
                raise DiffParseError(f'Expected b\'@\' in {position}')

            contents_pos = int(position[1:])
            _copy(fd_orig, fd_new, contents_pos, offset)
            action = action_len[0:1]
            contents_len = int(action_len[1:])
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

    _copy(fd_orig, fd_new)
    fd_new.seek(0)


def compute_diff(fd_orig, fd_new, fd_diff):
    pos = 0
    while True:
        orig_bytes, new_bytes = fd_orig.read(BLOCK_SIZE), fd_new.read(BLOCK_SIZE)
        if not orig_bytes and not new_bytes:
            break
        elif not orig_bytes:
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

            contents = new_bytes[local_pos - num:local_pos]
            diff += f'@{pos+local_pos-num}'.encode('utf-8') + SEP + action
            if action == DEL:
                contents = b''
                local_pos -= num
            diff += f'{num}'.encode('utf-8') + SEP + contents + b'\n'
        fd_diff.write(diff)
        pos += BLOCK_SIZE
    fd_diff.seek(0)
