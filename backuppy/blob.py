import re

import edlib

from backuppy.exceptions import DiffParseError

DEL = b'-'
INS = b'+'
REPL = b'!'
SEP = b'|'


def _find_sep_index(diff, pos):
    try:
        return diff[pos:].index(SEP) + pos
    except ValueError as e:
        raise DiffParseError('No separator found after {pos}') from e


def apply_diff(diff, old_contents):
    pos = 0
    new_contents = bytearray(old_contents)
    while pos < len(diff):
        if diff[pos] != ord('@'):
            raise DiffParseError(f'Expected b\'@\' at {pos}, found {chr(diff[pos])}')

        pos += 1
        sep_ind = _find_sep_index(diff, pos)
        contents_pos = int(diff[pos:sep_ind])

        pos = sep_ind + 1
        action = diff[pos:pos+1]

        pos += 1
        sep_ind = _find_sep_index(diff, pos)
        contents_len = int(diff[pos:sep_ind])

        pos = sep_ind + 1
        contents = diff[pos:pos+contents_len]
        pos += contents_len + 1

        if action == DEL:
            pos -= contents_len
            del new_contents[contents_pos:contents_pos+contents_len]
        elif len(contents) != contents_len:
            raise DiffParseError(f'Contents length did not match expected ({len(contents)} != {contents_len})')
        elif action == INS:
            new_contents[contents_pos:contents_pos] = contents
        elif action == REPL:
            new_contents[contents_pos:contents_pos+contents_len] = contents
        else:
            raise DiffParseError(f'Expected an action, found {chr(action[0])}')

    return new_contents


def compute_diff(old_contents, new_contents):
    # Reverse the order of new_contents and old_contents since edlib outputs the cigar w.r.t. the 2nd arg
    result = edlib.align(new_contents, old_contents, task='path')
    steps = [(int(n), c) for n, c in re.findall('(\d+)([=DIX])', result['cigar'])]

    pos = 0
    diff = b''
    for num, case in steps:
        pos += num
        if case == '=':
            continue

        contents = new_contents[pos-num:pos]
        diff += f'@{pos-num}'.encode('utf-8') + SEP
        if case == 'D':
            diff += DEL
            contents = b''
            pos -= num
        elif case == 'I':
            diff += INS
        elif case == 'X':
            diff += REPL
        diff += f'{num}'.encode('utf-8') + SEP + contents + b'\n'
    return diff
