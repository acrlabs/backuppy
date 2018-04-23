import re

import edlib

from backuppy.exceptions import DiffParseError

DEL = b'-'
ADD = b'+'
REPLACE = b'!'
SEP = b'|'


def _write_diff_from_cigar(cigar, new_contents, old_contents, diff_file):
    pos = 0
    steps = [(int(n), c) for n, c in re.findall('(\d+)([=DIX])', cigar)]

    with open(diff_file, 'wb') as diff:
        for num, case in steps:
            if case == '=':
                pos += num
                continue

            contents = new_contents[pos:pos+num]
            diff.write(f'@{pos}'.encode('utf-8') + SEP)
            if case == 'D':
                diff.write(DEL)
                contents = b''
                pos -= num
            elif case == 'I':
                diff.write(ADD)
            elif case == 'X':
                diff.write(REPLACE)
            diff.write(f'{num}'.encode('utf-8') + SEP)
            diff.write(contents)
            diff.write(b'\n')
            pos += num


def apply_diff(diff_file, old_file, new_file):
    with open(old_file, 'rb') as f:
        old_contents = f.read()
    new_contents = bytearray(old_contents)
    with open(diff_file, 'rb') as f:
        diff = f.read()

    pos = 0
    while pos < len(diff):
        if diff[pos] != ord('@'):
            raise DiffParseError(f'Malformed diff; expected b\'@\', found {chr(diff[pos])}')

        pos += 1
        sep_ind = diff[pos:].index(SEP) + pos
        contents_pos = int(diff[pos:sep_ind])

        pos = sep_ind + 1
        action = diff[pos:pos+1]

        pos += 1
        sep_ind = diff[pos:].index(SEP) + pos
        contents_len = int(diff[pos:sep_ind])

        pos = sep_ind + 1
        contents = diff[pos:pos+contents_len]
        pos += contents_len + 1

        if action == DEL:
            pos -= contents_len
            del new_contents[contents_pos:contents_pos+contents_len]
        elif action == ADD:
            new_contents[contents_pos:contents_pos] = contents
        elif action == REPLACE:
            new_contents[contents_pos:contents_pos+contents_len] = contents
        else:
            raise DiffParseError(f'Malformed diff; expected an action, found {chr(action[0])}')

    with open(new_file, 'wb') as f:
        f.write(new_contents)


def compute_diff(new_file, old_file, diff_file):
    with open(new_file, 'rb') as new:
        with open(old_file, 'rb') as old:
            new_contents = new.read()
            old_contents = old.read()
            result = edlib.align(new_contents, old_contents, task='path')
            _write_diff_from_cigar(result['cigar'], new_contents, old_contents, diff_file)
