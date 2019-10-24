import random
import string

import pytest

from backuppy.blob import apply_diff
from backuppy.blob import compute_diff
from backuppy.io import IOIter

DATA_LEN = 100


def generate_data():
    orig_data = ''.join(random.choices(string.ascii_lowercase, k=DATA_LEN))
    new_data, pos, i = '', -1, 0
    while pos < DATA_LEN:
        i, l = [random.randint(1, DATA_LEN // 10) for _ in range(2)]
        new_data += orig_data[pos:pos+i]
        action = random.choice(['D', 'I', 'X'])

        if action == 'D':
            pos += l
        elif action == 'I':
            new_data += ''.join(random.choices(string.ascii_lowercase, k=l))
        elif action == 'X':
            pos += l
            new_data += ''.join(random.choices(string.ascii_lowercase, k=l))
        pos += i
    return orig_data.encode(), new_data.encode()


@pytest.mark.parametrize('orig_data,new_data', [
    (b'yvglcwpherjilzaivozswxogbicmycqlirmlohuggjcilznfbqemryzausgdvnmtvseatahalpgznorldtrkkgybvuxcianqzclt',  # noqa
        b'ferjilzaiicmljghprmlsogvrzbqemryzaudvnmtvseatxasfbvaeahalpgznorqldtrkkgybqxlcian'),
    ] + [(None, None)] * 10  # type: ignore
)
def test_validate_diffs(orig_data, new_data):
    if not orig_data:
        orig_data, new_data = generate_data()

    print(orig_data)
    print(new_data)

    with IOIter() as orig, IOIter() as new, IOIter() as diff, IOIter() as newnew:
        orig_writer = orig.writer(); next(orig_writer)
        orig_writer.send(orig_data)

        new_writer = new.writer(); next(new_writer)
        new_writer.send(new_data)
        compute_diff(orig, new, diff)
        apply_diff(orig, diff, newnew)

        new.fd.seek(0)
        newnew.fd.seek(0)
        assert new.fd.read() == newnew.fd.read()
