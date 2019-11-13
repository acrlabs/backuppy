import re

from backuppy.util import file_walker


# def test_ask_for_confirmation():


def test_file_walker(fs):
    fs.create_file('/foo')
    fs.create_file('/bar')
    fs.create_file('/skip/baz')
    fs.create_file('/skip/dip')
    fs.create_file('/fizz/buzz')
    fs.create_file('/fizz/skip2')
    results = {f for f in file_walker('/', exclusions=[re.compile('skip')])}
    assert results == {'/foo', '/bar', '/fizz/buzz'}
