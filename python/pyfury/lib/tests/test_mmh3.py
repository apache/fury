from pyfury.lib import mmh3


def test_mmh3():
    assert mmh3.hash_buffer(bytearray([1, 2, 3]), seed=47)[0] == -7373655978913577904
