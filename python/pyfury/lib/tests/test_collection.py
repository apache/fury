from pyfury.lib.collection import WeakIdentityKeyDictionary


def test_weak_identity_key_dict():
    d = WeakIdentityKeyDictionary()

    class A:
        def __hash__(self):
            raise Exception

    a = A()
    d[a] = 1
    assert d[a] == 1
    del a
    assert len(d) == 0
