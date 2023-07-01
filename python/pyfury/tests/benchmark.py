import timeit
import pickle
import pytest
import pyfury as fury
from pyfury.tests.record import create_foo, foo_schema

iter_nums = 100000


@pytest.mark.skip(reason="take too long")
def test_encode():
    # print("schema", foo_schema())
    encoder = fury.create_row_encoder(foo_schema())
    foo = create_foo()
    row = encoder.to_row(foo)
    assert foo == encoder.from_row(row)

    t1 = timeit.timeit(lambda: encoder.to_row(foo), number=iter_nums)
    print(
        "encoder take {0} for {1} times, avg: {2}".format(t1, iter_nums, t1 / iter_nums)
    )
    t2 = timeit.timeit(lambda: pickle.dumps(foo), number=iter_nums)
    print(
        "pickle take {0} for {1} times, avg: {2}".format(t2, iter_nums, t2 / iter_nums)
    )


@pytest.mark.skip(reason="take too long")
def test_decode():
    # print(foo_schema()
    encoder = fury.create_row_encoder(foo_schema())
    foo = create_foo()

    row = encoder.to_row(foo)
    assert foo == encoder.from_row(row)
    t1 = timeit.timeit(lambda: encoder.from_row(row), number=iter_nums)
    print(
        "encoder take {0} for {1} times, avg: {2}, size {3}".format(
            t1, iter_nums, t1 / iter_nums, row.size_bytes()
        )
    )
    pickled_data = pickle.dumps(foo)
    t2 = timeit.timeit(lambda: pickle.loads(pickled_data), number=iter_nums)
    print(
        "pickle take {0} for {1} times, avg: {2}, size {3}".format(
            t2, iter_nums, t2 / iter_nums, len(pickled_data)
        )
    )


if __name__ == "__main__":
    test_encode()
    test_decode()
