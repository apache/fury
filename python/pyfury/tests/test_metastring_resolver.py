from pyfury import Buffer
from pyfury._serialization import MetaStringResolver, MetaStringBytes
from pyfury.meta.metastring import MetaStringEncoder


def test_metastring_resolver():
    resolver = MetaStringResolver()
    encoder = MetaStringEncoder()
    metastr1 = encoder.encode("hello, world")
    metabytes1 = resolver.get_metastr_bytes(metastr1)
    buffer = Buffer.allocate(32)
    resolver.write_meta_string_bytes(buffer, metabytes1)
    assert resolver.read_meta_string_bytes(buffer) == metabytes1
    metabytes2 = MetaStringBytes(
        data=b"\xbf\x05\xa4q\xa9\x92S\x96\xa6IOr\x9ch)\x80",
        hashcode=-5456063526933366015,
    )
    resolver.write_meta_string_bytes(buffer, metabytes2)
    assert resolver.read_meta_string_bytes(buffer) == metabytes2
