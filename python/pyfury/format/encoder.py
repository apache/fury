import pyfury
import pyfury.type


class Encoder:
    def __init__(self, clz=None, schema=None):
        """
        A pojo class whose schema can be inferred by `pyfury.infer_schema`
        """
        self.clz = clz
        self.schema = schema or pyfury.format.infer.infer_schema(clz)
        self.row_encoder = pyfury.create_row_encoder(self.schema)
        self.schema_hash: bytes = pyfury.format.infer.compute_schema_hash(self.schema)

    def encode(self, obj):
        row = self.row_encoder.to_row(obj)
        buffer = pyfury.Buffer.allocate(8 + row.size_bytes())
        buffer.write_int64(self.schema_hash)
        row_bytes = row.to_bytes()
        buffer.write_bytes(row_bytes)
        return buffer.to_bytes(0, buffer.writer_index)

    def decode(self, binary: bytes):
        buf = pyfury.Buffer(binary, 0, len(binary))
        peer_hash = buf.read_int64()
        assert self.schema_hash == peer_hash, (
            f"Schema is not consistent, encoder schema is {self.schema}, "
            f"clz is {self.clz}. Self/peer schema hash is "
            f"{self.schema_hash, peer_hash}. "
            f"Please check writer schema."
        )
        buf = pyfury.Buffer(binary, 8, len(binary))
        row = pyfury.RowData(self.schema, buf)
        return self.row_encoder.from_row(row)

    def to_row(self, obj):
        return self.row_encoder.to_row(obj)

    def from_row(self, binary: bytes):
        buf = pyfury.Buffer(binary, 0, len(binary))
        row = pyfury.RowData(self.schema, buf)
        return self.row_encoder.from_row(row)


def encoder(clz=None, schema=None):
    """A pojo class whose schema can be inferred by `pyfury.infer_schema`"""
    return Encoder(clz=clz, schema=schema)
