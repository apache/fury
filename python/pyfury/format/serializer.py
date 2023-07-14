import pyarrow as pa
from pyfury.serializer import CrossLanguageCompatibleSerializer, BufferObject
from pyfury.buffer import Buffer
from pyfury.type import FuryType


class ArrowRecordBatchSerializer(CrossLanguageCompatibleSerializer):
    def write(self, buffer, value: pa.RecordBatch):
        self.fury.write_buffer_object(buffer, ArrowRecordBatchBufferObject(value))

    def read(self, buffer: Buffer) -> pa.Table:
        fury_buf = self.fury.read_buffer_object(buffer)
        # If the input source supports zero-copy reads (e.g. like a memory
        # map, or pa.BufferReader), then the returned batches are also
        # zero-copy and do not allocate any new memory on read.
        # So here the read is zero copy.
        reader = pa.ipc.open_stream(pa.py_buffer(fury_buf))
        [batch] = [batch for batch in reader]
        return batch

    def get_xtype_id(self):
        return FuryType.FURY_ARROW_RECORD_BATCH.value


class ArrowRecordBatchBufferObject(BufferObject):
    def __init__(self, batch: pa.RecordBatch):
        self.batch = batch
        mock_sink = pa.MockOutputStream()
        ArrowRecordBatchBufferObject._write(batch, mock_sink)
        self.nbytes = mock_sink.size()

    def total_bytes(self) -> int:
        return self.nbytes

    def write_to(self, buffer: Buffer):
        assert isinstance(buffer, Buffer)

        sink = pa.FixedSizeBufferWriter(pa.py_buffer(buffer))
        self._write(self.batch, sink)

    def to_buffer(self) -> Buffer:
        sink = pa.BufferOutputStream()
        ArrowRecordBatchBufferObject._write(self.batch, sink)
        return Buffer(sink.getvalue())

    @staticmethod
    def _write(batch, sink):
        stream_writer = pa.RecordBatchStreamWriter(sink, batch.schema)
        stream_writer.write_batch(batch)
        stream_writer.close()


class ArrowTableSerializer(CrossLanguageCompatibleSerializer):
    def write(self, buffer, value: pa.Table):
        self.fury.write_buffer_object(buffer, ArrowTableBufferObject(value))

    def read(self, buffer: Buffer) -> pa.Table:
        fury_buf = self.fury.read_buffer_object(buffer)
        # If the input source supports zero-copy reads (e.g. like a memory
        # map, or pa.BufferReader), then the returned batches are also
        # zero-copy and do not allocate any new memory on read.
        # So here the read is zero copy.
        reader = pa.ipc.open_stream(pa.py_buffer(fury_buf))
        batches = [batch for batch in reader]
        return pa.Table.from_batches(batches)

    def get_xtype_id(self):
        return FuryType.FURY_ARROW_TABLE.value


class ArrowTableBufferObject(BufferObject):
    def __init__(self, table: pa.Table):
        self.table = table
        mock_sink = pa.MockOutputStream()
        ArrowTableBufferObject._write(table, mock_sink)
        self.nbytes = mock_sink.size()

    def total_bytes(self) -> int:
        return self.nbytes

    def write_to(self, buffer: Buffer):
        assert isinstance(buffer, Buffer)
        sink = pa.FixedSizeBufferWriter(pa.py_buffer(buffer))
        ArrowTableBufferObject._write(self.table, sink)

    def to_buffer(self) -> Buffer:
        sink = pa.BufferOutputStream()
        self._write(self.table, sink)
        return Buffer(sink.getvalue())

    @staticmethod
    def _write(table, sink):
        stream_writer = pa.RecordBatchStreamWriter(sink, table.schema)
        for batch in table.to_batches():
            stream_writer.write_batch(batch)
        stream_writer.close()
