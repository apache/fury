from libcpp.memory cimport shared_ptr
from libc.stdint cimport *
from pyfury.includes.libformat cimport CArrowWriter
from pyarrow.lib cimport  CMemoryPool, CRecordBatch
from pyarrow.lib cimport Schema, MemoryPool, check_status
import pyarrow as pa
cimport pyarrow.lib as libpa

cdef class ArrowWriter:
    cdef:
        shared_ptr[CSchema] c_schema
        CMemoryPool *c_pool
        shared_ptr[CArrowWriter] c_arrow_writer

    def __init__(self, Schema schema, MemoryPool pool=None):
        self.c_schema = schema.sp_schema
        if pool is None:
            pool = pa.default_memory_pool()
        self.c_pool = pool.pool
        check_status(CArrowWriter.Make(
            self.c_schema, self.c_pool, &self.c_arrow_writer))

    def write(self, RowData row):
        check_status(self.c_arrow_writer.get().Write(row.data))

    def finish(self):
        cdef shared_ptr[CRecordBatch] batch
        check_status(self.c_arrow_writer.get().Finish(&batch))
        return libpa.pyarrow_wrap_batch(batch)

    def reset(self):
        self.c_arrow_writer.get().Reset()
