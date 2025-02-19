#include "pyfury.h"

static PyObject **PySequenceGetItems(PyObject *collection) {
  if (PyList_CheckExact(collection)) {
    return _PyList_CAST(collection)->ob_item;
  } else if (PyTuple_CheckExact(collection)) {
    return _PyTuple_CAST(collection)->ob_item;
  }
  return nullptr;
}

namespace fury {
int Fury_PyBooleanSequenceWriteToBuffer(PyObject *collection, Buffer *buffer,
                                        Py_ssize_t start_index) {
  PyObject **items = PySequenceGetItems(collection);
  if (items == nullptr) {
    return -1;
  }
  Py_ssize_t size = Py_SIZE(collection);
  for (Py_ssize_t i = 0; i < size; i++) {
    bool b = Py_IsTrue(items[i]);
    buffer->UnsafePut(start_index, b);
    start_index += sizeof(bool);
  }
  return 0;
}

int Fury_PyFloatSequenceWriteToBuffer(PyObject *collection, Buffer *buffer,
                                      Py_ssize_t start_index) {
  PyObject **items = PySequenceGetItems(collection);
  if (items == nullptr) {
    return -1;
  }
  Py_ssize_t size = Py_SIZE(collection);
  for (Py_ssize_t i = 0; i < size; i++) {
    auto *f = (PyFloatObject *)items[i];
    buffer->UnsafePut(start_index, f->ob_fval);
    start_index += sizeof(double);
  }
  return 0;
}
} // namespace fury
