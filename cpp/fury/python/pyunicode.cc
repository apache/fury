/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "pyunicode.h"
#include "fury/util/array_util.h"
#include "fury/util/logging.h"
#include "fury/util/string_util.h"
#include "unicodeobject.h"
#include <cassert>

namespace fury {

static PyObject *unicode_latin1[256] = {nullptr};

static PyObject *get_latin1_char(unsigned char ch) {
  PyObject *unicode = unicode_latin1[ch];
  if (!unicode) {
    unicode = PyUnicode_New(1, ch);
    if (!unicode)
      return NULL;
    PyUnicode_1BYTE_DATA(unicode)[0] = ch;
    // assert(_PyUnicode_CheckConsistency(unicode, 1));
    unicode_latin1[ch] = unicode;
  }
  Py_INCREF(unicode);
  return unicode;
}

PyObject *Fury_PyUnicode_FromUCS1(const char *u, Py_ssize_t size) {
  PyObject *res;
  unsigned char max_char;
  FURY_CHECK(size > 0);
  if (size == 1)
    return get_latin1_char(u[0]);
  max_char = isAscii(reinterpret_cast<const char *>(u), size) ? 127 : 255;
  res = PyUnicode_New(size, max_char);
  if (!res)
    return NULL;
  memcpy(PyUnicode_1BYTE_DATA(res), u, size);
  // assert(_PyUnicode_CheckConsistency(res, 1));
  return res;
}

PyObject *Fury_PyUnicode_FromUCS2(const uint16_t *u, Py_ssize_t size) {
  PyObject *res;
  Py_UCS2 max_char;
  FURY_CHECK(size > 0);
  if (size == 1) {
    max_char = u[0];
    if (max_char < 256) {
      return get_latin1_char(max_char);
    } else {
      res = PyUnicode_New(1, max_char);
      if (res == NULL) {
        return NULL;
      }
      if (PyUnicode_KIND(res) == PyUnicode_2BYTE_KIND) {
        PyUnicode_2BYTE_DATA(res)[0] = (Py_UCS2)max_char;
      } else {
        FURY_CHECK(PyUnicode_KIND(res) == PyUnicode_4BYTE_KIND);
        PyUnicode_4BYTE_DATA(res)[0] = max_char;
      }
      return res;
    }
  }
  max_char = getMaxValue(u, size);
  res = PyUnicode_New(size, max_char);
  if (!res) {
    return NULL;
  }
  if (max_char >= 256) {
    memcpy(PyUnicode_2BYTE_DATA(res), u, sizeof(Py_UCS2) * size);
  } else {
    copyArray(u, PyUnicode_1BYTE_DATA(res), size);
  }
  // assert(_PyUnicode_CheckConsistency(res, 1));
  return res;
}
} // namespace fury
