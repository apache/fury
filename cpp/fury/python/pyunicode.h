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

#pragma once

#include "fury/util/array_util.h"
#include "fury/util/buffer.h"
#include "fury/util/logging.h"
#include "fury/util/string_util.h"
#include "object.h"
#include "pyport.h"
#include "unicodeobject.h"
#include <cstring>
#include <string>

namespace fury {

// unicodeobject.c
PyObject *Fury_PyUnicode_FromUCS1(const char *u, Py_ssize_t size);

PyObject *Fury_PyUnicode_FromUCS2(const uint16_t *u, Py_ssize_t size);

} // namespace fury
