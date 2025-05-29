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

#include "fory/util/status.h"
#include <assert.h>
#include <string>
#include <unordered_map>

namespace std {
template <> struct hash<fory::StatusCode> {
  size_t operator()(const fory::StatusCode &t) const { return size_t(t); }
};
} // namespace std

namespace fory {
#define STATUS_CODE_OK "OK"
#define STATUS_CODE_OUT_OF_MEMORY "Out of memory"
#define STATUS_CODE_KEY_ERROR "Key error"
#define STATUS_CODE_TYPE_ERROR "Type error"
#define STATUS_CODE_INVALID "Invalid"
#define STATUS_CODE_IO_ERROR "IOError"
#define STATUS_CODE_UNKNOWN_ERROR "Unknown error"

std::string Status::ToString() const {
  std::string result(CodeAsString());
  if (state_ == NULL) {
    return result;
  }
  result += ": ";
  result += state_->msg;
  return result;
}

std::string Status::CodeAsString() const {
  if (state_ == NULL) {
    return STATUS_CODE_OK;
  }

  // Ensure this is consistent with `str_to_code` in `StringToCode`.
  static std::unordered_map<StatusCode, std::string> code_to_str = {
      {StatusCode::OK, STATUS_CODE_OK},
      {StatusCode::OutOfMemory, STATUS_CODE_OUT_OF_MEMORY},
      {StatusCode::KeyError, STATUS_CODE_KEY_ERROR},
      {StatusCode::TypeError, STATUS_CODE_TYPE_ERROR},
      {StatusCode::Invalid, STATUS_CODE_INVALID},
      {StatusCode::IOError, STATUS_CODE_IO_ERROR},
      {StatusCode::UnknownError, STATUS_CODE_UNKNOWN_ERROR},
  };

  auto it = code_to_str.find(code());
  if (it == code_to_str.end()) {
    return STATUS_CODE_UNKNOWN_ERROR;
  }
  return it->second;
}

StatusCode Status::StringToCode(const std::string &str) {
  // Ensure this is consistent with `code_to_str` in `CodeAsString`.
  static std::unordered_map<std::string, StatusCode> str_to_code = {
      {STATUS_CODE_OK, StatusCode::OK},
      {STATUS_CODE_OUT_OF_MEMORY, StatusCode::OutOfMemory},
      {STATUS_CODE_KEY_ERROR, StatusCode::KeyError},
      {STATUS_CODE_TYPE_ERROR, StatusCode::TypeError},
      {STATUS_CODE_INVALID, StatusCode::Invalid},
      {STATUS_CODE_IO_ERROR, StatusCode::IOError},
      {STATUS_CODE_UNKNOWN_ERROR, StatusCode::UnknownError},
  };

  auto it = str_to_code.find(str);
  if (it == str_to_code.end()) {
    return StatusCode::IOError;
  }
  return it->second;
}

} // namespace fory