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

#include "fury/util/logging.h"
#include <cstring>
#include <iosfwd>
#include <memory>
#include <string>

//
// GCC can be told that a certain branch is not likely to be taken (for
// instance, a CHECK failure), and use that information in static analysis.
// Giving it this information can help it optimize for the common case in
// the absence of better information (ie. -fprofile-arcs).
//
#if defined(__GNUC__)
#define FURY_PREDICT_FALSE(x) (__builtin_expect(x, 0))
#define FURY_PREDICT_TRUE(x) (__builtin_expect(!!(x), 1))
#define FURY_NORETURN __attribute__((noreturn))
#define FURY_PREFETCH(addr) __builtin_prefetch(addr)
#elif defined(_MSC_VER)
#define FURY_NORETURN __declspec(noreturn)
#define FURY_PREDICT_FALSE(x) x
#define FURY_PREDICT_TRUE(x) x
#define FURY_PREFETCH(addr)
#else
#define FURY_NORETURN
#define FURY_PREDICT_FALSE(x) x
#define FURY_PREDICT_TRUE(x) x
#define FURY_PREFETCH(addr)
#endif

namespace fury {

// Return the given status if it is not OK.
#define FURY_RETURN_NOT_OK(s)                                                  \
  do {                                                                         \
    ::fury::Status _s = (s);                                                   \
    if (FURY_PREDICT_FALSE(!_s.ok())) {                                        \
      return _s;                                                               \
    }                                                                          \
  } while (0)

#define FURY_RETURN_NOT_OK_ELSE(s, else_)                                      \
  do {                                                                         \
    ::fury::Status _s = (s);                                                   \
    if (!_s.ok()) {                                                            \
      else_;                                                                   \
      return _s;                                                               \
    }                                                                          \
  } while (0)

// If 'to_call' returns a bad status, CHECK immediately with a logged message
// of 'msg' followed by the status.
#define FURY_CHECK_OK_PREPEND(to_call, msg)                                    \
  do {                                                                         \
    ::fury::Status _s = (to_call);                                             \
    FURY_CHECK(_s.ok()) << (msg) << ": " << _s.ToString();                     \
  } while (0)

// If the status is bad, CHECK immediately, appending the status to the
// logged message.
#define FURY_CHECK_OK(s) FURY_CHECK_OK_PREPEND(s, "Bad status")

enum class StatusCode : char {
  OK = 0,
  OutOfMemory = 1,
  OutOfBound = 2,
  KeyError = 3,
  TypeError = 4,
  Invalid = 5,
  IOError = 6,
  UnknownError = 7,
};

class Status {
public:
  // Create a success status.
  Status() : state_(nullptr) {}
  ~Status() = default;

  Status(StatusCode code, std::string msg)
      : state_(new State{code, std::move(msg)}) {}

  // Copy the specified status.
  Status(const Status &s) : state_(s.state_ ? new State(*s.state_) : nullptr) {}
  Status &operator=(const Status &s) {
    if (s.state_) {
      state_.reset(new State(*s.state_));
    } else {
      state_ = nullptr;
    }

    return *this;
  }

  // Move the specified status.
  Status(Status &&s) = default;
  Status &operator=(Status &&s) = default;

  // Return a success status.
  static Status OK() { return Status(); }

  // Return error status of an appropriate type.
  static Status OutOfMemory(const std::string &msg) {
    return Status(StatusCode::OutOfMemory, msg);
  }

  static Status OutOfBound(const std::string &msg) {
    return Status(StatusCode::OutOfMemory, msg);
  }

  static Status KeyError(const std::string &msg) {
    return Status(StatusCode::KeyError, msg);
  }

  static Status TypeError(const std::string &msg) {
    return Status(StatusCode::TypeError, msg);
  }

  static Status UnknownError(const std::string &msg) {
    return Status(StatusCode::UnknownError, msg);
  }

  static Status Invalid(const std::string &msg) {
    return Status(StatusCode::Invalid, msg);
  }

  static Status IOError(const std::string &msg) {
    return Status(StatusCode::IOError, msg);
  }

  static StatusCode StringToCode(const std::string &str);

  // Returns true iff the status indicates success.
  bool ok() const { return state_ == nullptr; }

  bool IsOutOfMemory() const { return code() == StatusCode::OutOfMemory; }
  bool IsKeyError() const { return code() == StatusCode::KeyError; }
  bool IsInvalid() const { return code() == StatusCode::Invalid; }
  bool IsIOError() const { return code() == StatusCode::IOError; }
  bool IsTypeError() const { return code() == StatusCode::TypeError; }
  bool IsUnknownError() const { return code() == StatusCode::UnknownError; }

  // Return a string representation of this status suitable for printing.
  // Returns the string "OK" for success.
  std::string ToString() const;

  // Return a string representation of the status code, without the message
  // text or posix code information.
  std::string CodeAsString() const;

  StatusCode code() const { return ok() ? StatusCode::OK : state_->code; }

  std::string message() const { return ok() ? "" : state_->msg; }

private:
  struct State {
    StatusCode code;
    std::string msg;
  };
  // OK status has a `NULL` state_.  Otherwise, `state_` points to
  // a `State` structure containing the error code and message(s)
  std::unique_ptr<State> state_;
};

inline std::ostream &operator<<(std::ostream &os, const Status &x) {
  return os << x.ToString();
}

} // namespace fury
