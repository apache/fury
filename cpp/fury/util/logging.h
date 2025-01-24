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

#include <cstring>
#include <iosfwd>
#include <iostream>
#include <string>

namespace fury {

/// This function returns the current call stack information.
std::string GetCallTrace();

// Simple logging implementation to avoid introduce glog dependency.

enum class FuryLogLevel {
  FURY_DEBUG = -1,
  FURY_INFO = 0,
  FURY_WARNING = 1,
  FURY_ERROR = 2,
  FURY_FATAL = 3
};

#define FURY_LOG_INTERNAL(level)                                               \
  ::fury::FuryLog(__FILE__, __LINE__, ::fury::FuryLogLevel::level)

#define FURY_LOG_ENABLED(level)                                                \
  ::fury::FuryLog::IsLevelEnabled(::fury::FuryLogLevel::level)

#define FURY_LOG(level)                                                        \
  if (FURY_LOG_ENABLED(level))                                                 \
  FURY_LOG_INTERNAL(level)

#define FURY_CHECK(condition)                                                  \
  if (!(condition))                                                            \
  FURY_LOG_INTERNAL(FURY_FATAL) << " Check failed: " #condition " "

#define FURY_CHECK_OP(left, op, right)                                         \
  do {                                                                         \
    const auto &_left_ = left;                                                 \
    const auto &_right_ = right;                                               \
    FURY_CHECK((_left_ op _right_)) << " " << _left_ << " vs " << _right_;     \
  } while (0)

#define FURY_CHECK_EQ(left, right) FURY_CHECK_OP(left, ==, right)
#define FURY_CHECK_NE(left, right) FURY_CHECK_OP(left, !=, right)
#define FURY_CHECK_LE(left, right) FURY_CHECK_OP(left, <=, right)
#define FURY_CHECK_LT(left, right) FURY_CHECK_OP(left, <, right)
#define FURY_CHECK_GE(left, right) FURY_CHECK_OP(left, >=, right)
#define FURY_CHECK_GT(left, right) FURY_CHECK_OP(left, >, right)

class FuryLog {
public:
  FuryLog(const char *file_name, int line_number, FuryLogLevel severity);

  virtual ~FuryLog();

  template <typename T> FuryLog &operator<<(const T &t) {
    Stream() << t;
    return *this;
  };

  /// Return whether or not the log level is enabled in current setting.
  ///
  /// \param log_level The input log level to test.
  /// \return True if input log level is not lower than the threshold.
  static bool IsLevelEnabled(FuryLogLevel log_level);

  static FuryLogLevel GetLogLevel();

protected:
  virtual std::ostream &Stream() { return std::cerr; };

private:
  /// log level.
  FuryLogLevel severity_;
};

} // namespace fury
