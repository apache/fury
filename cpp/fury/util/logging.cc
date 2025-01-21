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

#include "fury/util/logging.h"
#include "absl/debugging/failure_signal_handler.h"
#include "absl/debugging/stacktrace.h"
#include "absl/debugging/symbolize.h"
#include "fury/util/time_util.h"
#include <unordered_map>
#include <vector>

namespace std {
template <> struct hash<fury::FuryLogLevel> {
  size_t operator()(fury::FuryLogLevel t) const { return size_t(t); }
};
} // namespace std

namespace fury {

const FuryLogLevel fury_severity_threshold = FuryLog::GetLogLevel();

std::string GetCallTrace() {
  std::vector<void *> local_stack;
  local_stack.resize(100);
  absl::GetStackTrace(local_stack.data(), 100, 0);
  static constexpr size_t buf_size = 16 * 1024;
  char buf[buf_size];
  std::string output;
  for (auto &stack : local_stack) {
    if (absl::Symbolize(stack, buf, buf_size)) {
      output.append("    ").append(buf).append("\n");
    }
  }
  return output;
}

std::unordered_map<FuryLogLevel, std::string> log_level_to_str = {
    {FuryLogLevel::FURY_DEBUG, "DEBUG"},
    {FuryLogLevel::FURY_INFO, "INFO"},
    {FuryLogLevel::FURY_WARNING, "WARNING"},
    {FuryLogLevel::FURY_ERROR, "ERROR"},
    {FuryLogLevel::FURY_FATAL, "FATAL"},
};

std::string LogLevelAsString(FuryLogLevel level) {
  auto it = log_level_to_str.find(level);
  if (it == log_level_to_str.end()) {
    return "UNKNOWN";
  }
  return it->second;
}

FuryLogLevel FuryLog::GetLogLevel() {
  FuryLogLevel severity_threshold = FuryLogLevel::FURY_INFO;
  const char *var_value = std::getenv("FURY_LOG_LEVEL");
  if (var_value != nullptr) {
    std::string data = var_value;
    std::transform(data.begin(), data.end(), data.begin(), ::tolower);
    if (data == "debug") {
      severity_threshold = FuryLogLevel::FURY_DEBUG;
    } else if (data == "info") {
      severity_threshold = FuryLogLevel::FURY_INFO;
    } else if (data == "warning") {
      severity_threshold = FuryLogLevel::FURY_WARNING;
    } else if (data == "error") {
      severity_threshold = FuryLogLevel::FURY_ERROR;
    } else if (data == "fatal") {
      severity_threshold = FuryLogLevel::FURY_FATAL;
    } else {
      FURY_LOG_INTERNAL(FURY_WARNING)
          << "Unrecognized setting of FuryLogLevel=" << var_value;
    }
    FURY_LOG_INTERNAL(FURY_INFO)
        << "Set ray log level from environment variable RAY_BACKEND_LOG_LEVEL"
        << " to " << static_cast<int>(severity_threshold);
  }
  return severity_threshold;
}

FuryLog::FuryLog(const char *file_name, int line_number, FuryLogLevel severity)
    : severity_(severity) {
  Stream() << "[" << FormatTimePoint(std::chrono::system_clock::now()) << "] "
           << LogLevelAsString(severity) << " " << file_name << ":"
           << line_number << ": ";
}

FuryLog::~FuryLog() {
  if (severity_ == FuryLogLevel::FURY_FATAL) {
    Stream() << "\n*** StackTrace Information ***\n" << ::fury::GetCallTrace();
    Stream() << std::endl;
    std::_Exit(EXIT_FAILURE);
  }
  Stream() << "\n" << std::endl;
}

bool FuryLog::IsLevelEnabled(FuryLogLevel log_level) {
  return log_level >= fury_severity_threshold;
}

} // namespace fury
