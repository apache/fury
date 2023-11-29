/*
 * Copyright 2023 The Fury Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "fury/util/time_util.h"
#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <string>

namespace fury {

using std::chrono::system_clock;

std::string FormatTimePoint(system_clock::time_point tp) {
  std::stringstream ss;
  time_t raw_time = system_clock::to_time_t(tp);
  // unnecessary to release tm, it's created by localtime and every thread will
  // have one.
  struct tm *timeinfo = std::localtime(&raw_time);
  char buffer[80];
  std::strftime(buffer, 80, "%Y-%m-%d %H:%M:%S,", timeinfo);
  ss << buffer;
  std::chrono::milliseconds ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          tp.time_since_epoch());
  std::string milliseconds_str = std::to_string(ms.count() % 1000);
  if (milliseconds_str.length() < 3) {
    milliseconds_str =
        std::string(3 - milliseconds_str.length(), '0') + milliseconds_str;
  }
  ss << milliseconds_str;
  return ss.str();
}

} // namespace fury
