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
