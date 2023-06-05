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
  DEBUG = -1,
  INFO = 0,
  WARNING = 1,
  ERROR = 2,
  FATAL = 3
};

#define FURY_LOG_INTERNAL(level) ::fury::FuryLog(__FILE__, __LINE__, level)

#define FURY_LOG_ENABLED(level)                                                \
  fury::FuryLog::IsLevelEnabled(fury::FuryLogLevel::level)

#define FURY_LOG(level)                                                        \
  if (fury::FuryLog::IsLevelEnabled(fury::FuryLogLevel::level))                \
  FURY_LOG_INTERNAL(fury::FuryLogLevel::level)

#define FURY_IGNORE_EXPR(expr) ((void)(expr))

#define FURY_CHECK(condition)                                                  \
  (condition)                                                                  \
      ? FURY_IGNORE_EXPR(0)                                                    \
      : ::fury::Voidify() &                                                    \
            ::fury::FuryLog(__FILE__, __LINE__, fury::FuryLogLevel::FATAL)     \
                << " Check failed: " #condition " "

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
    if (IsEnabled()) {
      Stream() << t;
    }
    return *this;
  };

  /// Return whether or not current logging instance is enabled.
  ///
  /// \return True if logging is enabled and false otherwise.
  virtual bool IsEnabled() const;

  /// Return whether or not the log level is enabled in current setting.
  ///
  /// \param log_level The input log level to test.
  /// \return True if input log level is not lower than the threshold.
  static bool IsLevelEnabled(FuryLogLevel log_level);

  static FuryLogLevel GetLogLevel();

protected:
  virtual std::ostream &Stream() { return std::cerr; };

private:
  /// True if log messages should be logged and false if they should be ignored.
  bool is_enabled_;
  /// log level.
  FuryLogLevel severity_;
};

const FuryLogLevel __fury_severity_threshold__ = ::fury::FuryLog::GetLogLevel();

// This class make FURY_CHECK compilation pass to change the << operator to
// void.
class Voidify {
public:
  Voidify() {}
  // This has to be an operator with a precedence lower than << but
  // higher than ?:
  void operator&(FuryLog &) {}
};

} // namespace fury
