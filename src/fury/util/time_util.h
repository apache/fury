#pragma once

#include <chrono>
#include <string>

namespace fury {

std::string FormatTimePoint(std::chrono::system_clock::time_point tp);

}
