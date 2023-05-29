#include "fury/util/status.h"
#include "gtest/gtest.h"

namespace fury {
class StatusTest : public ::testing::Test {};

TEST_F(StatusTest, StringToCode) {
  auto ok = Status::OK();
  StatusCode status = Status::StringToCode(ok.CodeAsString());
  ASSERT_EQ(status, StatusCode::OK);

  auto invalid = Status::Invalid("invalid");
  status = Status::StringToCode(invalid.CodeAsString());
  ASSERT_EQ(status, StatusCode::Invalid);

  ASSERT_EQ(Status::StringToCode("foobar"), StatusCode::IOError);
}

} // namespace fury

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
