#include <vector>

#include "Base/Utils.h"
#include "Base/Log.h"
#include "UnitTest/UnitTest.h"

#include "DataBase/Table.h"
#include "Storage/PageRecord_Common.h"

namespace DB {

class TableTest: public UnitTest {
 private:

 public:

};

}  // namespace Schema

int main() {
  DB::TableTest test;
  test.setup();
  test.teardown();

  std::cout << "\033[2;32mPassed ^_^\033[0m" << std::endl;
  return 0;
}