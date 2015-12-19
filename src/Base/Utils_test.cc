#include <vector>
#include <set>

#include "UnitTest/UnitTest.h"
#include "Utils.h"

class UtilsTest: public UnitTest {
 public:
  void Test_RandomListFromRange() {
    for (int i = 0; i < 100; i++) {
      int start = Utils::RandomNumber(10000);
      int end = Utils::RandomNumber(10000);
      if (start > end) {
        Utils::Swap(&start, &end);
      }
      int size = Utils::RandomNumber(end - start + 1) + 1;
      // generate random list
      std::vector<int> result = Utils::RandomListFromRange(start, end, size);
      AssertEqual(size, (int)result.size(), "List size mismatch");
      std::set<int> included;
      for (int ele: result) {
        AssertGreaterEqual(ele, start, "element less than start");
        AssertLessEqual(ele, end, "element greater than end");
        AssertTrue(included.find(ele) == included.end(), "duplicated element");
        included.insert(ele);
      }
    }
  }

};


int main() {
  UtilsTest test;
  test.Test_RandomListFromRange();

  std::cout << "\033[2;32mPassed ^_^\033[0m" << std::endl;
  return 0;
}