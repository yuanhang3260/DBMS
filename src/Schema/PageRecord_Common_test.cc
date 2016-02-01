#include <vector>

#include "UnitTest/UnitTest.h"
#include "Base/Utils.h"
#include "Base/Log.h"

#include "PageRecord_Common.h"

namespace Schema {

class PageRecordCommonTest: public UnitTest {
 private:

 public:
  void Test_Merge_RidMutations() {
    {
      std::vector<DataRecordRidMutation> v1;
      v1.emplace_back(std::shared_ptr<RecordBase>(),
                      RecordID(1,2), RecordID(3,4));
      v1.emplace_back(std::shared_ptr<RecordBase>(),
                      RecordID(5,6), RecordID(7,8));
      v1.emplace_back(std::shared_ptr<RecordBase>(),
                      RecordID(9,10), RecordID(11,12));

      std::vector<DataRecordRidMutation> v2;
      v2.emplace_back(std::shared_ptr<RecordBase>(),
                      RecordID(11,12), RecordID(15,16));
      v2.emplace_back(std::shared_ptr<RecordBase>(),
                      RecordID(15,16), RecordID(17,18));
      v2.emplace_back(std::shared_ptr<RecordBase>(),
                      RecordID(17,18), RecordID(19,20));

      DataRecordRidMutation::Merge(v1, v2);
      AssertEqual(5, (int)v1.size());
      AssertTrue(v1[2].old_rid == RecordID(9, 10));
      AssertTrue(v1[2].new_rid == RecordID(15, 16));
      AssertTrue(v1[4].old_rid == RecordID(17,18));
      AssertTrue(v1[4].new_rid == RecordID(19,20));
      AssertTrue(DataRecordRidMutation::ValidityCheck(v1));
    }

    {
      std::vector<DataRecordRidMutation> v1;
      v1.emplace_back(std::shared_ptr<RecordBase>(),
                      RecordID(1,2), RecordID(3,4));
      v1.emplace_back(std::shared_ptr<RecordBase>(),
                      RecordID(5,6), RecordID(7,8));
      v1.emplace_back(std::shared_ptr<RecordBase>(),
                      RecordID(9,10), RecordID(11,12));

      std::vector<DataRecordRidMutation> v2;
      v2.emplace_back(std::shared_ptr<RecordBase>(),
                      RecordID(13,14), RecordID(15,16));
      v2.emplace_back(std::shared_ptr<RecordBase>(),
                      RecordID(15,16), RecordID(19,20));

      DataRecordRidMutation::Merge(v1, v2);
      AssertEqual(5, (int)v1.size());
      AssertTrue(v1[4].old_rid == RecordID(15,16));
      AssertTrue(v1[4].new_rid == RecordID(19,20));
      AssertTrue(DataRecordRidMutation::ValidityCheck(v1));
    }
  }
};

}  // namespace Schema

int main() {
  Schema::PageRecordCommonTest test;
  test.setup();
  test.Test_Merge_RidMutations();
  test.teardown();

  std::cout << "\033[2;32mPassed ^_^\033[0m" << std::endl;
  return 0;
}