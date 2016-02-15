#include <vector>

#include "UnitTest/UnitTest.h"
#include "Base/Utils.h"
#include "Base/Log.h"
#include "Operation.h"

namespace DataBase {

class OperationTest: public UnitTest {
 private:

 public:
  void Test_MergeDeleteResult() {
    // Result 1
    DeleteResult del_result;
    del_result.rid_mutations.emplace_back(std::shared_ptr<Schema::RecordBase>(),
                   Schema::RecordID(1,2), Schema::RecordID(3,4));
    del_result.rid_mutations.emplace_back(std::shared_ptr<Schema::RecordBase>(),
                   Schema::RecordID(5,6), Schema::RecordID(7,8));  // m
    del_result.rid_mutations.emplace_back(std::shared_ptr<Schema::RecordBase>(),
                   Schema::RecordID(9,10), Schema::RecordID(11,12));  // d
    del_result.rid_mutations.emplace_back(std::shared_ptr<Schema::RecordBase>(),
                   Schema::RecordID(0,0), Schema::RecordID(1,1)); 
    del_result.rid_mutations.emplace_back(std::shared_ptr<Schema::RecordBase>(),
                   Schema::RecordID(13,14), Schema::RecordID(15,16));  //d
    del_result.rid_mutations.emplace_back(std::shared_ptr<Schema::RecordBase>(),
                   Schema::RecordID(17,18), Schema::RecordID(19,20));  //m
    del_result.rid_mutations.emplace_back(std::shared_ptr<Schema::RecordBase>(),
                   Schema::RecordID(21,22), Schema::RecordID(23,24));

    del_result.rid_deleted.emplace_back(std::shared_ptr<Schema::RecordBase>(),
                   Schema::RecordID(25,26), Schema::RecordID());
    del_result.rid_deleted.emplace_back(std::shared_ptr<Schema::RecordBase>(),
                   Schema::RecordID(27,28), Schema::RecordID());

    // Result 2
    DeleteResult to_merge;
    to_merge.rid_mutations.emplace_back(std::shared_ptr<Schema::RecordBase>(),
                 Schema::RecordID(7,8), Schema::RecordID(30,31));
    to_merge.rid_mutations.emplace_back(std::shared_ptr<Schema::RecordBase>(),
                 Schema::RecordID(19,20), Schema::RecordID(32,33));
    to_merge.rid_mutations.emplace_back(std::shared_ptr<Schema::RecordBase>(),
                 Schema::RecordID(34,35), Schema::RecordID(36,37));

    to_merge.rid_deleted.emplace_back(std::shared_ptr<Schema::RecordBase>(),
                 Schema::RecordID(11,12), Schema::RecordID());
    to_merge.rid_deleted.emplace_back(std::shared_ptr<Schema::RecordBase>(),
                 Schema::RecordID(38,39), Schema::RecordID());
    to_merge.rid_deleted.emplace_back(std::shared_ptr<Schema::RecordBase>(),
                 Schema::RecordID(15,16), Schema::RecordID());

    // Merge
    AssertTrue(del_result.MergeFrom(to_merge), "Merge failed");
    AssertEqual(6, (int)del_result.rid_mutations.size());
    AssertEqual(5, (int)del_result.rid_deleted.size());
    AssertTrue(del_result.rid_mutations[1].old_rid == Schema::RecordID(5, 6));
    AssertTrue(del_result.rid_mutations[1].new_rid == Schema::RecordID(30, 31));
    AssertTrue(del_result.rid_mutations[3].old_rid == Schema::RecordID(17,18));
    AssertTrue(del_result.rid_mutations[3].new_rid == Schema::RecordID(32,33));
    AssertTrue(del_result.rid_deleted[2].old_rid == Schema::RecordID(9,10));
    AssertTrue(del_result.rid_deleted[3].old_rid == Schema::RecordID(38,39));
    AssertTrue(del_result.rid_deleted[4].old_rid == Schema::RecordID(13,14));
  }

  void Test_UpdateDeleteRidList() {
    DeleteResult del_list;
    del_list.rid_deleted.emplace_back(std::shared_ptr<Schema::RecordBase>(),
                   Schema::RecordID(1,2), Schema::RecordID());
    del_list.rid_deleted.emplace_back(std::shared_ptr<Schema::RecordBase>(),
                   Schema::RecordID(3,4), Schema::RecordID());
    del_list.rid_deleted.emplace_back(std::shared_ptr<Schema::RecordBase>(),
                   Schema::RecordID(5,6), Schema::RecordID());
    del_list.rid_deleted.emplace_back(std::shared_ptr<Schema::RecordBase>(),
                   Schema::RecordID(7,8), Schema::RecordID());
    del_list.rid_deleted.emplace_back(std::shared_ptr<Schema::RecordBase>(),
                   Schema::RecordID(9,10), Schema::RecordID());

    DeleteResult del_result;
    del_result.rid_mutations.emplace_back(std::shared_ptr<Schema::RecordBase>(),
                   Schema::RecordID(1,2), Schema::RecordID(3,4));
    del_result.rid_mutations.emplace_back(std::shared_ptr<Schema::RecordBase>(),
                   Schema::RecordID(3,4), Schema::RecordID(0,1));
    del_result.rid_mutations.emplace_back(std::shared_ptr<Schema::RecordBase>(),
                   Schema::RecordID(11,12), Schema::RecordID(13,14));
    del_result.rid_mutations.emplace_back(std::shared_ptr<Schema::RecordBase>(),
                   Schema::RecordID(9,10), Schema::RecordID(7,8));

    AssertTrue(del_list.MergeDeleteRidsFromMutatedRids(del_result),
               "Update delete rid list failed");
    AssertTrue(del_list.rid_deleted[0].old_rid == Schema::RecordID(3, 4));
    AssertTrue(del_list.rid_deleted[1].old_rid == Schema::RecordID(0, 1));
    AssertTrue(del_list.rid_deleted[4].old_rid == Schema::RecordID(7, 8));
    AssertTrue(del_list.rid_deleted[3].old_rid == Schema::RecordID(7, 8));
  }
};

}  // namespace Schema

int main() {
  DataBase::OperationTest test;
  test.setup();
  test.Test_MergeDeleteResult();
  test.Test_UpdateDeleteRidList();
  test.teardown();

  std::cout << "\033[2;32mPassed ^_^\033[0m" << std::endl;
  return 0;
}