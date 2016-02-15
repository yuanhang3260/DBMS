#ifndef DATABSE_TABLE_OPERATION_
#define DATABSE_TABLE_OPERATION_

#include <vector>
#include <memory>

#include "Schema/Record.h"
#include "Schema/PageRecord_Common.h"

namespace DataBase {

enum OpCondition {
  EQ = 0,
  LT = 1,  
  GT = 2,
  NE = 3,
  BT = 4,  // Betwee two values
};

class DeleteOp {
 public:
  int key_index = -1;
  std::vector<std::shared_ptr<Schema::RecordBase>> keys;
  OpCondition op_cond = EQ;
};

class DeleteResult {
 public:
  enum DeleteMode {
    DEL_DATA,
    DEL_INDEX_PRE,
    DEL_INDEX_POS,
  };

  bool records_moved = true;
  DeleteMode del_mode = DEL_DATA;
  std::vector<int> mutated_leaves;
  std::vector<Schema::DataRecordRidMutation> rid_deleted;
  std::vector<Schema::DataRecordRidMutation> rid_mutations;

  // Merge another delete result into me.
  bool MergeFrom(DeleteResult& other);
  // Used when deleting data records from deleted_rid list given an index
  // tree's delete result.
  bool MergeDeleteRidsFromMutatedRids(DeleteResult& other);

  // Validity check - It checks no duplicated old_rids from rid_mutations and
  // rid_deleted, and no duplicated new_rids from rid_mutations (rid_deleted 
  // has all new_rids (-1, -1).
  bool ValidityCheck();
};

}  // namespace DataBase

#endif  /* DATABSE_TABLE_OPERATION_ */
