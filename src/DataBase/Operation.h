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
  DeleteMode mode = DEL_DATA;
  std::vector<int> mutated_leaves;
  std::vector<Schema::DataRecordRidMutation> rid_deleted;
  std::vector<Schema::DataRecordRidMutation> rid_mutations;
};

}  // namespace DataBase

#endif  /* DATABSE_TABLE_OPERATION_ */
