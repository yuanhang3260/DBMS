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
};

class DeleteOp {
 public:
  int key_index = -1;
  std::vector<std::shared_ptr<Schema::RecordBase>> keys;
  OpCondition op_cond = EQ;
};

class DeleteResult {
 public:
  
};

}  // namespace DataBase

#endif  /* DATABSE_TABLE_OPERATION_ */
