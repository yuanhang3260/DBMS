#ifndef DATABSE_TABLE_OPERATION_
#define DATABSE_TABLE_OPERATION_

#include <vector>
#include <memory>

#include "Query/Common.h"
#include "Storage/Record.h"
#include "Storage/PageRecord_Common.h"

namespace DB {

struct SearchOp {
  std::vector<uint32> field_indexes;

  std::shared_ptr<Storage::RecordBase> key;

  std::shared_ptr<Storage::RecordBase> left_key;
  bool left_open = true;

  std::shared_ptr<Storage::RecordBase> right_key;
  bool right_open = true;

  Storage::RecordBase* AddKey() {
    key.reset(new Storage::RecordBase());
    return key.get();
  }

  Storage::RecordBase* AddLeftKey() {
    left_key.reset(new Storage::RecordBase());
    return left_key.get();
  }

  Storage::RecordBase* AddRightKey() {
    right_key.reset(new Storage::RecordBase());
    return right_key.get();
  }

  void reset() {
    field_indexes.clear();
    key.reset();
    left_key.reset();
    left_open = true;
    right_key.reset();
    right_open = true;
  }
};

struct DeleteOp {
  std::vector<uint32> field_indexes;
  std::vector<std::shared_ptr<Storage::RecordBase>> keys;
  Query::OperatorType op_cond = Query::EQUAL;
};

class DeleteResult {
 public:
  enum DeleteMode {
    DEL_DATA,
    DEL_INDEX_PRE,
    DEL_INDEX_POS,
  };

  DeleteMode del_mode = DEL_DATA;
  std::vector<int> mutated_leaves;
  std::vector<Storage::DataRecordRidMutation> rid_deleted;
  std::vector<Storage::DataRecordRidMutation> rid_mutations;

  // Merge another delete result into me.
  bool MergeFrom(DeleteResult& other);
  // Used when deleting data records from deleted_rid list given an index
  // tree's delete result.
  bool UpdateDeleteRidsFromMutatedRids(DeleteResult& other);

  // Validity check - It checks no duplicated old_rids from rid_mutations and
  // rid_deleted, and no duplicated new_rids from rid_mutations (rid_deleted 
  // has all new_rids (-1, -1).
  bool ValidityCheck();
};

}  // namespace DB

#endif  /* DATABSE_TABLE_OPERATION_ */
