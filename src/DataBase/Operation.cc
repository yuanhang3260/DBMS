#include "Operation.h"

namespace DataBase {

void DeleteResult::MergeFrom(DeleteResult& other) {
  // Merge mutated rids.
  Schema::DataRecordRidMutation::Merge(rid_mutations, other.rid_mutations);
  
  // Previously mutated rids may be deleted later in new DeleteResult.
  // Find them and append to deleted_rid list.
  Schema::DataRecordRidMutation::Merge(rid_mutations, other.rid_deleted, true);
  rid_deleted.insert(rid_deleted.end(),
                     other.rid_deleted.begin(), other.rid_deleted.end());
}

}  // namespace DataBase
