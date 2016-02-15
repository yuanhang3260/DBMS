#include <set>
#include <map>

#include "Base/Log.h"
#include "Base/Utils.h"
#include "Operation.h"

namespace DataBase {

bool DeleteResult::MergeFrom(DeleteResult& other) {
  if (!ValidityCheck() || !other.ValidityCheck()) {
    LogERROR("Pre-check DeleteResult failed - Invalid");
    return false;
  }

  // Merge mutated rids.
  if (!Schema::DataRecordRidMutation::Merge(rid_mutations,other.rid_mutations)){
    return false;
  }

  // Previously mutated rids may be deleted later in new DeleteResult.
  // Find them and append to deleted_rid list.
  if (!Schema::DataRecordRidMutation::Merge(rid_mutations, other.rid_deleted,
                                            true)) {
    return false;
  }
  rid_deleted.insert(rid_deleted.end(),
                     other.rid_deleted.begin(), other.rid_deleted.end());

  if (!ValidityCheck()) {
    LogERROR("Malicious merge - got an invalid DeleteResult");
    return false;
  }
  return true;
}

bool DeleteResult::MergeDeleteRidsFromMutatedRids(DeleteResult& other) {
  if (other.rid_mutations.empty()) {
    return true;
  }

  if (!ValidityCheck() || !other.ValidityCheck()) {
    LogERROR("Pre-check DeleteResult failed - Invalid");
    return false;
  }

  std::map<int, int> updated_rids_map;
  for (int i = 0; i < (int)other.rid_mutations.size(); i++) {
    for (int j = 0; j < (int)rid_deleted.size(); j++) {
      if (rid_deleted[j].old_rid == other.rid_mutations[i].old_rid) {
        updated_rids_map.emplace(j, i);
      }
    }
  }

  for (auto const& e: updated_rids_map) {
    rid_deleted[e.first].old_rid = other.rid_mutations[e.second].new_rid;
  }
  return true;
}

bool DeleteResult::ValidityCheck() {
  if (rid_mutations.empty() && rid_deleted.empty()) {
    return true;
  }

  std::set<Schema::RecordID> old_rid_set;
  std::set<Schema::RecordID> new_rid_set;

  // Checks rid mutations.
  for (const auto& r : rid_mutations) {
    if (old_rid_set.find(r.old_rid) != old_rid_set.end()) {
      return false;
    }
    old_rid_set.insert(r.old_rid);
    if (new_rid_set.find(r.new_rid) != new_rid_set.end()) {
      return false;
    }
    new_rid_set.insert(r.new_rid);
  }

  // Checks rid deleted - no duplicated old_rid
  for (const auto& r : rid_deleted) {
    if (old_rid_set.find(r.old_rid) != old_rid_set.end()) {
      return false;
    }
    old_rid_set.insert(r.old_rid);
  }
  return true;
}

}  // namespace DataBase
