#include <set>
#include <map>

#include "Base/Log.h"
#include "Base/Utils.h"

#include "Database/Operation.h"

namespace DB {

bool DeleteResult::MergeFrom(DeleteResult& other) {
  // printf("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n");
  // printf("my rid_mutations\n");
  // for (const auto& m: rid_mutations) {
  //   m.Print();
  // }
  // printf("my rid_deleted\n");
  // for (const auto& m: rid_deleted) {
  //   m.Print();
  // }
  // printf("other rid_mutations\n");
  // for (const auto& m: other.rid_mutations) {
  //   m.Print();
  // }
  // printf("other rid_deleted\n");
  // for (const auto& m: other.rid_deleted) {
  //   m.Print();
  // }
  // printf("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n");

  if (!ValidityCheck()) {
    LogERROR("Self Pre-check DeleteResult failed - Invalid");
    return false;
  }

  if (!other.ValidityCheck()) {
    LogERROR("Pre-check DeleteResult failed - Invalid");
    return false;
  }

  // Previously mutated rids may be deleted later in new DeleteResult.
  // Find them and append to deleted_rid list.
  if (!Storage::DataRecordRidMutation::Merge(rid_mutations, other.rid_deleted,
                                             true)) {
    LogERROR("Failed to merge rid_deleted to rid mutations");
    return false;
  }
  rid_deleted.insert(rid_deleted.end(),
                     other.rid_deleted.begin(), other.rid_deleted.end());

  if (!ValidityCheck()) {
    LogFATAL("Malicious merge 1 - got an invalid DeleteResult");
    return false;
  }

  // Merge mutated rids.
  if (!Storage::DataRecordRidMutation::Merge(
          rid_mutations, other.rid_mutations)) {
    LogERROR("Failed to merge rid mutations");
    return false;
  }

  if (!ValidityCheck()) {
    LogFATAL("Malicious merge 2 - got an invalid DeleteResult");
    return false;
  }

  return true;
}

bool DeleteResult::UpdateDeleteRidsFromMutatedRids(DeleteResult& other) {
  if (other.rid_mutations.empty()) {
    return true;
  }

  // printf("$$$$ other mutation $$$$\n");
  // for (const auto& m: other.rid_mutations) {
  //   m.Print();
  // }
  // printf("#### rids\n");
  // for (const auto& m: rid_deleted) {
  //   m.Print();
  // }
  // printf("**************************\n");

  if (!other.ValidityCheck()) {
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

  std::set<Storage::RecordID> old_rid_set;
  std::set<Storage::RecordID> new_rid_set;

  // Checks rid mutations.
  for (const auto& r : rid_mutations) {
    if (old_rid_set.find(r.old_rid) != old_rid_set.end()) {
      LogERROR("Found duplicated rid_mutations");
      r.old_rid.Print();
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
      LogERROR("Found duplicated rid_deleted");
      r.old_rid.Print();
      return false;
    }
    old_rid_set.insert(r.old_rid);
  }
  return true;
}

}  // namespace DB
