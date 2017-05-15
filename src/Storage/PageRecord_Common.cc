#include <vector>
#include <map>
#include <set>
#include <algorithm>
#include <functional>

#include "Base/Utils.h"
#include "Base/MacroUtils.h"
#include "Base/Log.h"

#include "Storage/PageRecord_Common.h"

namespace Storage {

// **************************** PageLoadedRecord **************************** //
bool PageLoadedRecord::GenerateRecordPrototype(
         const Schema::TableSchema& schema,
         std::vector<int> key_indexes,
         FileType file_type,
         PageType page_type) {
  // Create record based on file tpye and page type
  if (file_type == INDEX_DATA &&
      page_type == TREE_LEAVE) {
    record_.reset(new DataRecord());
  }
  else if (file_type == INDEX &&
           page_type == TREE_LEAVE) {
    record_.reset(new IndexRecord());
  }
  else if (page_type == TREE_NODE ||
           page_type == TREE_ROOT) {
    record_.reset(new TreeNodeRecord());
  }

  if (!record_) {
    LogERROR("Illegal file_type and page_type combination");
    return false;
  }

  record_->InitRecordFields(schema, key_indexes, file_type, page_type);
  return true;
}

bool PageLoadedRecord::Comparator(const PageLoadedRecord& r1,
                                  const PageLoadedRecord& r2,
                                  const std::vector<int>& indexes) {
  // TODO: Compare Rid for Index B+ tree?
  return RecordBase::RecordComparator(r1.record_, r2.record_, indexes);
}


// *************************** DataRecordWithRid **************************** //
bool DataRecordWithRid::Comparator(const DataRecordWithRid& r1,
                                   const DataRecordWithRid& r2,
                                   const std::vector<int>& indexes) {
  return RecordBase::RecordComparator(r1.record, r2.record, indexes);
}

void DataRecordWithRid::Sort(std::vector<DataRecordWithRid>& records,
                             const std::vector<int>& key_indexes) {
  auto comparator = std::bind(Comparator,
                              std::placeholders::_1, std::placeholders::_2,
                              key_indexes);
  std::stable_sort(records.begin(), records.end(), comparator);
}


// ************************ DataRecordRidMutation *************************** //
bool DataRecordRidMutation::Comparator(const DataRecordRidMutation& r1,
                                       const DataRecordRidMutation& r2,
                                       const std::vector<int>& indexes) {
  return RecordBase::RecordComparator(r1.record, r2.record, indexes);
}

void DataRecordRidMutation::Sort(
         std::vector<DataRecordRidMutation>& records,
         const std::vector<int>& key_indexes) {
  auto comparator = std::bind(Comparator,
                              std::placeholders::_1, std::placeholders::_2,
                              key_indexes);
  std::stable_sort(records.begin(), records.end(), comparator);
}

void DataRecordRidMutation::SortByOldRid(
         std::vector<DataRecordRidMutation>& records) {
  std::function<bool (const DataRecordRidMutation& r1,
                      const DataRecordRidMutation& r2)> comparator =
      [](const DataRecordRidMutation& r1,
         const DataRecordRidMutation& r2) {
        return r1.old_rid < r2.old_rid;
      };
  std::stable_sort(records.begin(), records.end(), comparator);
}

void DataRecordRidMutation::Print() const {
  printf("rid: (%d, %d) --> (%d, %d)   ",
         old_rid.page_id(), old_rid.slot_id(),
         new_rid.page_id(), new_rid.slot_id());
  if (record) {
    record->Print();
  }
  else {
    printf("(null)\n");
  }
}

bool DataRecordRidMutation::ValidityCheck(
         const std::vector<DataRecordRidMutation>& v) {
  if (v.empty()) {
    return true;
  }

  std::set<RecordID> old_rid_set;
  std::set<RecordID> new_rid_set;
  for (const auto& r : v) {
    if (old_rid_set.find(r.old_rid) != old_rid_set.end()) {
      return false;
    }
    old_rid_set.insert(r.old_rid);
    if (r.new_rid.IsValid() &&
        new_rid_set.find(r.new_rid) != new_rid_set.end()) {
      return false;
    }
    new_rid_set.insert(r.new_rid);
  }
  return true;
}

bool DataRecordRidMutation::Merge(std::vector<DataRecordRidMutation>& v1,
                                  std::vector<DataRecordRidMutation>& v2,
                                  bool v2_is_deleted_rid) {
  if (!ValidityCheck(v1)) {
    return false;
  }
  if (!ValidityCheck(v2)) {
    return false;
  }

  std::map<int, int> rid_m_cascade_map;
  std::vector<int> insert_list;
  for (int i = 0; i < (int)v2.size(); i++) {
    bool cascade = false;
    for (int j = 0; j < (int)v1.size(); j++) {
      if (rid_m_cascade_map.find(j) == rid_m_cascade_map.end() &&
          v1[j].new_rid == v2[i].old_rid) {
        rid_m_cascade_map.emplace(j, i);
        cascade = true;
      }
    }
    if (!cascade) {
      insert_list.push_back(i);
    }
  }
  // update cascaded rid mutations.
  for (auto const& e: rid_m_cascade_map) {
    if (!v2_is_deleted_rid) {
      v1[e.first].new_rid = v2[e.second].new_rid;
    }
    else {
      v2[e.second].old_rid = v1[e.first].old_rid;
      v1[e.first].new_rid = RecordID();
    }
  }

  if (!v2_is_deleted_rid) {
    for (auto const i: insert_list) {
      v1.push_back(v2[i]);
    }
  }

  // Scan v1 - if v2 is a deleted_rid list, rid mutations in v1 now with empty
  // "new_rid" are the rids to delete from tree. Remove them from v1.
  if (v2_is_deleted_rid) {
    for (auto it = v1.begin(); it != v1.end();) {
      if (it->new_rid.IsValid()) {
        it++;
      }
      else {
        it = v1.erase(it);
      }
    }
  }

  return true;
}

void DataRecordRidMutation::GroupDataRecordRidMutations(
          std::vector<DataRecordRidMutation>& rid_mutations,
          std::vector<int> key_index,
          std::vector<RecordGroup>* rgroups) {
  if (rid_mutations.empty()) {
    return;
  }

  auto crt_record = rid_mutations[0].record;
  int crt_start = 0;
  int num_records = 0;
  for (int i = 0; i < (int)rid_mutations.size(); i++) {
    if (RecordBase::CompareRecordsBasedOnIndex(
            crt_record.get(), rid_mutations[i].record.get(), key_index) == 0) {
      num_records++;
    }
    else {
      rgroups->push_back(RecordGroup(crt_start, num_records, -1));
      crt_start = i;
      num_records = 1;
      crt_record = rid_mutations[crt_start].record;
    }
  }
  rgroups->push_back(RecordGroup(crt_start, num_records, -1));
}

}  // namespace Schema
