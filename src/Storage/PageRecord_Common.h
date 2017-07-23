#ifndef STORAGE_PAGE_RECORD_COMMON_
#define STORAGE_PAGE_RECORD_COMMON_

#include <memory>

#include "Database/Catalog_pb.h"
#include "Storage/Record.h"
#include "Storage/Common.h"
#include "Storage/RecordPage.h"

namespace Storage {

// This class wraps a record loaded from page.
class PageLoadedRecord {
 public:
  PageLoadedRecord() = default;
  PageLoadedRecord(int32 slot_id) : slot_id_(slot_id) {}

  DEFINE_ACCESSOR(slot_id, int32);

  DEFINE_ACCESSOR_SMART_PTR(record, RecordBase);
  std::shared_ptr<RecordBase> shared_record() { return record_; }

  uint32 NumFields() const {
    if (!record_) {
      return 0;
    }
    return record_->fields().size();
  }

  // Generate internal record type for this PageLoadedRecord. The internal
  // reocrd can be DataRecord, IndexRecord or TreeNodeRecord, depending on
  // the specified file_type and page_type.
  bool GenerateRecordPrototype(const DB::TableInfo& schema,
                               const std::vector<int>& key_indexes,
                               FileType file_type,
                               PageType page_type);

  // Comparator
  static bool Comparator(const PageLoadedRecord& r1,
                         const PageLoadedRecord& r2,
                         const std::vector<int>& indexes);

  void Print() const {
    std::cout << "slot[" << slot_id_ << "] ";
    record_->Print();
  }

  int LoadFromMem(const byte* buf);
  int DumpToMem(byte* buf) const;

 private:
  std::shared_ptr<RecordBase> record_;
  int32 slot_id_ = -1;
};


// Record group.
class RecordGroup {
 public:
  RecordGroup(int start_index_, int num_records_, int size_) :
      start_index(start_index_),
      num_records(num_records_),
      size(size_) {
  }
  uint32 start_index;
  uint32 num_records;
  uint32 size;
};


// Inserted DataRecord with RecordID, used to produce index files.
class DataRecordWithRid {
 public:
  DataRecordWithRid(std::shared_ptr<RecordBase> record_, RecordID rid_) :
      record(record_),
      rid(rid_) {
  }

  std::shared_ptr<RecordBase> record;
  RecordID rid;

  static bool Comparator(const DataRecordWithRid& r1,
                         const DataRecordWithRid& r2,
                         const std::vector<int>& indexes);

  static void Sort(std::vector<DataRecordWithRid>* records,
                   const std::vector<int>& key_indexes);
};


// Record ID of a DataRecord may be changed when leaves are splited or merged,
// and index files needs to be updated.
class DataRecordRidMutation {
 public:
  DataRecordRidMutation(std::shared_ptr<RecordBase> record_,
                        RecordID old_rid_, RecordID new_rid_) :
      record(record_),
      old_rid(old_rid_),
      new_rid(new_rid_) {
  }

  std::shared_ptr<RecordBase> record;
  RecordID old_rid;
  RecordID new_rid;

  void Print() const;

  static bool Comparator(const DataRecordRidMutation& r1,
                         const DataRecordRidMutation& r2,
                         const std::vector<int>& indexes);

  static void Sort(std::vector<DataRecordRidMutation>* records,
                   const std::vector<int>& key_indexes);

  static void SortByOldRid(std::vector<DataRecordRidMutation>* records);

  static bool ValidityCheck(const std::vector<DataRecordRidMutation>& v);

  // Merge v2 into v1. After merging, v1 contains all valid RecordID mutations,
  // and v2 contains RecordIDs to delete.
  static bool Merge(std::vector<DataRecordRidMutation>& v1,
                    std::vector<DataRecordRidMutation>& v2,
                    bool v2_is_deleted_rid=false);

  // Group DataRecordRidMutation list by key index.
  static void GroupDataRecordRidMutations(
                    std::vector<DataRecordRidMutation>& rid_mutations,
                    std::vector<int> key_index,
                    std::vector<RecordGroup>* rgroups);
};


}  // namespace Schema

#endif  /* STORAGE_PAGE_RECORD_COMMON_ */