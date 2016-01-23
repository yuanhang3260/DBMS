#ifndef SCHEMA_PAGE_RECORD_
#define SCHEMA_PAGE_RECORD_

#include <memory>

#include "Record.h"
#include "DBTable_pb.h"
#include "Storage/Common.h"
#include "Storage/RecordPage.h"

namespace Schema {

// This class wraps a record loaded from page.
class PageLoadedRecord {
 public:
  PageLoadedRecord() = default;
  PageLoadedRecord(int slot_id) : slot_id_(slot_id) {}
  
  DEFINE_ACCESSOR(slot_id, int);
  DEFINE_ACCESSOR_SMART_PTR(record, Schema::RecordBase);

  std::shared_ptr<RecordBase> Record() { return record_; }

  int NumFields() const {
    if (!record_) {
      return 0;
    }
    return record_->fields().size();
  }

  // Generate internal record type for this PageLoadedRecord. The internal
  // reocrd can be DataRecord, IndexRecord or TreeNodeRecord, depending on
  // the specified file_type and page_type.
  bool GenerateRecordPrototype(const TableSchema* schema,
                               std::vector<int> key_indexes,
                               DataBaseFiles::FileType file_type,
                               DataBaseFiles::PageType page_type);

  // Comparator
  static bool Comparator(const PageLoadedRecord& r1, const PageLoadedRecord& r2,
                         const std::vector<int>& indexes);

  void Print() const {
    std::cout << "slot[" << slot_id_ << "] ";
    record_->Print();
  }

 private:
  std::shared_ptr<RecordBase> record_;
  int slot_id_ = -1;
};


// Record group.
class RecordGroup {
 public:
  RecordGroup(int start_index_, int num_records_, int size_) :
      start_index(start_index_),
      num_records(num_records_),
      size(size_) {
  }
  int start_index;
  int num_records;
  int size;
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

  static void Sort(std::vector<Schema::DataRecordWithRid>& records,
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

  static bool Comparator(const DataRecordRidMutation& r1,
                         const DataRecordRidMutation& r2,
                         const std::vector<int>& indexes);
};


}  // namespace Schema

#endif  /* SCHEMA_PAGE_RECORD_ */