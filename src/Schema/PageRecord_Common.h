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


}  // namespace Schema

#endif  /* SCHEMA_PAGE_RECORD_ */