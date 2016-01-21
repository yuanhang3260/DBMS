#include "Base/Utils.h"
#include "Base/MacroUtils.h"
#include "Base/Log.h"

#include "PageRecord_Common.h"

namespace Schema {

// **************************** PageLoadedRecord **************************** //
bool PageLoadedRecord::GenerateRecordPrototype(
         const TableSchema* schema,
         std::vector<int> key_indexes,
         DataBaseFiles::FileType file_type,
         DataBaseFiles::PageType page_type) {
  // Create record based on file tpye and page type
  if (file_type == DataBaseFiles::INDEX_DATA &&
      page_type == DataBaseFiles::TREE_LEAVE) {
    record_.reset(new DataRecord());
  }
  else if (file_type == DataBaseFiles::INDEX &&
      page_type == DataBaseFiles::TREE_LEAVE) {
    record_.reset(new IndexRecord());
  }
  else if (page_type == DataBaseFiles::TREE_NODE ||
      page_type == DataBaseFiles::TREE_ROOT) {
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


// ************************ DataRecordRidMutation *************************** //
bool DataRecordRidMutation::Comparator(const DataRecordRidMutation& r1,
                                       const DataRecordRidMutation& r2,
                                       const std::vector<int>& indexes) {
  return RecordBase::RecordComparator(r1.record, r2.record, indexes);
}

}  // namespace Schema
