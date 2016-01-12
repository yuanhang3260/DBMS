#include <climits>
#include <string.h>
#include <iostream>
#include <stdexcept>
#include <algorithm>

#include "Base/Utils.h"
#include "Base/Log.h"
#include "PageRecordsManager.h"

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


// ************************** PageRecordsManager **************************** //
PageRecordsManager::PageRecordsManager(DataBaseFiles::RecordPage* page,
                                       TableSchema* schema,
                                       std::vector<int> key_indexes,
                                       DataBaseFiles::FileType file_type,
                                       DataBaseFiles::PageType page_type) :
    page_(page),
    schema_(schema),
    key_indexes_(key_indexes),
    file_type_(file_type),
    page_type_(page_type) {
  if (!page) {
    LogFATAL("Can't init PageRecordsManager with page nullptr");
  }
  if (!schema) {
    LogFATAL("Can't init PageRecordsManager with schema nullptr");
  }

  if (!LoadRecordsFromPage()) {
    LogFATAL("Load page %d records failed", page->id());
  }
}


void PageRecordsManager::SortRecords(
         std::vector<std::shared_ptr<Schema::RecordBase>>& records,
         const std::vector<int>& key_indexes) {
  for (int i: key_indexes) {
    if (i >= records[0]->NumFields()) {
      LogERROR("key index = %d, records only has %d fields",
               i, records[0]->NumFields());
      throw std::out_of_range("key index out of range");
    }
  }
  auto comparator = std::bind(RecordBase::RecordComparator,
                              std::placeholders::_1, std::placeholders::_2,
                              key_indexes);
  std::stable_sort(records.begin(), records.end(), comparator);
}

bool PageRecordsManager::LoadRecordsFromPage() {
  if (!page_) {
    LogERROR("Can't load records from page nullptr");
    return false;
  }

  // Clean previous data.
  plrecords_.clear();

  const auto& slot_directory = page_->Meta()->slot_directory();
  for (int slot_id = 0; slot_id < (int)slot_directory.size(); slot_id++) {
    int offset = slot_directory.at(slot_id).offset();
    int length = slot_directory.at(slot_id).length();
    if (offset < 0) {
      continue;
    }
    plrecords_.push_back(PageLoadedRecord(slot_id));
    plrecords_.back().GenerateRecordPrototype(schema_, key_indexes_,
                                              file_type_, page_type_);
    int load_size = plrecords_.back().record()->
                        LoadFromMem(page_->Record(slot_id));
    if (load_size != length) {
      LogERROR("Error loading slot %d from page - expect %d byte, actual %d ",
               slot_id, length, load_size);
      return false;
    }
    total_size_ += load_size;
  }

  if (plrecords_.empty()) {
    return true;  // Got empty page.
  }

  // Sort records
  auto comparator = std::bind(PageLoadedRecord::Comparator,
                              std::placeholders::_1, std::placeholders::_2,
                              ProduceIndexesToCompare());
  std::stable_sort(plrecords_.begin(), plrecords_.end(), comparator);

  return true;
}

bool PageRecordsManager::InsertRecordToPage(const RecordBase* record) {
  byte* buf = page_->InsertRecord(record->size());
  if (buf) {
    // Write the record content to page.
    record->DumpToMem(buf);
    return true;
  }
  return false;
}

std::vector<int> PageRecordsManager::ProduceIndexesToCompare() const {
  std::vector<int> indexes;
  if (file_type_ == DataBaseFiles::INDEX_DATA &&
      page_type_ == DataBaseFiles::TREE_LEAVE) {
    indexes = key_indexes_;
  }
  else {
    for (int i = 0; i < (int)key_indexes_.size(); i++) {
      indexes.push_back(i);
    }
  }
  return indexes;
}

bool PageRecordsManager::CheckSort() const {
  if (plrecords_.empty()) {
    return true;
  }

  std::vector<int> check_indexes = ProduceIndexesToCompare();
  for (int i = 0; i < (int)plrecords_.size() - 1; i++) {
    const auto& r1 = plrecords_.at(i);
    const auto& r2 = plrecords_.at(i + 1);
    for (int index: check_indexes) {
      int re = RecordBase::CompareSchemaFields(
                   (r1.record()->fields())[index].get(),
                   (r2.record()->fields())[index].get());
      if (re > 0) {
        return false;
      }
      if (re < 0) {
        return true;
      }
    }
  }
  return true;
}

int PageRecordsManager::AppendRecordAndSplitPage(RecordBase* record) {
  if (!InsertNewRecord(record)) {
    LogERROR("Can't insert new record to PageRecordsManager");
    return -1;
  }

  int acc_size = 0;
  int i = 0;
  for (; i < (int)plrecords_.size(); i++) {
    acc_size += plrecords_.at(i).record()->size();
    if (acc_size > total_size_ / 2) {
      break;
    }
  }
  if (acc_size - total_size_ / 2 >
      total_size_ / 2 - acc_size + (int)plrecords_.at(i).record()->size()) {
    i--;
  }
  return i + 1;
}

RecordBase* PageRecordsManager::Record(int index) const {
  if (index >= (int)plrecords_.size()) {
    return nullptr;
  }
  return plrecords_.at(index).record();
}

int PageRecordsManager::CompareRecordWithKey(const RecordBase* key,
                                             const RecordBase* record) const {
  if (page_type_ == DataBaseFiles::TREE_NODE ||
      file_type_ == DataBaseFiles::INDEX) {
    return RecordBase::CompareRecordsBasedOnKey(key, record,
                                                ProduceIndexesToCompare());
  }
  else {
    // file_type = INDEX_DATA && page_type = TREE_LEAVE
    return RecordBase::CompareRecordWithKey(key, record, key_indexes_);
  }
}

int PageRecordsManager::SearchForKey(const RecordBase* key) const {
  if (!key) {
    LogERROR("key to search for is nullptr");
    return -1;
  }

  if (plrecords_.empty()) {
    LogERROR("Empty page, won't search");
    return -1;
  }

  int index = 0;
  for (; index < (int)plrecords_.size(); index++) {
    if (CompareRecordWithKey(key, Record(index)) < 0) {
      break;
    }
  }
  index--;
  if (index < 0) {
    LogFATAL("Search for key less than all record keys of this page");
    key->Print();
    Record(0)->Print();
  }

  return index;
}

bool PageRecordsManager::InsertNewRecord(RecordBase* record) {
  if (plrecords_.empty()) {
    LogERROR("Won't add the record - This PageRecordsManager has not loaded "
             "any PageLoadedRecord");
    return false;
  }
  if (record->NumFields() != plrecords_[0].NumFields()) {
    LogERROR("Can't insert a new reocrd to PageRecordsManager - record "
             "has mismatching number of fields with that of this page");
    return false;
  }

  // Create a new PageLoadedRecord with this record. We need to duplicate
  // the record and pass it to PageLoadedRecord so that it won't take ownership
  // of the original one.
  PageLoadedRecord new_plrecord;
  new_plrecord.set_record(record->Duplicate());
  plrecords_.insert(plrecords_.end(), new_plrecord);
  auto comparator = std::bind(PageLoadedRecord::Comparator,
                              std::placeholders::_1, std::placeholders::_2,
                              ProduceIndexesToCompare());
  std::stable_sort(plrecords_.begin(), plrecords_.end(), comparator);
  total_size_ += record->size();
  return true;
}

namespace {

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

class HalfSplitResult {
 public:
  HalfSplitResult() = default;

  int mid_index = -1;
  int left_records = 0;
  int left_size = 0;
  int right_records = 0;
  int right_size = 0;
  bool left_larger = false;
};

HalfSplitResult HalfSplitRecordGroups(const std::vector<RecordGroup>* rgroups,
                                      int start, int end) {
  HalfSplitResult result;
  for (int i = start; i <= end; i++) {
    result.right_records += rgroups->at(i).num_records;
    result.right_size += rgroups->at(i).size;
  }

  int min_abs = INT_MIN;
  int index = start;
  for (; index <= end; index++) {
    result.left_records += rgroups->at(index).num_records;
    result.left_size += rgroups->at(index).size;
    result.right_records -= rgroups->at(index).num_records;
    result.right_size -= rgroups->at(index).size;
    int abs_value = std::abs(result.left_size - result.right_size);
    if (abs_value < min_abs) {
      min_abs = abs_value;
    }
    else {
      result.left_records -= rgroups->at(index).num_records;
      result.left_size -= rgroups->at(index).size;
      result.right_records += rgroups->at(index).num_records;
      result.right_size += rgroups->at(index).size;
      break;
    }
  }

  result.mid_index = index;
  result.left_larger = result.left_size > result.right_size;
  return result;
}

}

std::vector<PageRecordsManager::SplitLeaveResults>
PageRecordsManager::InsertRecordAndSplitPage(RecordBase* record) {
  std::vector<PageRecordsManager::SplitLeaveResults> result;
  if (!InsertNewRecord(record)) {
    LogERROR("Can't insert new record to PageRecordsManager");
    return result;
  }

  RecordBase* crt_record = Record(0);
  int crt_start = 0;
  int num_records = 0;
  int size = 0;
  std::vector<RecordGroup> rgroups;
  for (int i = 0; i <= (int)plrecords_.size(); i++) {
    if (i < (int)plrecords_.size() &&
        CompareRecordWithKey(crt_record, Record(i)) == 0) {
      num_records++;
      size += Record(i)->size();
    }
    else {
      rgroups.push_back(RecordGroup(crt_start, num_records, size));
      crt_start = i + 1;
      num_records = 0;
      size = 0;
      crt_record = Record(crt_start);
    }
  }

  HalfSplitResult re1 = HalfSplitRecordGroups(&rgroups, 0, rgroups.size() - 1);
  if (re1.left_larger) {
    int new_record_inserted = false;
    auto page = tree_->AllocateNewPage(DataBaseFiles::TREE_LEAVE);
    for (int i = rgroups.at(re1.mid_index).start_index;
         i < (int)rgroups.size();
         i++) {
      if (!Record(i)->InsertToRecordPage(page)) {
        LogFATAL("Insert new record to right half split failed.");
      }
      if (plrecords_.at(i).slot_id() < 0) {
        new_record_inserted = true;
      }
      else {
        page_->DeleteRecord(plrecords_.at(i).slot_id());
      }
    }
    if (new_record_inserted || record->InsertToRecordPage(page_)) {
      DataBaseFiles::BplusTree::ConnectLeaves(page_, page);
      result.emplace_back(page_);
      result[0].record = plrecords_[0].record_;
      result.emplace_back(page);
      result[1].record =
          plrecords_[rgroups.at(re1.mid_index).start_index].record_;
      return result;
    }
    else {
      int gindex = re1.mid_index - 1;
      if (gindex == 0) {
        auto of_page = tree_->AppendOverflowPageTo(page_);
        if (!record->InsertToRecordPage(of_page)) {
          LogFATAL("Insert new record to first page's overflow page failed");
        }
        DataBaseFiles::BplusTree::ConnectLeaves(of_page, page);
        result.emplace_back(page_);
        result[0].record = plrecords_[0].record_;
        result.emplace_back(page);
        result[1].record =
          plrecords_[rgroups.at(re1.mid_index).start_index].record_;
        return result;
      }
      else {
        auto group = rgroups.at(gindex);
        bool new_record_in_second_page = false;
        for (int i = group.start_index;
             i < group.start_index + group.num_records;
             i++) {
          int slot_id = plrecords_.at(i).slot_id();
          if (slot_id < 0) {
            new_record_in_second_page = true;
          }
          else {
            page_->DeleteRecord(slot_id);
          }
        }
        if (!new_record_in_second_page) {
          if (!record->InsertToRecordPage(page_)) {
            LogFATAL("Failed to insert new record to first page");
          }
        }
        // Now we insert remaining records to a middle page.
        auto page2 = tree_->AllocateNewPage(DataBaseFiles::TREE_LEAVE);
        auto tail_page = page2;
        int index = group.start_index;
        for (; index < group.start_index + group.num_records; index++) {
          if (!Record(index)->InsertToRecordPage(tail_page)) {
            // Append overflow page to middle page.
            tail_page = tree_->AppendOverflowPageTo(tail_page);
            if (!record->InsertToRecordPage(tail_page)) {
              LogFATAL("Insert new record to mid page's overflow page failed");
            }
          }
        }
        DataBaseFiles::BplusTree::ConnectLeaves(tail_page, page2);
        DataBaseFiles::BplusTree::ConnectLeaves(page2, page);
        result.emplace_back(page_);
        result[0].record = plrecords_[0].record_;
        result.emplace_back(page2);
        result[1].record = plrecords_[group.start_index].record_;
        result.emplace_back(page);
        result[2].record = plrecords_[index].record_;
        return result;
      }
    }
  }
  else {  // Right half is larger.
    bool new_record_inserted = false;
    auto page2 = tree_->AllocateNewPage(DataBaseFiles::TREE_LEAVE);
    auto tail_page = page2;
    auto group = rgroups.at(re1.mid_index);
    // Page 1
    result.emplace_back(page_);
    result[0].record = plrecords_[0].record_;
    for (int i = group.start_index;
         i < group.start_index + group.num_records;
         i++) {
      if (!Record(i)->InsertToRecordPage(tail_page)) {
        // Append overflow page to middle page.
        tail_page = tree_->AppendOverflowPageTo(tail_page);
        if (!record->InsertToRecordPage(tail_page)) {
          LogFATAL("Insert new record to mid page's overflow page failed");
        }
      }
      int slot_id = plrecords_.at(i).slot_id();
      if (slot_id < 0) {
        new_record_inserted = true;
      }
      else {
        page_->DeleteRecord(slot_id);
      }
    }
    // Page 2
    int index = group.start_index + group.num_records;
    result.emplace_back(page2);
    result[1].record = plrecords_[index].record_;
    if (page2->Meta()->overflow_page() < 0) {
      for (; index < (int)plrecords_.size(); index++) {
        int slot_id = plrecords_.at(index).slot_id(); 
        if (slot_id < 0) {
          new_record_inserted = true;
        }
        else {
          page_->DeleteRecord(slot_id);
        }
        if (!Record(index)->InsertToRecordPage(page2)) {
          break;
        }
      }
    }
    // Page 3, maybe
    int page3_start_index = index;
    DataBaseFiles::RecordPage* page3 = nullptr;
    if (index < (int)plrecords_.size()) {
      page3 = tree_->AllocateNewPage(DataBaseFiles::TREE_LEAVE);
      for (; index < (int)plrecords_.size(); index++) {
        int slot_id = plrecords_.at(index).slot_id();
        if (slot_id < 0) {
          new_record_inserted = true;
        }
        else {
          page_->DeleteRecord(slot_id);
        }
        if (!Record(index)->InsertToRecordPage(page3)) {
          LogFATAL("Failed to insert record to third page");
        }
      }
    }
    if (!new_record_inserted) {
      if (!record->InsertToRecordPage(page_)) {
        LogFATAL("Failed to insert new record to first page");
      }
    }

    // Return result
    DataBaseFiles::BplusTree::ConnectLeaves(page_, page2);
    if (page3) {
      DataBaseFiles::BplusTree::ConnectLeaves(tail_page, page3);
      result.emplace_back(page3);
      result[2].record = plrecords_[page3_start_index].record_;
    }
  }

  return result;
}

}