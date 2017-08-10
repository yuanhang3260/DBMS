#include <climits>
#include <string.h>
#include <iostream>
#include <stdexcept>
#include <algorithm>

#include "Base/MacroUtils.h"
#include "Base/Log.h"
#include "Base/Utils.h"

#include "Storage/BplusTree.h"
#include "Storage/PageRecordsManager.h"

namespace Storage {

// ************************** PageRecordsManager **************************** //
PageRecordsManager::PageRecordsManager(RecordPage* page,
                                       const DB::TableInfo& schema,
                                       const std::vector<uint32>& key_indexes,
                                       FileType file_type,
                                       PageType page_type) :
    page_(page),
    schema_(&schema),
    key_indexes_(key_indexes),
    file_type_(file_type),
    page_type_(page_type) {
  if (!page) {
    LogFATAL("Can't init PageRecordsManager with page nullptr");
  }

  if (!LoadRecordsFromPage()) {
    LogFATAL("Load page %d records failed", page->id());
  }
}

void PageRecordsManager::SortRecords(
         std::vector<std::shared_ptr<RecordBase>>* records,
         const std::vector<uint32>& key_indexes) {
  for (uint32 i : key_indexes) {
    if (i >= records->at(0)->NumFields()) {
      LogERROR("key index = %d, records only has %d fields",
               i, records->at(0)->NumFields());
      throw std::out_of_range("key index out of range");
    }
  }

  auto comparator = [&key_indexes] (std::shared_ptr<RecordBase> r1,
                                    std::shared_ptr<RecordBase> r2) {
    return RecordBase::RecordComparator(*r1, *r2, key_indexes);
  };
  std::stable_sort(records->begin(), records->end(), comparator);
}

void PageRecordsManager::SortByIndexes(const std::vector<uint32>& key_indexes) {
  auto comparator = std::bind(PageLoadedRecord::Comparator,
                              std::placeholders::_1, std::placeholders::_2,
                              key_indexes);
  std::stable_sort(plrecords_.begin(), plrecords_.end(), comparator);
}

bool PageRecordsManager::LoadRecordsFromPage() {
  if (!page_) {
    LogERROR("Can't load records from page nullptr");
    return false;
  }

  // Clean previous data.
  plrecords_.clear();

  const auto& slot_directory = page_->meta().slot_directory();
  for (uint32 slot_id = 0; slot_id < slot_directory.size(); slot_id++) {
    int offset = slot_directory.at(slot_id).offset();
    int length = slot_directory.at(slot_id).length();
    if (offset < 0) {
      continue;
    }
    plrecords_.push_back(PageLoadedRecord(slot_id));
    slot_plrecords_.emplace(slot_id, &plrecords_.back());
    plrecords_.back().GenerateRecordPrototype(*schema_, key_indexes_,
                                              file_type_, page_type_);
    int load_size = plrecords_.back().LoadFromMem(page_->Record(slot_id));
    if (load_size != length) {
      LogERROR("Error load slot %d from page %d - expect %d byte, actual %d ",
               page_->id(), slot_id, length, load_size);
      plrecords_.back().record().Print();
      return false;
    }
    total_size_ += load_size;
  }

  if (plrecords_.empty()) {
    return true;  // Got empty page.
  }

  // Sort records
  SortByIndexes(ProduceIndexesToCompare());

  return true;
}

bool PageRecordsManager::InsertRecordToPage(const RecordBase* record) {
  int slot_id = page_->InsertRecord(record->size());
  if (slot_id >= 0) {
    // Write the record content to page.
    record->DumpToMem(page_->Record(slot_id));
    return true;
  }
  return false;
}

std::vector<uint32> PageRecordsManager::ProduceIndexesToCompare() const {
  std::vector<uint32> indexes;
  if (file_type_ == INDEX_DATA &&
      page_type_ == TREE_LEAVE) {
    indexes = key_indexes_;
  }
  else {
    for (uint32 i = 0; i < key_indexes_.size(); i++) {
      indexes.push_back(i);
    }
  }
  return indexes;
}

bool PageRecordsManager::CheckSort() const {
  if (plrecords_.empty()) {
    return true;
  }

  std::vector<uint32> check_indexes = ProduceIndexesToCompare();
  for (uint32 i = 0; i < plrecords_.size() - 1; i++) {
    const auto& r1 = plrecords_.at(i);
    const auto& r2 = plrecords_.at(i + 1);
    for (uint32 index: check_indexes) {
      int re = RecordBase::CompareSchemaFields(
                   r1.record().fields().at(index).get(),
                   r2.record().fields().at(index).get());
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

int PageRecordsManager::AppendRecordAndSplitPage(const RecordBase& record) {
  if (!InsertNewRecord(record)) {
    LogERROR("Can't insert new record to PageRecordsManager");
    return -1;
  }

  uint32 acc_size = 0;
  uint32 i = 0;
  for (; i < plrecords_.size(); i++) {
    acc_size += plrecords_.at(i).record().size();
    if (acc_size > total_size_ / 2) {
      break;
    }
  }
  if (acc_size - total_size_ / 2 >
      total_size_ / 2 - acc_size + plrecords_.at(i).record().size()) {
    i--;
  }
  return i + 1;
}

const RecordBase& PageRecordsManager::record(uint32 index) const {
  SANITY_CHECK(index < plrecords_.size(), "index out of range");
  return plrecords_.at(index).record();
}

RecordBase* PageRecordsManager::mutable_record(uint32 index) {
  if (index >= plrecords_.size()) {
    return nullptr;
  }
  return plrecords_.at(index).mutable_record();
}

std::shared_ptr<RecordBase> PageRecordsManager::shared_record(uint32 index) {
  if (index >= plrecords_.size()) {
    return std::shared_ptr<RecordBase>();
  }
  return plrecords_.at(index).shared_record();
}

int PageRecordsManager::RecordSlotID(uint32 index) const {
  if (index >= plrecords_.size()) {
    return -1;
  }
  return plrecords_.at(index).slot_id();
}

void PageRecordsManager::Print() const {
  printf("Printing page %d\n", page_->id());
  for (const auto& record: plrecords_) {
    record.Print();
  }
  printf("Ending page %d\n", page_->id());
}

int PageRecordsManager::CompareRecordWithKey(const RecordBase& record,
                                             const RecordBase& key) const {
  if (page_type_ == TREE_NODE || file_type_ == INDEX) {
    return RecordBase::CompareRecordsBasedOnIndex(record, key,
                                                  ProduceIndexesToCompare());
  }
  else {
    // file_type = INDEX_DATA && page_type = TREE_LEAVE
    return RecordBase::CompareRecordWithKey(record, key, key_indexes_);
  }
}

int PageRecordsManager::CompareRecords(const RecordBase& r1,
                                       const RecordBase& r2) const {
  return RecordBase::CompareRecordsBasedOnIndex(
                         r1, r2, ProduceIndexesToCompare());
}

int PageRecordsManager::SearchForKey(const RecordBase& key) const {
  if (plrecords_.empty()) {
    LogERROR("Empty page, won't search");
    return -1;
  }

  int index = 0;
  for (; index < (int)plrecords_.size(); index++) {
    if (CompareRecordWithKey(record(index), key) > 0) {
      break;
    }
  }
  index--;
  if (index < 0) {
    LogERROR("Search for key less than all record keys of this page");
    key.Print();
    record(0).Print();
    LogFATAL("SearchForKey fatal error");
  }

  return index;
}

bool PageRecordsManager::InsertNewRecord(const RecordBase& record) {
  if (plrecords_.empty()) {
    LogERROR("Won't add the record - This PageRecordsManager has not loaded "
             "any PageLoadedRecord");
    return false;
  }
  if (record.NumFields() != plrecords_[0].NumFields()) {
    LogERROR("Can't insert a new reocrd to PageRecordsManager - record "
             "has mismatching number of fields with that of this page");
    return false;
  }

  // Create a new PageLoadedRecord with this record. We need to duplicate
  // the record and pass it to PageLoadedRecord so that it won't take ownership
  // of the original one.
  PageLoadedRecord new_plrecord;
  new_plrecord.set_record(record.Duplicate());
  plrecords_.insert(plrecords_.end(), new_plrecord);
  SortByIndexes(ProduceIndexesToCompare());
  total_size_ += record.size();

  return true;
}

namespace {

class HalfSplitResult {
 public:
  HalfSplitResult() = default;

  uint32 mid_index = -1;
  uint32 left_records = 0;
  uint32 left_size = 0;
  uint32 right_records = 0;
  uint32 right_size = 0;
  bool left_larger = false;
};

HalfSplitResult HalfSplitRecordGroups(const std::vector<RecordGroup>* rgroups,
                                      uint32 start, uint32 end) {
  HalfSplitResult result;
  for (uint32 i = start; i <= end; i++) {
    result.right_records += rgroups->at(i).num_records;
    // printf("group %d: ", i);
    // printf("start_index = %d, ", rgroups->at(i).start_index);
    // printf("num_records = %d, ", rgroups->at(i).num_records);
    // printf("size = %d\n", rgroups->at(i).size);
    result.right_size += rgroups->at(i).size;
  }

  uint32 min_abs = INT_MAX;
  uint32 index = start;
  for (; index <= end; index++) {
    result.left_records += rgroups->at(index).num_records;
    result.left_size += rgroups->at(index).size;
    result.right_records -= rgroups->at(index).num_records;
    result.right_size -= rgroups->at(index).size;
    uint32 abs_value = result.left_size > result.right_size ?
                            result.left_size - result.right_size :
                            result.right_size - result.left_size;
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

void PageRecordsManager::GroupRecords(std::vector<RecordGroup>* rgroups) {
  // Tree node records are guaranteed different and each group contains exactly
  // one record.
  if (page_->meta().page_type() == TREE_NODE) {
    for (uint32 i = 0; i < plrecords_.size(); i++) {
      rgroups->push_back(RecordGroup(i, 1, record(i).size()));
    }
    return;
  }

  const RecordBase* crt_record = &record(0);
  int crt_start = 0;
  int num_records = 0;
  int size = 0;
  auto cmp_indexes = ProduceIndexesToCompare();
  for (uint32 i = 0; i < plrecords_.size(); i++) {
    if (RecordBase::CompareRecordsBasedOnIndex(*crt_record, record(i),
                                               cmp_indexes) == 0) {
      num_records++;
      size += record(i).size();
    }
    else {
      rgroups->push_back(RecordGroup(crt_start, num_records, size));
      crt_start = i;
      num_records = 1;
      size = record(i).size();
      crt_record = &record(crt_start);
    }
  }
  rgroups->push_back(RecordGroup(crt_start, num_records, size));
}

std::vector<PageRecordsManager::SplitLeaveResults>
PageRecordsManager::InsertRecordAndSplitPage(
    const RecordBase& new_record,
    std::vector<DataRecordRidMutation>* rid_mutations) {
  // Insert new record to prmanager.
  std::vector<PageRecordsManager::SplitLeaveResults> result;
  if (!InsertNewRecord(new_record)) {
    LogERROR("Can't insert new record to PageRecordsManager");
    return result;
  }

  std::vector<RecordGroup> rgroups;
  GroupRecords(&rgroups);

  HalfSplitResult re1 = HalfSplitRecordGroups(&rgroups, 0, rgroups.size() - 1);
  // std::cout << "re1.mid_index: " << re1.mid_index << std::endl;
  // std::cout << "re1.left_records: " << re1.left_records << std::endl;
  // std::cout << "re1.left_size: " << re1.left_size << std::endl;
  // std::cout << "re1.right_records: " << re1.right_records << std::endl;
  // std::cout << "re1.right_size: " << re1.right_size << std::endl;
  // std::cout << "re1.left_larger: " << re1.left_larger << std::endl;
  RecordID rid;
  if (re1.left_larger) {
    // Corner case - all records are same. We need to add new record to
    // overflow page.
    if (re1.mid_index >= rgroups.size()) {
      auto of_page = tree_->AppendOverflowPageTo(page_);
      int slot_id = new_record.InsertToRecordPage(of_page);
      if (slot_id < 0) {
        LogFATAL("Insert new record to first page's overflow page failed");
      }
      result.emplace_back(page_);
      result[0].record = plrecords_[0].shared_record();
      result[0].rid = RecordID(of_page->id(), slot_id);
      return result;
    }

    int new_record_page = -1;
    auto page = tree_->AllocateNewPage(TREE_LEAVE);
    // Allocate a new leave and insert right half records to it.
    for (uint32 i = rgroups.at(re1.mid_index).start_index;
         i < plrecords_.size();
         i++) {
      int new_slot_id = record(i).InsertToRecordPage(page);
      CheckLogFATAL(new_slot_id >= 0, "Insert right half to new leave failed.");
      int slot_id = plrecords_.at(i).slot_id();
      if (slot_id < 0) {
        new_record_page = 1;
        rid = RecordID(page->id(), new_slot_id);
      }
      else {
        page_->DeleteRecord(slot_id);
        if (file_type_ == INDEX_DATA) {
          rid_mutations->emplace_back(shared_record(i),
                                      RecordID(page_->id(), slot_id),
                                      RecordID(page->id(), new_slot_id));
        }
      }
    }
    // If new record is on right half (new_record_inserted = true), then left
    // half records can definitely fit in the original leave and we are done;
    // Otherwise we try inserting the new record to original leave and if
    // success we are also done.
    if (new_record_page < 0) {
      int slot_id = new_record.InsertToRecordPage(page_);
      if (slot_id >= 0) {
        rid = RecordID(page_->id(), slot_id);
        new_record_page = 0;
      }
    }
    if (new_record_page >= 0) {
      BplusTree::ConnectLeaves(page_, page);
      result.emplace_back(page_);
      result[0].record = plrecords_[0].shared_record();
      result.emplace_back(page);
      result[1].record =
          plrecords_[rgroups.at(re1.mid_index).start_index].shared_record();
      result[0].rid = rid;  // RecordID of the new record.
      return result;
    }
    else {
      // We have to split more.
      int gindex = re1.mid_index - 1;
      if (gindex == 0) {
        // This is special case. The first records group becomes so large that
        // original leave can't hold them all.
        auto of_page = tree_->AppendOverflowPageTo(page_);
        int slot_id = new_record.InsertToRecordPage(of_page);
        if (slot_id < 0) {
          LogFATAL("Insert new record to first page's overflow page failed");
        }
        BplusTree::ConnectLeaves(of_page, page);
        result.emplace_back(page_);
        result[0].record = plrecords_[0].shared_record();
        result.emplace_back(page);
        result[1].record =
          plrecords_[rgroups.at(re1.mid_index).start_index].shared_record();
        result[0].rid = RecordID(of_page->id(), slot_id);
        return result;
      }
      else {
        auto group = rgroups.at(gindex);
        bool new_record_in_second_page = false;
        auto page2 = tree_->AllocateNewPage(TREE_LEAVE);
        auto tail_page = page2;
        uint32 i = group.start_index;
        for (; i < group.start_index + group.num_records; i++) {
          // We insert the middle group of records to middle page (page 2).
          int new_slot_id = record(i).InsertToRecordPage(tail_page);
          if (new_slot_id < 0) {
            // Append overflow page to middle page.
            tail_page = tree_->AppendOverflowPageTo(tail_page);
            new_slot_id = new_record.InsertToRecordPage(tail_page);
            if (new_slot_id < 0) {
              LogFATAL("Insert new record to mid page's overflow page failed");
            }
          }
          int slot_id = plrecords_.at(i).slot_id();
          if (slot_id < 0) {
            new_record_in_second_page = true;
            rid = RecordID(tail_page->id(), new_slot_id);
          }
          else {
            page_->DeleteRecord(slot_id);
            if (file_type_ == INDEX_DATA) {
              rid_mutations->emplace_back(shared_record(i),
                                          RecordID(page_->id(), slot_id),
                                          RecordID(tail_page->id(),new_slot_id));
            }
          }
        }
        if (!new_record_in_second_page) {
          int slot_id = new_record.InsertToRecordPage(page_);
          if (slot_id < 0) {
            LogFATAL("Failed to insert new record to first page");
          }
          rid = RecordID(page_->id(), slot_id);
        }

        BplusTree::ConnectLeaves(page_, page2);
        BplusTree::ConnectLeaves(tail_page, page);
        result.emplace_back(page_);
        result[0].record = plrecords_[0].shared_record();
        result.emplace_back(page2);
        result[1].record = plrecords_[group.start_index].shared_record();
        result.emplace_back(page);
        result[2].record = plrecords_[i].shared_record();
        result[0].rid = rid;
        return result;
      }
    }
  }
  else {  // Right half is larger.
    // Page 1
    result.emplace_back(page_);
    result[0].record = plrecords_[0].shared_record();

    // Page 2 - Try inserting right half to it.
    auto group = rgroups.at(re1.mid_index);
    uint32 index = group.start_index;
    auto page2 = tree_->AllocateNewPage(TREE_LEAVE);
    result.emplace_back(page2);
    result[1].record = plrecords_[index].shared_record();

    bool new_record_inserted = false;
    auto tail_page = page2;
    for (; index < group.start_index + group.num_records; index++) {
      int new_slot_id = record(index).InsertToRecordPage(tail_page);
      if (new_slot_id < 0) {
        // Append overflow page to middle page.
        tail_page = tree_->AppendOverflowPageTo(tail_page);
        new_slot_id = record(index).InsertToRecordPage(tail_page);
        if (new_slot_id < 0) {
          LogFATAL("Insert record to mid page's overflow page failed");
        }
      }
      int slot_id = plrecords_.at(index).slot_id();
      if (slot_id < 0) {
        new_record_inserted = true;
        rid = RecordID(tail_page->id(), new_slot_id);
      }
      else {
        page_->DeleteRecord(slot_id);
        if (file_type_ == INDEX_DATA) {
          rid_mutations->emplace_back(shared_record(index),
                                      RecordID(page_->id(), slot_id),
                                      RecordID(tail_page->id(), new_slot_id));
        }
      }
    }

    uint32 gindex = re1.mid_index + 1;
    index = rgroups[gindex].start_index;
    if (page2->Meta()->overflow_page() < 0) {
      // Page 2 is not overflowed. Continue inserting records to it.
      for (; gindex < rgroups.size(); gindex++) {
        if (!page2->PreCheckCanInsert(rgroups[gindex].num_records,
                                      rgroups[gindex].size)) {
          break;
        }
        // Add this record group to pag2.
        for (index = rgroups[gindex].start_index;
             index < rgroups[gindex].start_index + rgroups[gindex].num_records;
             index++) {
          int new_slot_id = record(index).InsertToRecordPage(page2);
          if (new_slot_id < 0) {
            LogFATAL("Failed to insert record to page2");
          }
          int slot_id = plrecords_.at(index).slot_id(); 
          if (slot_id < 0) {
            new_record_inserted = true;
            rid = RecordID(page2->id(), new_slot_id);
          }
          else {
            page_->DeleteRecord(slot_id);
            if (file_type_ == INDEX_DATA) {
              rid_mutations->emplace_back(shared_record(index),
                                          RecordID(page_->id(), slot_id),
                                          RecordID(page2->id(), new_slot_id));
            }
          }
        }
      }
    }
    // Page 3, maybe
    uint32 page3_start_index = rgroups[gindex].start_index;
    RecordPage* page3 = nullptr;
    if (gindex < rgroups.size()) {
      page3 = tree_->AllocateNewPage(TREE_LEAVE);
      for (; index < plrecords_.size(); index++) {
        int new_slot_id = record(index).InsertToRecordPage(page3);
        if (new_slot_id < 0) {
          LogFATAL("Failed to insert record to 3rd page");
        }
        int slot_id = plrecords_.at(index).slot_id();
        if (slot_id < 0) {
          new_record_inserted = true;
          rid = RecordID(page3->id(), new_slot_id);
        }
        else {
          page_->DeleteRecord(slot_id);
          if (file_type_ == INDEX_DATA) {
            rid_mutations->emplace_back(shared_record(index),
                                        RecordID(page_->id(), slot_id),
                                        RecordID(page3->id(), new_slot_id));
          }
        }
      }
    }

    if (!new_record_inserted) {
      int slot_id = new_record.InsertToRecordPage(page_);
      if (slot_id < 0) {
        LogFATAL("Failed to insert new record to first page");
      }
      rid = RecordID(page_->id(), slot_id);
    }
    // Return result
    BplusTree::ConnectLeaves(page_, page2);
    if (page3) {
      BplusTree::ConnectLeaves(tail_page, page3);
      result.emplace_back(page3);
      result[2].record = plrecords_[page3_start_index].shared_record();
    }
  }

  result[0].rid = rid;
  return result;
}

bool PageRecordsManager::UpdateRecordID(int slot_id, const RecordID& rid) {
  if (file_type_ != INDEX ||
      page_->meta().page_type() != TREE_LEAVE) {
    LogERROR("Can't update rid on page type other than (index_data, leave)");
    return false;
  }

  rid.DumpToMem(page_->Record(slot_id) + page_->RecordLength(slot_id) -
                rid.size());
  return true;
}

}