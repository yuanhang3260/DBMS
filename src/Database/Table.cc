#include <sys/stat.h>
#include <string.h>

#include "Base/Log.h"
#include "Base/MacroUtils.h"
#include "Base/Utils.h"
#include "Strings/Utils.h"

#include "Database/Table.h"
#include "Storage/PageRecord_Common.h"
#include "Storage/PageRecordsManager.h"

namespace DB {

namespace {

using Storage::RecordBase;
using Storage::DataRecord;
using Storage::IndexRecord;
using Storage::RecordID;

}

Table::Table(const std::string& db_name, const std::string& name,
             TableInfoManager* table_m) :
    db_name_(db_name),
    name_(name),
    table_m_(table_m) {
}

Storage::BplusTree* Table::Tree(Storage::FileType file_type,
                                std::vector<int> key_index) {
  CHECK(HasIndex(key_index),
        Strings::StrCat("Index ", IndexStr(key_index), " not found"));

  std::string filename = BplusTreeFileName(file_type, key_index);
  if (tree_map_.find(filename) != tree_map_.end()) {
    return tree_map_.at(filename).get();
  }

  return nullptr;
}

Storage::BplusTree* Table::DataTree() {
  return Tree(Storage::INDEX_DATA, DataTreeKey());
}

bool Table::HasIndex(const std::vector<int32>& index) const {
  return table_m_->HasIndex(index);
}

std::string Table::IndexStr(const std::vector<int32>& index) {
  std::vector<std::string> index_str;
  for (const auto& i : index) {
    index_str.push_back(std::to_string(i));
  }
  return Strings::StrCat("{", Strings::Join(index_str, ","), "}");
}

std::string Table::BplusTreeFileName(Storage::FileType file_type,
                                     const std::vector<int>& key_index) {
  // B+ tree name is //data/${db_name}/${table_name}(key).index
  std::string filename =
      Strings::StrCat(Storage::kDataDirectory, db_name_, "/", name_, "(");
  for (int index : key_index) {
    filename += schema().fields(index).name() + "_";
  }
  filename += ")";

  if (file_type == Storage::UNKNOWN_FILETYPE) {
    // Check file type.
    // Data file.
    std::string fullname = filename + ".indata";
    if (access(fullname.c_str(), F_OK) != -1) {
      return fullname;
    }

    // Index file.
    fullname = filename + ".index";
    if (access(fullname.c_str(), F_OK) != -1) {
      return fullname;
    }
  }
  else if (file_type == Storage::INDEX_DATA) {
    return filename + ".indata";
  }
  else if (file_type == Storage::INDEX) {
    return filename + ".index";
  }
  LogFATAL("Can't generate B+ tree file name");
  return "";
}

std::vector<int> Table::DataTreeKey() const {
  return table_m_->PrimaryIndex();
}

bool Table::IsDataFileKey(const std::vector<int>& index) const {
  return table_m_->IsPrimaryIndex(index);
}

bool Table::InitTrees() {
  // Data tree.
  auto filename = BplusTreeFileName(Storage::INDEX_DATA, DataTreeKey());
  auto tree = std::make_shared<Storage::BplusTree>(
                   this, Storage::INDEX_DATA, DataTreeKey(), true);
  tree_map_.emplace(filename, tree);
  tree->SaveToDisk();

  // Index trees.
  for (auto field: schema().fields()) {
    std::vector<int> key_indexes{field.index()};
    if (IsDataFileKey(key_indexes)) {
      continue;
    }
    filename = BplusTreeFileName(Storage::INDEX, key_indexes);
    tree = std::make_shared<Storage::BplusTree>(
                     this, Storage::INDEX, key_indexes, true);
    tree->CreateBplusTreeFile();
    tree_map_.emplace(filename, tree);
    tree->SaveToDisk();
  }

  return true;
}

bool Table::PreLoadData(
         std::vector<std::shared_ptr<Storage::RecordBase>>& records) {
  if (records.empty()) {
    //(TODO): create empty trees?
    LogERROR("Can't load empty records");
    return false;
  }

  tree_map_.clear();

  // Sort the record based on DataTreeKey() (preferably primary key).
  Storage::PageRecordsManager::SortRecords(&records, DataTreeKey());

  // BulkLoad DataRecord.
  auto filename = BplusTreeFileName(Storage::INDEX_DATA, DataTreeKey());
  auto tree = std::make_shared<Storage::BplusTree>(
                   this, Storage::INDEX_DATA, DataTreeKey(), true);
  tree_map_.emplace(filename, tree);

  std::vector<Storage::DataRecordWithRid> record_rids;
  for (auto& record: records) {
    if (!record->CheckFieldsType(schema())) {
      return false;
    }
    if (!tree->BulkLoadRecord(*record)) {
      LogERROR("Failed to bulk load record, stop");
      return false;
    }
  }
  // Collect RecordIDs of all DataRecords, from first leave (page 1). We must
  // wait for all reocrds are loaded before collecting rids because previously
  // loaded records may be moved to other leaves if boundary duplication
  // happens in bulkloading. See CheckBoundaryDuplication() in class BplusTree.
  auto leave = tree->Page(1);
  int total_record_num = 0;
  while (leave) {
    auto slot_directory = leave->Meta()->slot_directory();
    for (int slot_id = 0; slot_id < (int)slot_directory.size(); slot_id++) {
      if (slot_directory[slot_id].offset() < 0) {
        continue;
      }
      Storage::RecordID rid(leave->id(), slot_id);
      record_rids.emplace_back(records.at(total_record_num), rid);
      total_record_num++;
    }
    leave = tree->Page(leave->Meta()->next_page());
  }
  CheckLogFATAL(total_record_num == (int)records.size(),
                "leave scan collected %d rids, expect %d",
                total_record_num, (int)records.size());
  tree->SaveToDisk();

  // Generate Index B+ tree files.
  for (const auto& index: table_m_->table_info().indexes()) {
    std::vector<int> key_index = TableInfoManager::MakeIndex(index);
    if (IsDataFileKey(key_index)) {
      continue;
    }
    Storage::DataRecordWithRid::Sort(&record_rids, key_index);

    filename = BplusTreeFileName(Storage::INDEX, key_index);
    tree = std::make_shared<Storage::BplusTree>(
                     this, Storage::INDEX, key_index, true);
    tree_map_.emplace(filename, tree);

    Storage::IndexRecord irecord;
    for (auto& r: record_rids) {
      (dynamic_cast<Storage::DataRecord*>(r.record.get()))
          ->ExtractKey(&irecord, key_index);
      irecord.set_rid(r.rid);
      // printf("insertng index record:\n");
      // irecord.Print();
      tree->BulkLoadRecord(irecord);
    }
    tree->SaveToDisk();
  }

  return true;
}

DataRecord* Table::CreateDataRecord() {
  DataRecord* drecord = new DataRecord();
  std::vector<int> indexes(schema().fields_size());
  for (uint32 i = 0; i < indexes.size(); i++) {
    indexes[i] = i;
  }
  drecord->InitRecordFields(schema(), indexes);
  return drecord;
}

IndexRecord* Table::CreateIndexRecord(const std::vector<int>& key_indexes) {
  IndexRecord* irecord = new IndexRecord();
  irecord->InitRecordFields(schema(), key_indexes);
  return irecord;
}

void Table::FetchDataRecordsByRids(
    const std::vector<std::shared_ptr<RecordBase>>& irecords,
    std::vector<std::shared_ptr<RecordBase>>* drecords) {
  std::vector<RecordID> rids;
  for (const auto& irecord : irecords) {
    rids.push_back((dynamic_cast<const IndexRecord&>(*irecord)).rid());
  }
  std::sort(rids.begin(), rids.end());
  for (const auto& rid : rids) {
    DataRecord* drecord = CreateDataRecord();
    SANITY_CHECK(drecord->LoadFromMem(DataTree()->Record(rid)) >= 0,
                 "Load data record failed");
    drecords->push_back(std::shared_ptr<RecordBase>(drecord));
  }
}

int Table::SearchRecords(const DB::SearchOp& op,
                         std::vector<std::shared_ptr<RecordBase>>* result) {
  auto data_tree = DataTree();
  if (IsDataFileKey(op.field_indexes)) {
    return data_tree->SearchRecords(*op.key, result);
  } else {
    std::vector<std::shared_ptr<RecordBase>> irecords;
    Tree(Storage::INDEX, op.field_indexes)->SearchRecords(*op.key, &irecords);
    FetchDataRecordsByRids(irecords, result);
  }
  return result->size();
}

int Table::RangeSearchRecords(const DB::RangeSearchOp& op,
                              std::vector<std::shared_ptr<RecordBase>>* result){
  // Compare left and right key. If they form an empty set, return 0.
  if (op.left_key && op.right_key) {
    int re = RecordBase::CompareRecords(*op.left_key, *op.right_key);
    if (re > 0 || (re == 0 && (op.left_open || op.right_open))) {
      LogINFO("Left and right keys produce empty range");
      return 0;
    }
  }

  auto data_tree = DataTree();
  if (IsDataFileKey(op.field_indexes)) {
    return data_tree->RangeSearchRecords(op, result);
  } else {
    std::vector<std::shared_ptr<RecordBase>> irecords;
    Tree(Storage::INDEX, op.field_indexes)->RangeSearchRecords(op, &irecords);
    FetchDataRecordsByRids(irecords, result);
  }
  return result->size();
}

int Table::ScanRecords(std::vector<std::shared_ptr<RecordBase>>* result) {
  return ScanRecords(result, DataTreeKey());
}

int Table::ScanRecords(std::vector<std::shared_ptr<RecordBase>>* result,
                       const std::vector<int>& key_index) {
  Storage::BplusTree* tree;
  if (IsDataFileKey(key_index)) {
    tree = DataTree();
  } else {
    tree = Tree(Storage::INDEX, key_index);
  }
  return tree->ScanRecords(result);
}

bool Table::ValidateAllIndexRecords(int num_records) {
  // Get the data tree.
  auto data_tree = Tree(Storage::INDEX_DATA, DataTreeKey());
  CheckLogFATAL(data_tree, "Can't find data B+ tree");

  Storage::DataRecord drecord;
  std::vector<int> indexes(schema().fields_size());
  for (uint32 i = 0; i < indexes.size(); i++) {
    indexes[i] = i;
  }
  drecord.InitRecordFields(schema(), indexes);

  for (const auto& index : table_m_->table_info().indexes()) {
    std::vector<int> key_index = TableInfoManager::MakeIndex(index);
    if (IsDataFileKey(key_index)) {
      continue;
    }

    // Get the index tree.
    auto tree = Tree(Storage::INDEX, key_index);

    // Empty tree.
    if (tree->meta()->num_pages() == 1) {
      CheckLogFATAL(data_tree->meta()->num_pages() == 1,
                    "Foun empty index tree but data tree is non-empty");
      continue;
    }

    // An IndexRecord instance to load index records from tree leaves.
    Storage::IndexRecord irecord;
    irecord.InitRecordFields(schema(), key_index);

    // Traverse all leaves for this index tree.
    auto leave = tree->FirstLeave();
    std::set<Storage::RecordID> rid_set;
    while (leave) {
      auto slot_directory = leave->Meta()->slot_directory();
      for (uint32 slot_id = 0; slot_id < slot_directory.size(); slot_id++) {
        if (slot_directory[slot_id].offset() < 0) {
          continue;
        }
        // Load this IndexRecord.
        CheckLogFATAL(irecord.LoadFromMem(leave->Record(slot_id)) >= 0,
                      "Load index record failed");
        auto rid = irecord.rid();
        // Check no duplicated RecordID.
        CheckLogFATAL(rid_set.find(rid) == rid_set.end(), "duplicated rid");
        rid_set.insert(rid);
        // Load the DataRecord pointed by this rid.
        CheckLogFATAL(drecord.LoadFromMem(data_tree->Record(rid)) >= 0,
                      "Load data record failed");
        if (Storage::RecordBase::CompareRecordWithKey(drecord, irecord,
                                                      key_index) != 0) {
          LogERROR("Compare index %d record failed with original data record",
                   IndexStr(key_index).c_str());
          drecord.Print();
          irecord.Print();
          printf("*********\n");
          return false;
        }
      }
      leave = tree->Page(leave->Meta()->next_page());
    }
    if (num_records < 0) {
      num_records = rid_set.size();
    }
    if ((int)rid_set.size() != num_records) {
      LogERROR("count %d index records, expect %d",
               (int)rid_set.size(), num_records);
      return false;
    }
  }
  return true;
}

bool Table::UpdateIndexTrees(
                std::vector<Storage::DataRecordRidMutation>& rid_mutations) {
  if (rid_mutations.empty()) {
    return true;
  }

  for (const auto& index : table_m_->table_info().indexes()) {
    std::vector<int> key_index = TableInfoManager::MakeIndex(index);
    if (IsDataFileKey(key_index)) {
      continue;
    }

    // Index Tree.
    auto tree = Tree(Storage::INDEX, key_index);
    tree->UpdateIndexRecords(rid_mutations);
  }

  return true;
}

bool Table::InsertRecord(const Storage::RecordBase& record) {
  if (record.type() != Storage::DATA_RECORD) {
    LogERROR("Can't insert record other than DataRecord");
    return false;
  }

  auto data_tree = Tree(Storage::INDEX_DATA, DataTreeKey());
  if (!data_tree) {
    LogERROR("Can't get DataRecord B+ tree");
    return false;
  }
  // Insert record to data B+ tree.
  std::vector<Storage::DataRecordRidMutation> rid_mutations;
  auto rid = data_tree->Do_InsertRecord(record, &rid_mutations);
  if (!rid.IsValid()) {
    LogFATAL("Failed to insert data record");
  }

  // Update changed RecordIDs of existing records in the B+ tree.
  if (!UpdateIndexTrees(rid_mutations)) {
    LogFATAL("Failed to Update IndexRecords");
  }

  if (!Storage::DataRecordRidMutation::ValidityCheck(rid_mutations)) {
    LogFATAL("Got invalid rid_mutations list");
  }

  // Insert IndexRecord of the new record to index files.
  rid_mutations.clear();
  for (const auto& index : table_m_->table_info().indexes()) {
    std::vector<int> key_index = TableInfoManager::MakeIndex(index);
    if (IsDataFileKey(key_index)) {
      continue;
    }
    // Index Tree.
    auto index_tree = Tree(Storage::INDEX, key_index);
    Storage::IndexRecord irecord;
    (dynamic_cast<const Storage::DataRecord&>(record))
        .ExtractKey(&irecord, key_index);
    irecord.set_rid(rid);
    auto irid = index_tree->Do_InsertRecord(irecord, &rid_mutations);
    if (!irid.IsValid()) {
      LogFATAL("Failed to insert index record for the new record at index %d",
               IndexStr(key_index).c_str());
    }
  }

  return true;;
}

int Table::DeleteRecord(const DeleteOp& op) {
  DeleteResult data_delete_result;
  std::vector<int> pre_index;
  if (op.op_cond == Query::EQUAL) {
    if (IsDataFileKey(op.field_indexes)) {
      auto data_tree = Tree(Storage::INDEX_DATA, DataTreeKey());
      if (!data_tree) {
        LogERROR("Can't find DataRecord B+ tree");
        return -1;
      }
      data_tree->Do_DeleteRecordByKey(op.keys, &data_delete_result);
    }
    else {
      // Delete index records from speficied key index tree.
      pre_index = op.field_indexes;
      auto index_tree = Tree(Storage::INDEX, op.field_indexes);
      DeleteResult index_delete_result;
      index_delete_result.del_mode = DB::DeleteResult::DEL_INDEX_PRE;
      index_tree->Do_DeleteRecordByKey(op.keys, &index_delete_result);

      // Delete data records from data tree.
      auto data_tree = Tree(Storage::INDEX_DATA, DataTreeKey());
      data_tree->Do_DeleteRecordByRecordID(index_delete_result,
                                           &data_delete_result);

      CheckLogFATAL((data_delete_result.rid_deleted.size() ==
                     index_delete_result.rid_deleted.size()),
                    "data tree deleted un-maching number of records");
    }

    if (data_delete_result.rid_deleted.empty()) {
      return 0;
    }

    // Update and delete index records in all index trees.
    printf("Begin updating index trees\n");
    for (const auto& index : table_m_->table_info().indexes()) {
      std::vector<int> key_index = TableInfoManager::MakeIndex(index);
      if (IsDataFileKey(key_index)) {
        continue;
      }
      // Index Tree.
      auto index_tree = Tree(Storage::INDEX, key_index);
      if (pre_index != key_index) {
        index_tree->UpdateIndexRecords(data_delete_result.rid_deleted);
      }
      index_tree->UpdateIndexRecords(data_delete_result.rid_mutations);
    }
  }

  return data_delete_result.rid_deleted.size();
}

}  // namespace DB
