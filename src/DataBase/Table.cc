#include <sys/stat.h>
#include <string.h>

#include "Base/Log.h"
#include "Base/Utils.h"

#include "DataBase/Table.h"
#include "Storage/PageRecord_Common.h"
#include "Storage/PageRecordsManager.h"

namespace DataBase {

Table::Table(std::string name) : name_(name) {
  if (!LoadSchema()) {
    LogFATAL("Failed to load schema for table %s", name_.c_str());
  }

  BuildFieldIndexMap();
}

bool Table::LoadSchema() {
  std::string schema_filename = Storage::kDataDirectory + name_ +
                                ".schema.pb";

  struct stat stat_buf;
  int re = stat(schema_filename.c_str(), &stat_buf);
  if (re < 0) {
    LogERROR("Failed to stat schema file %s", schema_filename.c_str());
    return false;
  }

  int size = stat_buf.st_size;
  FILE* file = fopen(schema_filename.c_str(), "r");
  if (!file) {
    LogERROR("Failed to open schema file %s", schema_filename.c_str());
    return false;
  }
  // Read schema file.
  char buf[size];
  re = fread(buf, 1, size, file);
  if (re != size) {
    LogERROR("Read schema file %s error, expect %d bytes, actual %d",
             schema_filename.c_str(), size, re);
    return false;
  }
  fclose(file);
  // Parse TableSchema proto data.
  schema_.DeSerialize(buf, size);
  return true;
}

bool Table::BuildFieldIndexMap() {
  // Build field name --> field index map.
  for (auto& field: schema_.fields()) {
    field_index_map_[field.name()] = field.index();
  }

  // Determine the indexes of INDEX_DATA file. If primary key is specified in 
  // schema, use primary keys. If not, use index 0.
  for (auto index: schema_.primary_key_indexes()) {
    idata_indexes_.push_back(index);
  }
  if (idata_indexes_.empty()) {
    idata_indexes_.push_back(0);
  }

  return true;
}

Storage::BplusTree* Table::Tree(Storage::FileType file_type,
                                std::vector<int> key_indexes) {
  std::string filename = BplusTreeFileName(file_type, key_indexes);
  if (tree_map_.find(filename) != tree_map_.end()) {
    return tree_map_.at(filename).get();  
  }

  auto tree = std::make_shared<Storage::BplusTree>(
                   this, file_type, key_indexes);
  tree_map_.emplace(filename, tree);

  return nullptr;
}

std::string Table::BplusTreeFileName(Storage::FileType file_type,
                                     std::vector<int> key_indexes) {
  std::string filename = Storage::kDataDirectory + name_ + "(";
  for (int index: key_indexes) {
    filename += std::to_string(index) + "_";
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
  return idata_indexes_;
}

bool Table::IsDataFileKey(int index) const {
  if (idata_indexes_.size() > 1) {
    return false;
  }
  return idata_indexes_[0] == index;
}

bool Table::InitTrees() {
  // Data tree.
  auto filename = BplusTreeFileName(Storage::INDEX_DATA, idata_indexes_);
  auto tree = std::make_shared<Storage::BplusTree>(
                   this, Storage::INDEX_DATA, idata_indexes_, true);
  tree_map_.emplace(filename, tree);
  tree->SaveToDisk();

  // Index trees.
  for (auto field: schema_.fields()) {
    auto key_index = std::vector<int>{field.index()};
    if (IsDataFileKey(key_index[0])) {
      continue;
    }
    filename = BplusTreeFileName(Storage::INDEX, key_index);
    tree = std::make_shared<Storage::BplusTree>(
                     this, Storage::INDEX, key_index, true);
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

  // Sort the record based on idata_indexes_ (preferably primary key).
  Storage::PageRecordsManager::SortRecords(&records, idata_indexes_);

  // BulkLoad DataRecord.
  auto filename = BplusTreeFileName(Storage::INDEX_DATA, idata_indexes_);
  auto tree = std::make_shared<Storage::BplusTree>(
                   this, Storage::INDEX_DATA, idata_indexes_, true);
  tree_map_.emplace(filename, tree);

  std::vector<Storage::DataRecordWithRid> record_rids;
  for (auto& record: records) {
    if (!record->CheckFieldsType(schema_)) {
      return false;
    }
    if (!tree->BulkLoadRecord(record.get())) {
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
  for (auto field: schema_.fields()) {
    auto key_index = std::vector<int>{field.index()};
    if (IsDataFileKey(key_index[0])) {
      continue;
    }
    Storage::DataRecordWithRid::Sort(&record_rids, key_index);
    // printf("sort by index %d\n", key_index[0]);
    // for (auto r: record_rids) {
    //   r.record->Print();
    // }
    // printf("***********************\n");
    filename = BplusTreeFileName(Storage::INDEX, key_index);
    tree = std::make_shared<Storage::BplusTree>(
                     this, Storage::INDEX, key_index, true);
    tree_map_.emplace(filename, tree);

    Storage::IndexRecord irecord;
    for (auto& r: record_rids) {
      (reinterpret_cast<Storage::DataRecord*>(r.record.get()))
          ->ExtractKey(&irecord, key_index);
      irecord.set_rid(r.rid);
      // printf("insertng index record:\n");
      // irecord.Print();
      tree->BulkLoadRecord(&irecord);
    }
    tree->SaveToDisk();
  }

  return true;
}

bool Table::ValidateAllIndexRecords(int num_records) {
  // Get the data tree.
  auto data_tree = Tree(Storage::INDEX_DATA, idata_indexes_);
  CheckLogFATAL(data_tree, "Can't find data B+ tree");

  Storage::DataRecord drecord;
  drecord.InitRecordFields(schema_, std::vector<int>{0},
                           Storage::INDEX_DATA,
                           Storage::TREE_LEAVE);

  for (auto field: schema_.fields()) {
    auto key_index = std::vector<int>{field.index()};
    if (IsDataFileKey(key_index[0])) {
      continue;
    }
    //printf("Verifying index %d file\n", key_index[0]);
    // Get the index tree.
    auto tree = Tree(Storage::INDEX, key_index);
    CheckLogFATAL(tree, "Can't find B+ tree for index %d", key_index[0]);

    // Empty tree.
    if (tree->meta()->num_pages() == 1) {
      CheckLogFATAL(data_tree->meta()->num_pages() == 1,
                    "Foun empty index tree but data tree is non-empty");
      continue;
    }

    // An IndexRecord instance to load index records from tree leaves.
    Storage::IndexRecord irecord;
    irecord.InitRecordFields(schema_, key_index,
                             Storage::INDEX,
                             Storage::TREE_LEAVE);

    // Traverse all leaves for this index tree.
    auto leave = tree->FirstLeave();
    std::set<Storage::RecordID> rid_set;
    while (leave) {
      auto slot_directory = leave->Meta()->slot_directory();
      for (int slot_id = 0; slot_id < (int)slot_directory.size(); slot_id++) {
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
        if (Storage::RecordBase::CompareRecordWithKey(irecord, drecord,
                                                      key_index) != 0) {
          LogERROR("Compare index %d record failed with original data record",
                   key_index[0]);
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

  for (auto field: schema_.fields()) {
    auto key_index = std::vector<int>{field.index()};
    if (IsDataFileKey(key_index[0])) {
      continue;
    }
    // printf("************** Updating Rid for index tree %d ******************\n",
    //        key_index[0]);

    // Index Tree.
    auto tree = Tree(Storage::INDEX, key_index);
    if (!tree) {
      // (TODO): Skip or Fatal ?
      LogERROR("Can't find B+ tree for index %d", key_index[0]);
      continue;
    }
    tree->UpdateIndexRecords(rid_mutations);
  }

  return true;
}

bool Table::InsertRecord(const Storage::RecordBase* record) {
  if (record->type() != Storage::DATA_RECORD) {
    LogERROR("Can't insert record other than DataRecord");
    return false;
  }

  auto data_tree = Tree(Storage::INDEX_DATA, idata_indexes_);
  if (!data_tree) {
    LogERROR("Can't get DataRecord B+ tree");
    return false;
  }
  // Insert record to data B+ tree.
  std::vector<Storage::DataRecordRidMutation> rid_mutations;
  auto rid = data_tree->Do_InsertRecord(record, rid_mutations);
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
  for (auto field: schema_.fields()) {
    auto key_index = std::vector<int>{field.index()};
    if (IsDataFileKey(key_index[0])) {
      continue;
    }
    // Index Tree.
    auto index_tree = Tree(Storage::INDEX, key_index);
    Storage::IndexRecord irecord;
      (reinterpret_cast<const Storage::DataRecord*>(record))
        ->ExtractKey(&irecord, key_index);
    irecord.set_rid(rid);
    auto irid = index_tree->Do_InsertRecord(&irecord, rid_mutations);
    if (!irid.IsValid()) {
      LogFATAL("Failed to insert index record for the new record at index %d",
               key_index[0]);
    }
  }

  return true;;
}

int Table::DeleteRecord(const DeleteOp& op) {
  if (op.key_index < 0 || op.key_index >= schema_.fields_size()) {
    LogERROR("Invalid key_index %d for DeleteOp, expect in [%d, %d]",
             op.key_index, 0, schema_.fields_size() - 1);
    return -1;
  }

  DeleteResult data_delete_result;
  int pre_index = -1;
  if (op.op_cond == EQ) {
    if (IsDataFileKey(op.key_index)) {
      auto data_tree = Tree(Storage::INDEX_DATA, idata_indexes_);
      if (!data_tree) {
        LogERROR("Can't find DataRecord B+ tree");
        return -1;
      }
      data_tree->Do_DeleteRecordByKey(op.keys, &data_delete_result);
    }
    else {
      // Delete index records from speficied key index tree.
      pre_index = op.key_index;
      auto index_tree = Tree(Storage::INDEX,
                             std::vector<int>{op.key_index});
      DeleteResult index_delete_result;
      index_delete_result.del_mode = DataBase::DeleteResult::DEL_INDEX_PRE;
      index_tree->Do_DeleteRecordByKey(op.keys, &index_delete_result);

      // Delete data records from data tree.
      auto data_tree = Tree(Storage::INDEX_DATA, idata_indexes_);
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
    for (auto field: schema_.fields()) {
      auto key_index = std::vector<int>{field.index()};
      if (IsDataFileKey(key_index[0])) {
        continue;
      }
      // Index Tree.
      auto index_tree = Tree(Storage::INDEX, key_index);
      if (pre_index != key_index[0]) {
        index_tree->UpdateIndexRecords(data_delete_result.rid_deleted);
      }
      index_tree->UpdateIndexRecords(data_delete_result.rid_mutations);
    }
  }

  return data_delete_result.rid_deleted.size();
}

}  // namespace DATABASE
