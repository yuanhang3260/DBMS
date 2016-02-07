#include <sys/stat.h>
#include <string.h>

#include "Base/Log.h"
#include "Base/Utils.h"
#include "Schema/PageRecord_Common.h"
#include "Schema/PageRecordsManager.h"
#include "Table.h"

namespace DataBase {

Table::Table(std::string name) : name_(name) {
  if (!LoadSchema()) {
    LogFATAL("Failed to load schema for table %s", name_.c_str());
  }

  BuildFieldIndexMap();
}

bool Table::LoadSchema() {
  std::string schema_filename = DataBaseFiles::kDataDirectory + name_ +
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
  schema_.reset(new Schema::TableSchema());
  schema_->DeSerialize(buf, size);
  return true;
}

bool Table::BuildFieldIndexMap() {
  if (!schema_) {
    return false;
  }

  // Build field name --> field index map.
  for (auto& field: schema_->fields()) {
    field_index_map_[field.name()] = field.index();
  }

  // Determine the indexes of INDEX_DATA file. If primary key is specified in 
  // schema, use primary keys. If not, use index 0.
  for (auto index: schema_->primary_key_indexes()) {
    idata_indexes_.push_back(index);
  }
  if (idata_indexes_.empty()) {
    idata_indexes_.push_back(0);
  }

  return true;
}

DataBaseFiles::BplusTree* Table::Tree(DataBaseFiles::FileType file_type,
                                      std::vector<int> key_indexes) {
  std::string filename = BplusTreeFileName(file_type, key_indexes);
  if (tree_map_.find(filename) != tree_map_.end()) {
    return tree_map_.at(filename).get();  
  }

  auto tree = std::make_shared<DataBaseFiles::BplusTree>(
                   this, file_type, key_indexes);
  tree_map_.emplace(filename, tree);

  return nullptr;
}

std::string Table::BplusTreeFileName(DataBaseFiles::FileType file_type,
                                     std::vector<int> key_indexes) {
  std::string filename = DataBaseFiles::kDataDirectory + name_ + "(";
  for (int index: key_indexes) {
    filename += std::to_string(index) + "_";
  }
  filename += ")";

  if (file_type == DataBaseFiles::UNKNOWN_FILETYPE) {
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
  else if (file_type == DataBaseFiles::INDEX_DATA) {
    return filename + ".indata";
  }
  else if (file_type == DataBaseFiles::INDEX) {
    return filename + ".index";
  }
  LogFATAL("Can't generate B+ tree file name");
  return "";
}

bool Table::IsDataFileKey(int index) const {
  if (idata_indexes_.size() > 1) {
    return false;
  }
  return idata_indexes_[0] == index;
}

bool Table::InitTrees() {
  // Data tree.
  auto filename = BplusTreeFileName(DataBaseFiles::INDEX_DATA, idata_indexes_);
  auto tree = std::make_shared<DataBaseFiles::BplusTree>(
                   this, DataBaseFiles::INDEX_DATA, idata_indexes_, true);
  tree_map_.emplace(filename, tree);
  tree->SaveToDisk();

  // Index trees.
  for (auto field: schema_->fields()) {
    auto key_index = std::vector<int>{field.index()};
    if (IsDataFileKey(key_index[0])) {
      continue;
    }
    filename = BplusTreeFileName(DataBaseFiles::INDEX, key_index);
    tree = std::make_shared<DataBaseFiles::BplusTree>(
                     this, DataBaseFiles::INDEX, key_index, true);
    tree->CreateBplusTreeFile();
    tree_map_.emplace(filename, tree);
    tree->SaveToDisk();
  }

  return true;
}

bool Table::PreLoadData(
         std::vector<std::shared_ptr<Schema::RecordBase>>& records) {
  if (records.empty()) {
    //(TODO): create empty trees?
    LogERROR("Can't load empty records");
    return false;
  }

  // Sort the record based on idata_indexes_ (preferably primary key).
  Schema::PageRecordsManager::SortRecords(records, idata_indexes_);

  // BulkLoad DataRecord.
  auto filename = BplusTreeFileName(DataBaseFiles::INDEX_DATA, idata_indexes_);
  auto tree = std::make_shared<DataBaseFiles::BplusTree>(
                   this, DataBaseFiles::INDEX_DATA, idata_indexes_, true);
  tree_map_.emplace(filename, tree);

  std::vector<Schema::DataRecordWithRid> record_rids;
  for (auto& record: records) {
    if (!record->CheckFieldsType(schema_.get())) {
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
      Schema::RecordID rid(leave->id(), slot_id);
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
  for (auto field: schema_->fields()) {
    auto key_index = std::vector<int>{field.index()};
    if (IsDataFileKey(key_index[0])) {
      continue;
    }
    Schema::DataRecordWithRid::Sort(record_rids, key_index);
    // printf("sort by index %d\n", key_index[0]);
    // for (auto r: record_rids) {
    //   r.record->Print();
    // }
    // printf("***********************\n");
    filename = BplusTreeFileName(DataBaseFiles::INDEX, key_index);
    tree = std::make_shared<DataBaseFiles::BplusTree>(
                     this, DataBaseFiles::INDEX, key_index, true);
    tree_map_.emplace(filename, tree);

    Schema::IndexRecord irecord;
    for (auto& r: record_rids) {
      (reinterpret_cast<Schema::DataRecord*>(r.record.get()))
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
  auto data_tree = Tree(DataBaseFiles::INDEX_DATA, idata_indexes_);
  CheckLogFATAL(data_tree, "Can't find data B+ tree");

  Schema::DataRecord drecord;
  drecord.InitRecordFields(schema_.get(), std::vector<int>{0},
                           DataBaseFiles::INDEX_DATA,
                           DataBaseFiles::TREE_LEAVE);

  for (auto field: schema_->fields()) {
    auto key_index = std::vector<int>{field.index()};
    if (IsDataFileKey(key_index[0])) {
      continue;
    }
    //printf("Verifying index %d file\n", key_index[0]);
    // Get the index tree.
    auto tree = Tree(DataBaseFiles::INDEX, key_index);
    CheckLogFATAL(tree, "Can't find B+ tree for index %d", key_index[0]);

    // An IndexRecord instance to load index records from tree leaves.
    Schema::IndexRecord irecord;
    irecord.InitRecordFields(schema_.get(), key_index,
                               DataBaseFiles::INDEX,
                               DataBaseFiles::TREE_LEAVE);

    auto leave = tree->Page(1);
    std::set<Schema::RecordID> rid_set;
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
        if (Schema::RecordBase::CompareRecordWithKey(&irecord, &drecord,
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
    if ((int)rid_set.size() != num_records) {
      LogERROR("count %d index records, expect %d",
               (int)rid_set.size(), num_records);
      return false;
    }
  }
  return true;
}

bool Table::UpdateIndexRecords(
                std::vector<Schema::DataRecordRidMutation>& rid_mutations) {
  if (rid_mutations.empty()) {
    return true;
  }

  for (auto field: schema_->fields()) {
    auto key_index = std::vector<int>{field.index()};
    if (IsDataFileKey(key_index[0])) {
      continue;
    }
    // printf("************** Updating Rid for index %d *******************\n",
    //        key_index[0]);

    // Index Tree.
    auto tree = Tree(DataBaseFiles::INDEX, key_index);
    if (!tree) {
      // (TODO): Skip or Fatal ?
      LogERROR("Can't find B+ tree for index %d", key_index[0]);
      continue;
    }
    // Sort DataRecordRidMutation list.
    Schema::DataRecordRidMutation::Sort(rid_mutations, key_index);

    std::vector<Schema::RecordGroup> rgroups;
    GroupDataRecordRidMutations(rid_mutations, key_index, &rgroups);

    std::vector<RidMutationLeaveGroup> ridmlgroup;
    GroupRidMutationLeaveGroups(rid_mutations, rgroups, tree, key_index,
                                &ridmlgroup);

    for (const auto& rmlgroup: ridmlgroup) {
      auto crt_leave = tree->Page(rmlgroup.leave_id);
      CheckLogFATAL(crt_leave, "Failed to get leave of RidMutationLeaveGroup");
      std::shared_ptr<Schema::PageRecordsManager> prmanager(
          new Schema::PageRecordsManager(
              crt_leave, schema_.get(), key_index,
              DataBaseFiles::INDEX, DataBaseFiles::TREE_LEAVE)
      );
      int i = 0;
      for (int rgroup_index = rmlgroup.start_rgroup;
           rgroup_index <= rmlgroup.end_rgroup;
           rgroup_index++) {
        auto group = rgroups[rgroup_index];
        auto crt_record = rid_mutations[group.start_index].record;
        Schema::RecordBase key;
        (reinterpret_cast<const Schema::DataRecord*>(crt_record.get()))
          ->ExtractKey(&key, key_index);

        int num_rids_updated = 0;
        while (crt_leave) {
          for (; i < (int)prmanager->NumRecords(); i++) {
            int re = prmanager->CompareRecordWithKey(&key,prmanager->Record(i));
            //printf("scanning index record: re %d, slot %d\n",
            //       re, prmanager.RecordSlotID(i));
            //prmanager.Record(i)->Print();
            if (re < 0) {
              break;
            } else if (re > 0) {
              continue;
            }
            Schema::RecordID rid = reinterpret_cast<Schema::IndexRecord*>(
                                       prmanager->Record(i))->rid();
            //printf("scanning old rid\n");
            //rid.Print();
            for (int rid_m_index = group.start_index;
                 rid_m_index < group.start_index + group.num_records;
                 rid_m_index++) {
              if (rid == rid_mutations[rid_m_index].old_rid) {
                // Update the rid for the IndexRecord.
                prmanager->UpdateRecordID(prmanager->RecordSlotID(i),
                                          rid_mutations[rid_m_index].new_rid);
                num_rids_updated++;
              }
            }
          }

          // This is not overflow page because it has more than 1 keys groups.
          if (rmlgroup.start_rgroup != rmlgroup.end_rgroup) {
            break;
          }
          // Otherwise we might need to continue searching overflow pages.
          crt_leave = tree->Page(crt_leave->Meta()->overflow_page());
          if (crt_leave) {
            prmanager.reset(new Schema::PageRecordsManager(
                crt_leave, schema_.get(), key_index,
                DataBaseFiles::INDEX, DataBaseFiles::TREE_LEAVE)
            );
            i = 0;
          }
        }
        if (num_rids_updated != group.num_records) {
          LogERROR("Updated %d number of rids, expect %d",
                   num_rids_updated, group.num_records);
          key.Print();
          return false;
        }
      }
      // printf("---------- udpate for key ----------: \n");
      // key.Print();
      // printf("leave id = %d\n", leave->id());
    }
  }

  return true;
}

void Table::GroupDataRecordRidMutations(
                std::vector<Schema::DataRecordRidMutation>& rid_mutations,
                std::vector<int> key_index,
                std::vector<Schema::RecordGroup>* rgroups) {
  if (rid_mutations.empty()) {
    return;
  }

  auto crt_record = rid_mutations[0].record;
  int crt_start = 0;
  int num_records = 0;
  for (int i = 0; i < (int)rid_mutations.size(); i++) {
    if (Schema::RecordBase::CompareRecordsBasedOnIndex(
            crt_record.get(), rid_mutations[i].record.get(), key_index) == 0) {
      num_records++;
    }
    else {
      rgroups->push_back(Schema::RecordGroup(crt_start, num_records, -1));
      crt_start = i;
      num_records = 1;
      crt_record = rid_mutations[crt_start].record;
    }
  }
  rgroups->push_back(Schema::RecordGroup(crt_start, num_records, -1));
}

void Table::GroupRidMutationLeaveGroups(
                std::vector<Schema::DataRecordRidMutation>& rid_mutations,
                std::vector<Schema::RecordGroup>& rgroups,
                DataBaseFiles::BplusTree* tree,
                std::vector<int> key_index,
                std::vector<RidMutationLeaveGroup>* ridmlgroup) {
  if (rgroups.empty()) {
    return;
  }

  int crt_leave_id = -1;
  int crt_start = -1;
  int crt_end = -1;
  for (int i = 0; i < (int)rgroups.size(); i++) {
    auto crt_record = rid_mutations[rgroups[i].start_index].record;
    Schema::RecordBase key;
    (reinterpret_cast<const Schema::DataRecord*>(crt_record.get()))
      ->ExtractKey(&key, key_index);

    auto leave = tree->SearchByKey(&key);
    CheckLogFATAL(leave, "Failed to search key at index %d", key_index[0]);
    if (leave->id() == crt_leave_id) {
      crt_end = i;
    }
    else {
      // new leave group.
      if (i > 0) {
        ridmlgroup->emplace_back(crt_start, crt_end, crt_leave_id);
      }
      crt_start = i;
      crt_end = i;
      crt_leave_id = leave->id();
    }
  }
  ridmlgroup->emplace_back(crt_start, crt_end, crt_leave_id);

  // printf("rid_mutations size = %d\n", rid_mutations.size());
  // for (const auto& i : rgroups) {
  //   printf("%d, %d\n", i.start_index, i.num_records);
  // }
  // printf("rgroups size = %d\n", rgroups.size());
  // for (const auto& i: *ridmlgroup) {
  //   printf("(%d, %d), id = %d\n", i.start_rgroup, i.end_rgroup, i.leave_id);
  //   for (int ii = i.start_rgroup; ii <= i.end_rgroup; ii++) {
  //     printf("  %d\n", rgroups[ii].start_index);
  //   }
  // }
}

bool Table::InsertRecord(const Schema::RecordBase* record) {
  if (record->type() != Schema::DATA_RECORD) {
    LogERROR("Can't insert record other than DataRecord");
    return false;
  }

  auto data_tree = Tree(DataBaseFiles::INDEX_DATA, idata_indexes_);
  if (!data_tree) {
    LogERROR("Can't get DataRecord B+ tree");
    return false;
  }
  // Insert record to data B+ tree.
  std::vector<Schema::DataRecordRidMutation> rid_mutations;
  auto rid = data_tree->Do_InsertRecord(record, rid_mutations);
  if (!rid.IsValid()) {
    LogFATAL("Failed to insert data record");
  }

  // Update changed RecordIDs of existing records in the B+ tree.
  if (!UpdateIndexRecords(rid_mutations)) {
    LogFATAL("Failed to Update IndexRecords");
  }

  if (!Schema::DataRecordRidMutation::ValidityCheck(rid_mutations)) {
    LogFATAL("Got invalid rid_mutations list");
  }

  // Insert IndexRecord of the new record to index files.
  rid_mutations.clear();
  for (auto field: schema_->fields()) {
    auto key_index = std::vector<int>{field.index()};
    if (IsDataFileKey(key_index[0])) {
      continue;
    }
    // Index Tree.
    auto index_tree = Tree(DataBaseFiles::INDEX, key_index);
    Schema::IndexRecord irecord;
      (reinterpret_cast<const Schema::DataRecord*>(record))
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

bool Table::DeleteRecord(const DeleteOp* op) {
  if (!op) {
    LogERROR("Nullptr DeleteOp");
    return false;
  }

  if (op->key_index < 0 || op->key_index >= schema_->fields_size()) {
    LogERROR("Invalid key_index %d for DeleteOp, expect in [%d, %d]",
             op->key_index, 0, schema_->fields_size());
    return false;
  }

  DeleteResult delete_result;
  if (op->op_cond == EQ) {
    if (IsDataFileKey(op->key_index)) {
      auto data_tree = Tree(DataBaseFiles::INDEX_DATA, idata_indexes_);
      if (!data_tree) {
        LogERROR("Can't get DataRecord B+ tree");
        return false;
      }
      data_tree->Do_DeleteRecordByKey(op->keys, &delete_result);
    }
    else {
      auto index_tree = Tree(DataBaseFiles::INDEX,
                             std::vector<int>{op->key_index});
      index_tree->Do_DeleteRecordByKey(op->keys, &delete_result);
      // TODO: Sort rids and delete data records from data tree.
    }
  }

  return true;
}

}  // namespace DATABASE
