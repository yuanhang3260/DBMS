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

DataBaseFiles::BplusTree* Table::Tree(std::string filename) {
  return tree_map_.at(filename).get();
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

bool Table::PreLoadData(
         std::vector<std::shared_ptr<Schema::RecordBase>>& records) {
  // Sort the record based on idata_indexes_ (preferably primary key).
  Schema::PageRecordsManager::SortRecords(records, idata_indexes_);

  // BulkLoad DataRecord.
  auto filename = BplusTreeFileName(DataBaseFiles::INDEX_DATA, idata_indexes_);
  auto tree = std::make_shared<DataBaseFiles::BplusTree>(
                   this, DataBaseFiles::INDEX_DATA, idata_indexes_);
  tree->CreateBplusTreeFile();
  tree_map_.emplace(filename, tree);

  std::vector<Schema::DataRecordWithRid> record_rids;
  for (auto& record: records) {
    if (!record->CheckFieldsType(schema_.get())) {
      return false;
    }
    auto rid = tree->BulkLoadRecord(record.get());
    record_rids.emplace_back(record, rid);
    //rid.Print();
  }
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
                     this, DataBaseFiles::INDEX, key_index);
    tree->CreateBplusTreeFile();
    tree_map_.emplace(filename, tree);

    Schema::IndexRecord irecord;
    for (auto& r: record_rids) {
      (reinterpret_cast<Schema::DataRecord*>(r.record.get()))
          ->ExtractKey(&irecord, key_index);
      irecord.set_rid(r.rid);
      tree->BulkLoadRecord(&irecord);
    }
    tree->SaveToDisk();
  }

  return true;
}

}  // namespace DATABASE
