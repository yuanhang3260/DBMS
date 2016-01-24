#ifndef DATABSE_TABLE_
#define DATABSE_TABLE_

#include <vector>
#include <map>
#include <memory>

#include "Schema/Record.h"
#include "Schema/DBTable_pb.h"
#include "Storage/BplusTree.h"

namespace DataBaseFiles {
  class BplusTree;
}

namespace DataBase {

class Table {
 public:
  Table() = default;
  Table(std::string name);
  FORBID_COPY_AND_ASSIGN(Table);

  DEFINE_ACCESSOR(name, std::string);
  DEFINE_ACCESSOR(idata_indexes, std::vector<int>);
  DEFINE_ACCESSOR_SMART_PTR(schema, Schema::TableSchema);

  std::string BplusTreeFileName(DataBaseFiles::FileType file_type,
                                std::vector<int> key_indexes);

  // Bulkload data records and generate all index files.
  bool PreLoadData(std::vector<std::shared_ptr<Schema::RecordBase>>& records);

  DataBaseFiles::BplusTree* Tree(std::string filename);

  bool IsDataFileKey(int index) const;

  bool ValidateAllIndexRecords(int num_records);

 private:
  bool BuildFieldIndexMap();
  bool LoadSchema();

  std::string name_;
  std::unique_ptr<Schema::TableSchema> schema_;

  // indexes of INDEX_DATA file.
  std::vector<int> idata_indexes_;
  // map: field name --> field index
  std::map<std::string, int> field_index_map_;
  // map: file name --> B+ tree
  using TreeMap =
          std::map<std::string, std::shared_ptr<DataBaseFiles::BplusTree>>;
  TreeMap tree_map_;
};

}  // namespace DATABASE


#endif  /* DATABSE_TABLE_ */
