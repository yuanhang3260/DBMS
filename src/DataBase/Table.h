#ifndef DATABSE_TABLE_
#define DATABSE_TABLE_

#include <vector>
#include <map>
#include <memory>

#include "Schema/DBTable_pb.h"
#include "Storage/Record.h"
#include "Storage/BplusTree.h"
#include "DataBase/Operation.h"

namespace Storage {
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

  const Schema::TableSchema& schema() const { return schema_; }

  std::string BplusTreeFileName(Storage::FileType file_type,
                                std::vector<int> key_indexes);

  // Bulkload data records and generate all index files.
  bool PreLoadData(std::vector<std::shared_ptr<Storage::RecordBase>>& records);

  Storage::BplusTree* Tree(Storage::FileType,
                                 std::vector<int> key_indexes);

  bool IsDataFileKey(int index) const;
  std::vector<int> DataTreeKey() const;

  bool InitTrees();

  bool ValidateAllIndexRecords(int num_records);

  bool UpdateIndexTrees(
           std::vector<Storage::DataRecordRidMutation>& rid_mutations);

  bool InsertRecord(const Storage::RecordBase* record);

  int DeleteRecord(const DeleteOp& op);

 private:
  bool BuildFieldIndexMap();
  bool LoadSchema();

  // Given a sorted list of DataRecordRidMutation groups, we further group them
  // into leave groups - that is, group them based on the index B+ tree leave 
  // reside in. It's an optimization so that we batch update/delete rids records
  // based on leaves. 
  class RidMutationLeaveGroup {
   public:
    RidMutationLeaveGroup(int start, int end, int id) :
        start_rgroup(start),
        end_rgroup(end),
        leave_id(id) {
    }
    int start_rgroup;
    int end_rgroup;
    int leave_id;
  };

  std::string name_;
  Schema::TableSchema schema_;

  // indexes of INDEX_DATA file.
  std::vector<int> idata_indexes_;
  // map: field name --> field index
  std::map<std::string, int> field_index_map_;
  // map: file name --> B+ tree
  using TreeMap =
          std::map<std::string, std::shared_ptr<Storage::BplusTree>>;
  TreeMap tree_map_;
};

}  // namespace DATABASE


#endif  /* DATABSE_TABLE_ */
