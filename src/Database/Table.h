#ifndef DATABSE_TABLE_
#define DATABSE_TABLE_

#include <vector>
#include <map>
#include <memory>

#include "Database/CatalogManager.h"
#include "Database/Operation.h"
#include "Storage/Record.h"
#include "Storage/BplusTree.h"

namespace DB {

class TableRecordIterator;

class Table {
 public:
  Table() = default;
  Table(const std::string& db_name, const std::string& name,
        TableInfoManager* table_m);

  DEFINE_ACCESSOR(name, std::string);

  const DB::TableInfo& schema() const { return table_m_->table_info(); }

  std::string BplusTreeFileName(Storage::FileType file_type,
                                const std::vector<uint32>& key_indexes);

  // Bulkload data records and generate all index files.
  bool PreLoadData(std::vector<std::shared_ptr<Storage::RecordBase>>& records);

  Storage::BplusTree* Tree(Storage::FileType, std::vector<uint32> key_indexes);
  Storage::BplusTree* DataTree();

  bool IsDataFileKey(const std::vector<uint32>& indexes) const;
  std::vector<uint32> DataTreeKey() const;
  bool HasIndex(const std::vector<uint32>& index) const;
  static std::string IndexStr(const std::vector<uint32>& index);

  bool InitTrees();

  bool ValidateAllIndexRecords(int num_records);

  // Operations
  int SearchRecords(const DB::SearchOp& op,
                    std::vector<std::shared_ptr<Storage::RecordBase>>* result);

  int ScanRecords(std::vector<std::shared_ptr<Storage::RecordBase>>* result);

  int ScanRecords(std::vector<std::shared_ptr<Storage::RecordBase>>* result,
                  const std::vector<uint32>& key_indexes);

  std::shared_ptr<TableRecordIterator> RecordIterator(const DB::SearchOp* op);

  bool InsertRecord(const Storage::RecordBase& record);

  int DeleteRecord(const DeleteOp& op);

 private:
  bool UpdateIndexTrees(
           std::vector<Storage::DataRecordRidMutation>& rid_mutations);

  Storage::DataRecord* CreateDataRecord();
  Storage::IndexRecord* CreateIndexRecord(const std::vector<uint32>& key_indexes);

  void FetchDataRecordsByRids(
      const std::vector<std::shared_ptr<Storage::RecordBase>>& irecords,
      std::vector<std::shared_ptr<Storage::RecordBase>>* drecords);

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

  std::string db_name_;
  std::string name_;
  DB::TableInfoManager* table_m_;

  // map: file name --> B+ tree
  using TreeMap =
          std::map<std::string, std::shared_ptr<Storage::BplusTree>>;
  TreeMap tree_map_;

  friend class TableRecordIterator;

  FORBID_COPY_AND_ASSIGN(Table);
};

struct TableRecordIterator {
  TableRecordIterator() = default;
  TableRecordIterator(Table* table_, const SearchOp* search_op_) :
    table(table_),
    search_op(search_op_) {}

  Table* table;
  const SearchOp* search_op;

  std::shared_ptr<Storage::TreeRecordIterator> tree_iter;

  std::vector<Storage::RecordID> rids;
  // This is for implementing "fake" iterating - prefetch all data records
  // and wait for iteration.
  std::vector<std::shared_ptr<Storage::RecordBase>> data_records;

  uint32 record_index = 0;

  bool ready = false;
  bool end = false;

  void Init();
  std::shared_ptr<Storage::RecordBase> GetNextRecord();
};

}  // namespace DATABASE


#endif  /* DATABSE_TABLE_ */
