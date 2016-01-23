#ifndef STORAGE_BPLUS_TREE_
#define STORAGE_BPLUS_TREE_

#include <memory>
#include <map>
#include <queue>

#include "Schema/Record.h"
#include "Schema/DBTable_pb.h"
#include "Schema/PageRecord_Common.h"
#include "DataBase/Table.h"
#include "PageBase.h"
#include "RecordPage.h"

namespace Schema {
  class PageRecordsManager;
}

namespace DataBase {
  class Table;
}

namespace DataBaseFiles {

class BplusTreeTest;

class BplusTreeHeaderPage: public HeaderPage {
 public:
  BplusTreeHeaderPage() = default;
  BplusTreeHeaderPage(FileType file_type);
  BplusTreeHeaderPage(FILE* file);
  BplusTreeHeaderPage(FILE* file, FileType file_type);

  DEFINE_ACCESSOR(root_page, int);
  DEFINE_ACCESSOR(num_leaves, int);
  DEFINE_INCREMENTOR_DECREMENTOR(num_leaves, int);
  DEFINE_ACCESSOR(depth, int);
  DEFINE_INCREMENTOR_DECREMENTOR(depth, int);
  DEFINE_ACCESSOR(next_id, int);
  DEFINE_INCREMENTOR_DECREMENTOR(next_id, int);

  // Dump header page to memory.
  bool DumpToMem(byte* buf) const override;
  // Parse header page from memory.
  bool ParseFromMem(const byte* buf) override;

  // Save header page to disk.
  bool SaveToDisk() const override;
  // Load header page from disk.
  bool LoadFromDisk() override;

 private:
  bool ConsistencyCheck(const char* op) const;

  int root_page_ = -1;
  int num_leaves_ = 0;
  int depth_ = 0;
  int next_id_ = 1;
};


class BplusTree {
 public:
  BplusTree() = default;
  // Contruct B+ tree from an existing file.
  BplusTree(DataBase::Table* table,
            FileType file_type,
            std::vector<int> key_indexes);

  // Destructor
  virtual ~BplusTree();

  // Accessors
  DEFINE_ACCESSOR(file, FILE*);
  DEFINE_ACCESSOR(table, DataBase::Table*);
  DEFINE_ACCESSOR(key_indexes, std::vector<int>);
  DEFINE_ACCESSOR_ENUM(file_type, FileType);
  BplusTreeHeaderPage* meta() { return header_.get(); }
  RecordPage* root();
  Schema::TableSchema* schema() const;

  // Load B+ tree from file.
  bool LoadTree();

  // Create the B+ tree file;
  bool CreateBplusTreeFile();

  // Load and save the B+ tree
  bool SaveToDisk() const;
  bool LoadFromDisk();

  // Produce indexes to compare records. For INDEX-DATA file, they are key
  // indexes; For INDEX file, they are [0, 1, 2 ... (num_keys-1)]
  std::vector<int> IndexesToCompareLeaveRecords() const;

  bool BlukLoadInsertRecordToLeave(RecordPage* leave,
                                   Schema::RecordBase* record);

  // BulkLoading data. Input is a list of Record consisting of various fields
  // defined in Schema/DataTypes. These records must have been sorted based
  // on a key which consists of fields from Record speficed from key_indexes.
  bool BulkLoad(std::vector<std::shared_ptr<Schema::RecordBase>>& records);
  Schema::RecordID BulkLoadRecord(Schema::RecordBase* record);

  // Validity check for the B+ tree.
  bool ValidityCheck();

  // Serach records by a key. Returns all records that matches this key.
  int SearchByKey(const Schema::RecordBase* key,
                  std::vector<std::shared_ptr<Schema::RecordBase>>* result);

  // Insert a data record to the B+ tree.
  bool Do_InsertRecord(const Schema::RecordBase* record);

  // Allocate a new page in bulkloading.
  RecordPage* AllocateNewPage(PageType page_type);

  // Append a overflow page to an existing page.
  RecordPage* AppendOverflowPageTo(RecordPage* page);

  // Connect two leaves.
  static bool ConnectLeaves(RecordPage* page1, RecordPage* page2);

  friend class BplusTreeTest;

 protected:
  // Generate B+ tree file name, based on table name, key indexes and file type.
  std::string GenerateBplusTreeFileName(FileType file_type);
  // Load header page from disk.
  bool LoadHeaderPage();
  // Load root node from disk.
  bool LoadRootNode();
  // Load table schema from schema file, which is a serialized protocal buffer
  // raw file. It saves message TableSchema defined in Schema/DBTable.proto.
  bool LoadSchema();

  // Insert a record to a leave node.
  bool InsertRecordToLeave(const Schema::DataRecord* record);
  // Add first leave to empty tree.
  bool AddFirstLeaveToTree(RecordPage* leave);
  // Insert a page to a parent node.
  bool AddLeaveToTree(RecordPage* leave, Schema::TreeNodeRecord* tn_record);
  // Insert a new TreeNodeRecord to tree node page.
  bool InsertTreeNodeRecord(Schema::TreeNodeRecord* tn_record,
                            RecordPage* tn_page);

  // Used in bulk loading. We don't allow records with same key are spread
  // to 2 successive pages. Same keys must be merged into a single page and
  // possibly, into overflow pages if there are many duplicates.
  // More specifically, below is not allowd:
  //
  //                        parent node
  //                      | 1   5   X....|
  //              ____________|   |_______
  //             |                        |
  //         leave N                   leave N+1
  // | 1, 2, 3, 4, 5, 5, 5 |        | 5, 5, 5, ... |
  //
  // Because this violates that all records of leave N < right boundary given
  // by its parent (which is 5), where the range is [1, 5). It can easily
  // happen in bulkloading. We need to move all 5 in leave N to leave N + 1
  // before the first 5 is inserted into leave N + 1.
  bool CheckBoundaryDuplication(Schema::RecordBase* record);

  // Fetch a page, either from page map or load it from disk.
  RecordPage* FetchPage(int page_id);
  // Save page to disk and de-cache it from page map.
  bool CheckoutPage(int page_id, bool write_to_disk=true);

  // Consistency check for a tree node. It loads and sorts all TreeNodeRecords
  // from this tree node, and verify each child node within the interval of
  // every two tree node record boundaries.
  bool CheckTreeNodeValid(RecordPage* page);
  // Verify a child node records are within range given from parent node.
  bool VerifyChildRecordsRange(RecordPage* child_page,
                               Schema::RecordBase* left_bound,
                               Schema::RecordBase* right_bound);
  // Enqueue children tree nodes, used in level-traversing B+ tree.
  bool EqueueChildNodes(RecordPage* page, std::queue<RecordPage*>* page_q);

  // Check meta data consistency in ValidityCheck().
  bool MetaConsistencyCheck() const;

  // Check overflow page.
  bool VerifyOverflowPage(RecordPage* page);

  class SearchTreeNodeResult {
   public:
    int slot = -1;
    int child_id = -1;
    std::shared_ptr<Schema::RecordBase> record;
    int next_slot = -1;
    int next_child_id = -1;
    std::shared_ptr<Schema::RecordBase> next_record;
    int next_leave_id = -1;
  };

  // Search for a key in the page and returns next level page this key
  // should reside in.
  SearchTreeNodeResult SearchInTreeNode(RecordPage* page,
                                        const Schema::RecordBase* key);

  // Fetch all matching records from BB+ tree.
  int FetchResultsFromLeave(
          RecordPage* leave,
          const Schema::RecordBase* key,
          std::vector<std::shared_ptr<Schema::RecordBase>>* result);

  bool CheckKeyFieldsType(const Schema::RecordBase* key) const;
  bool CheckRecordFieldsType(const Schema::RecordBase* record) const;

  // Create a new leave in bulk loading.
  RecordPage* AppendNewLeave();
  // Create a new overflow leave in bulk loading.
  RecordPage* AppendNewOverflowLeave();

  bool ProduceKeyRecordFromLeaveRecord(
        const Schema::RecordBase* leave_record, Schema::RecordBase* tn_record);

  // Redistribute records with next leave.
  bool ReDistributeWithNextLeave(RecordPage* leave,
                                 SearchTreeNodeResult* search_result,
                                 const Schema::RecordBase* record);

  // Incurring overflow page when inserting new record.
  bool InsertAfterOverflowLeave(RecordPage* leave,
                                SearchTreeNodeResult* search_result,
                                const Schema::RecordBase* record);
  // Insert record to next leave.
  bool InsertNewRecordToNextLeave(RecordPage* leave,
                                  SearchTreeNodeResult* search_result,
                                  const Schema::RecordBase* record);
  // Re-distribute records from next leave.
  bool ReDistributeRecordsFromNextLeave(RecordPage* leave,
                                        SearchTreeNodeResult* search_result);

  // Insert a new record to leave which will split the leave.
  bool InsertNewRecordToLeaveWithSplit(RecordPage* leave,
                                       int next_leave_id,
                                       const Schema::RecordBase* record);

  // Go to last overflow page of an overflow chain.
  RecordPage* GotoOverflowChainEnd(RecordPage* leave);

  // Insert a new record to the end of a overflow chain of leaves.
  bool InsertNewRecordToOverFlowChain(
           RecordPage* leave, const Schema::RecordBase* record);

  // Create a new leave with a new record inserted.
  RecordPage* CreateNewLeaveWithRecord(
      const Schema::RecordBase* record,
      Schema::TreeNodeRecord* tn_record=nullptr);

  FILE* file_ = nullptr;

  // Table Schema
  DataBase::Table* table_;

  // FileType
  FileType file_type_ = UNKNOWN_FILETYPE;

  // Index of fields in key.
  std::vector<int> key_indexes_;

  // Header page contains meta data of this B+ tree.
  std::unique_ptr<BplusTreeHeaderPage> header_;

  // PageMap: <page_id --> RecordPage>
  // This is a page cache for active pages that are being processed. Root node
  // page should always reside in this cache. Whenever a page is removed from
  // cache, it must be written to disk file.
  using PageMap = std::map<int, std::shared_ptr<RecordPage>>;
  PageMap page_map_;

  // Helper class for bulkloading and validity check.
  class BulkLoadingStatus {
   public:
    RecordPage* crt_leave = nullptr;
    RecordPage* prev_leave = nullptr;
    std::shared_ptr<Schema::RecordBase> last_record;
    Schema::RecordID rid;
  };

  BulkLoadingStatus bl_status_;

  class ValidityCheckStatus {
   public:
    int count_num_pages = 0;
    int count_num_used_pages = 0;
    int count_num_free_pages = 0;
    int count_num_leaves = 0;
    int count_depth = 0;
    int prev_leave_id = -1;
    int prev_leave_next = -1;
    int count_num_records = 0;
  };

  ValidityCheckStatus vc_status_;
};

}

#endif  /* STORAGE_BPLUS_TREE_ */
