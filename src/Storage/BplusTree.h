#ifndef STORAGE_BPLUS_TREE_
#define STORAGE_BPLUS_TREE_

#include <map>
#include <memory>
#include <queue>

#include "Database/Catalog_pb.h"
#include "Database/Operation.h"
#include "Storage/Record.h"
#include "Storage/PageRecord_Common.h"
#include "Storage/PageBase.h"
#include "Storage/RecordPage.h"

namespace DB {
  class Table;
}

namespace Storage {

class BplusTreeTest;
class PageRecordsManager;
class TreeRecordIterator;

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

  // Dump header page to memory.
  bool DumpToMem(byte* buf) const override;
  // Parse header page from memory.
  bool ParseFromMem(const byte* buf) override;

  // Save header page to disk.
  bool SaveToDisk() const override;
  // Load header page from disk.
  bool LoadFromDisk() override;

  void reset();

 private:
  bool ConsistencyCheck(const char* op) const;

  int root_page_ = -1;
  int num_leaves_ = 0;
  int depth_ = 0;
};


class BplusTree {
 public:
  BplusTree() = default;
  // Contruct B+ tree from an existing file.
  BplusTree(DB::Table* table,
            FileType file_type,
            std::vector<uint32> key_indexes,
            bool create=false);

  // Destructor
  virtual ~BplusTree();

  // Accessors
  DEFINE_ACCESSOR(file, FILE*);
  DEFINE_ACCESSOR(table, DB::Table*);
  DEFINE_ACCESSOR(key_indexes, std::vector<uint32>);
  DEFINE_ACCESSOR_ENUM(file_type, FileType);
  BplusTreeHeaderPage* meta() { return header_.get(); }
  RecordPage* root();
  const DB::TableInfo& schema() const;

  // Load B+ tree from file.
  bool LoadTree();

  // Create the B+ tree file;
  bool CreateBplusTreeFile();

  // Load and save the B+ tree
  bool SaveToDisk() const;
  bool LoadFromDisk();

  // Tree is empty.
  bool Empty() const;

  // Fetch a page, either from page map or load it from disk.
  RecordPage* Page(int page_id);

  byte* Record(RecordID rid);

  // Produce indexes to compare records. For INDEX-DATA file, they are key
  // indexes; For INDEX file, they are [0, 1, 2 ... (num_keys-1)]
  std::vector<uint32> IndexesToCompareLeaveRecords() const;

  // BulkLoading data. Input is a list of Record consisting of various fields
  // defined in Schema/DataTypes. These records must have been sorted based
  // on a key which consists of fields from Record speficed from key_indexes.
  bool BulkLoad(std::vector<std::shared_ptr<RecordBase>>& records);
  bool BulkLoadRecord(const RecordBase& record);

  // Validity check for the B+ tree.
  bool ValidityCheck();

  // Fetch record from tree given record ID.
  std::shared_ptr<RecordBase> GetRecord(RecordID rid);

  // Serach records by a key. Returns all records that matches this key.
  // int SearchRecords(const std::vector<std::shared_ptr<RecordBase>>& keys,
  //                   std::vector<std::shared_ptr<RecordBase>>* result);

  int ScanRecords(std::vector<std::shared_ptr<RecordBase>>* result);

  // Search a key and return the leave.
  RecordPage* SearchByKey(const RecordBase& key);

  int SearchRecords(const DB::SearchOp& op,
                    std::vector<std::shared_ptr<RecordBase>>* result);

  std::shared_ptr<TreeRecordIterator> RecordIterator(const DB::SearchOp* op);

  // Insert a data record to the B+ tree.
  RecordID Do_InsertRecord(
           const RecordBase& record,
           std::vector<DataRecordRidMutation>* rid_mutations);

  // Delete records by key.
  bool Do_DeleteRecordByKey(
           const std::vector<std::shared_ptr<RecordBase>>& keys,
           DB::DeleteResult* result);

  // Delete records by Record ID - This is used in deleting data records after
  // deletion from index tree.
  // arg index_del_result: delete result from index tree deletion.
  bool Do_DeleteRecordByRecordID(
           DB::DeleteResult& index_del_result,
           DB::DeleteResult* result);

  // Update/Delete index records for an index tree, after data tree has been
  // modified (delete or insert records).
  bool UpdateIndexRecords(
           std::vector<DataRecordRidMutation>& rid_mutations);

  // Allocate a new page in bulkloading.
  RecordPage* AllocateNewPage(PageType page_type);

  // Recycle a page into free page list.
  bool RecyclePage(int page_id);

  // Append a overflow page to an existing page.
  RecordPage* AppendOverflowPageTo(RecordPage* page);

  bool DeleteOverflowLeave(RecordPage* leave);

  // Connect two leaves.
  static bool ConnectLeaves(RecordPage* page1, RecordPage* page2);

  // Get the left-most leave id.
  RecordPage* FirstLeave();

  friend class BplusTreeTest;

 protected:
  // Generate B+ tree file name, based on table name, key indexes and file type.
  std::string GenerateBplusTreeFileName(FileType file_type);
  // Load header page from disk.
  bool LoadHeaderPage();
  // Load root node from disk.
  bool LoadRootNode();
  // Load table schema from schema file, which is a serialized protocal buffer
  // raw file. It saves message DB::TableInfo defined in Schema/DBTable.proto.
  bool LoadSchema();

  // Verify an empty tree.
  bool VerifyEmptyTree() const;

  // Add first leave to empty tree.
  bool AddFirstLeaveToTree(RecordPage* leave);
  // Insert a page to a parent node.
  bool AddLeaveToTree(RecordPage* leave, TreeNodeRecord* tn_record);
  // Insert a new TreeNodeRecord to tree node page.
  bool InsertTreeNodeRecord(const TreeNodeRecord& tn_record,
                            RecordPage* tn_page);

  bool BlukLoadInsertRecordToLeave(const RecordBase& record,
                                   RecordPage* leave);

  // Delete a node from the B+ tree.
  bool DeleteNodeFromTree(RecordPage* page, int slot_id_in_parent);

  // Process a node when it has a record deleted. Mostly it check if space
  // occpution is below 1/2. If yes, probably we need to re-distribute records
  // or merge node with sibling nodes, and a merging operation may also
  // propagate tree node record deletion to upper nodes.
  bool ProcessNodeAfterRecordDeletion(
           RecordPage* page,
           DB::DeleteResult* result);

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
  bool CheckBoundaryDuplication(const RecordBase& record);

  // Save page to disk and de-cache it from page map.
  bool CheckoutPage(int page_id, bool write_to_disk=true);

  // Consistency check for a tree node. It loads and sorts all TreeNodeRecords
  // from this tree node, and verify each child node within the interval of
  // every two tree node record boundaries.
  bool CheckTreeNodeValid(RecordPage* page);
  // Verify a child node records are within range given from parent node.
  bool VerifyChildRecordsRange(RecordPage* child_page,
                               const RecordBase* left_bound,
                               const RecordBase* right_bound);
  // Enqueue children tree nodes, used in level-traversing B+ tree.
  bool EqueueChildNodes(RecordPage* page, std::queue<RecordPage*>* page_q);

  // Check meta data consistency in ValidityCheck().
  bool MetaConsistencyCheck() const;

  // Check overflow page.
  bool VerifyOverflowPage(RecordPage* page);

  // Verify a record in B+ tree is same as the given record.
  bool VerifyRecord(const RecordID& rid, const RecordBase& r);

  // Parse page_id / RecordID at the end of a record.
  template<class T>
  T* ParseRecordField(const RecordPage* node, int slot_id) {
    return reinterpret_cast<T*>(node->Record(slot_id) +
                                node->RecordLength(slot_id) -
                                sizeof(T));
  }

  // Print all records on a page.
  void PrintNodeRecords(RecordPage* page);

  class SearchTreeNodeResult {
   public:
    int slot = -1;
    int child_id = -1;
    std::shared_ptr<RecordBase> record;

    int next_slot = -1;
    int next_child_id = -1;
    std::shared_ptr<RecordBase> next_record;
    int next_leave_id = -1;

    int prev_slot = -1;
    int prev_child_id = -1;
    std::shared_ptr<RecordBase> prev_record;
    int prev_leave_id = -1;    
  };

  // Search for a key in the page and returns next level page this key
  // should reside in.
  SearchTreeNodeResult SearchInTreeNode(const RecordBase& key,
                                        RecordPage* page);

  SearchTreeNodeResult LookUpTreeNodeInfoForPage(const RecordPage& page);

  // Fetch all matching records from BB+ tree.
  // int FetchResultsFromLeave(
  //         const RecordBase& key,
  //         RecordPage* leave,
  //         std::vector<std::shared_ptr<RecordBase>>* result);

  int DeleteMatchedRecordsFromLeave(
         const RecordBase& key,
         RecordPage* leave,
         DB::DeleteResult* result);

  bool CheckKeyFieldsType(const RecordBase& key) const;
  bool CheckRecordFieldsType(const RecordBase& record) const;

  // Create a new leave in bulk loading.
  RecordPage* AppendNewLeave();
  // Create a new overflow leave in bulk loading.
  RecordPage* AppendNewOverflowLeave();

  bool ProduceKeyRecordFromNodeRecord(
        const RecordBase& leave_record, RecordBase* tn_record);

  // Redistribute records with next leave.
  RecordID ReDistributeToNextLeave(
           const RecordBase& record,
           RecordPage* leave,
           SearchTreeNodeResult* search_result,
           std::vector<DataRecordRidMutation>* rid_mutations);

  // Incurring overflow page when inserting new record.
  RecordID InsertAfterOverflowLeave(
           const RecordBase& record,
           RecordPage* leave,
           SearchTreeNodeResult* search_result,
           std::vector<DataRecordRidMutation>* rid_mutations);
  // Insert record to next leave.
  RecordID InsertNewRecordToNextLeave(const RecordBase& record,
                                      RecordPage* leave,
                                      SearchTreeNodeResult* search_result);

  // Re-distribute records from next leave.
  bool ReDistributeRecordsWithinTwoPages(
           RecordPage* page1, RecordPage* page2, int page2_slot_id_in_parent,
           std::vector<DataRecordRidMutation>* rid_mutations,
           bool force_redistribute=false);

  // Merge two nodes.
  bool MergeTwoNodes(RecordPage* page1, RecordPage* page2,
                     int page2_slot_id_in_parent,
                     std::vector<DataRecordRidMutation>* rid_mutations);

  // Insert a new record to leave which will split the leave.
  RecordID InsertNewRecordToLeaveWithSplit(
           const RecordBase& record,
           int next_leave_id,
           RecordPage* leave,
           std::vector<DataRecordRidMutation>* rid_mutations);

  // Go to last overflow page of an overflow chain.
  RecordPage* GotoOverflowChainEnd(RecordPage* leave);

  // Insert a new record to the end of a overflow chain of leaves.
  RecordID InsertNewRecordToOverFlowChain(
           const RecordBase& record, RecordPage* leave);

  // Create a new leave with a new record inserted.
  RecordPage* CreateNewLeaveWithRecord(
      const RecordBase& record,
      TreeNodeRecord* tn_record=nullptr);

  FILE* file_ = nullptr;

  // Table Schema
  DB::Table* table_;

  // FileType
  FileType file_type_ = UNKNOWN_FILETYPE;

  // Index of fields in key.
  std::vector<uint32> key_indexes_;

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
    std::shared_ptr<RecordBase> last_record;
    RecordID rid;
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

  // Only for test - to allocated pages randomly.
  std::vector<int> new_page_id_list;
  int next_index = 0;

  friend class TreeRecordIterator;
};

struct TreeRecordIterator {
  TreeRecordIterator() = default;
  TreeRecordIterator(BplusTree* tree_, const DB::SearchOp* search_op_) :
    tree(tree_),
    search_op(search_op_) {}

  BplusTree* tree = nullptr;
  const DB::SearchOp* search_op = nullptr;

  RecordPage* leave = nullptr;
  std::shared_ptr<PageRecordsManager> prmanager;

  bool ready = false;
  bool end = false;

  uint32 page_record_index = 0;
  bool last_is_match = false;

  void Init();
  std::shared_ptr<RecordBase> GetNextRecord();
};

}  // namespace Storage

#endif  /* STORAGE_BPLUS_TREE_ */
