#ifndef STORAGE_BPLUS_TREE_
#define STORAGE_BPLUS_TREE_

#include <memory>
#include <map>

#include "Schema/Record.h"
#include "Schema/DBTable_pb.h"
#include "PageBase.h"
#include "RecordPage.h"

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
  DEFINE_ACCESSOR(depth, int);

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
};


class BplusTree {
 public:
  BplusTree() = default;
  // Contruct B+ tree from an existing file.
  BplusTree(std::string tablename, std::vector<int> key_indexes);

  // Destructor
  ~BplusTree();

  // Accessors
  DEFINE_ACCESSOR(tablename, std::string);
  DEFINE_ACCESSOR(file, FILE*);
  DEFINE_ACCESSOR(key_indexes, std::vector<int>);
  DEFINE_ACCESSOR_ENUM(file_type, FileType);
  BplusTreeHeaderPage* meta() { return header_.get(); }
  RecordPage* root();
  Schema::TableSchema* schema() { return schema_.get(); }

  // Create an index or index-data file based on B+ tree;
  bool CreateFile(std::string tablename, std::vector<int> key_indexes,
                  FileType file_type);

  // Load and save the B+ tree
  bool SaveToDisk() const;
  bool LoadFromDisk();

  // BulkLoading data. Input is a list of Record consisting of various fields
  // defined in Schema/DataTypes. These records must have been sorted based
  // on a key which consists of fields from Record speficed from key_indexes.
  bool BulkLoading(std::vector<Schema::DataRecord>& records,
                   const std::vector<int>& key_indexes);

  friend class BplusTreeTest;

 private:
  // Generate B+ tree file name, based on table name, key indexes and file type.
  std::string GenerateBplusTreeFilename(FileType file_type);
  // Load header page from disk.
  bool LoadHeaderPage();
  // Load root node from disk.
  bool LoadRootNode();
  // Load table schema from schema file, which is a serialized protocal buffer
  // raw file. It saves message TableSchema defined in Schema/DBTable.proto.
  bool LoadSchema();

  // Allocate a new page in bulkloading.
  RecordPage* AllocateNewPage();
  // Insert a record to a leave node.
  bool InsertRecordToLeave(const Schema::DataRecord& record, RecordPage* leave);
  // Insert a page to a parent node.
  void InsertPageToParentNode(RecordPage* page, RecordPage* parent);

  std::string tablename_;
  FILE* file_ = nullptr;

  // Index of fields in key.
  std::vector<int> key_indexes_;

  // FileType
  FileType file_type_ = UNKNOWN_FILETYPE;

  // Table Schema
  std::unique_ptr<Schema::TableSchema> schema_;

  // Header page contains meta data of this B+ tree.
  std::unique_ptr<BplusTreeHeaderPage> header_;

  // PageMap: <page_id ==> RecordPage>
  // This is a page cache for active pages that are being processed. Root node
  // page should always reside in this cache. Whenever a page is removed from
  // cache, it must be written to disk file.
  using PageMap = std::map<int, std::shared_ptr<RecordPage>>;
  PageMap page_map_;

  // helper variables for bulkloading
  RecordPage* crt_leave = nullptr;
  RecordPage* crt_node = nullptr;
  int next_id = 1;
};

}

#endif  /* STORAGE_BPLUS_TREE_ */
