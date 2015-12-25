#ifndef STORAGE_BPLUS_TREE_
#define STORAGE_BPLUS_TREE_

#include <memory>
#include <map>

#include "Schema/Record.h"
#include "PageBase.h"
#include "RecordPage.h"

namespace DataBaseFiles {

class TreeNodeRecord {
 public:
  TreeNodeRecord() = default;
  ~TreeNodeRecord();

  DEFINE_ACCESSOR(page_id, int);

  int size() const { return key_->size() + sizeof(page_id_); }

  int ParseFromMem(const byte* buf);
  int DumpToMem(byte* buf) const;

 private:
  std::shared_ptr<Schema::RecordKey> key_;
  int page_id_ = -1;
};


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
  BplusTree(std::string filename);
  ~BplusTree();

  // Accessors
  DEFINE_ACCESSOR(file, FILE*);
  BplusTreeHeaderPage* meta() { return header_.get(); }
  RecordPage* root();

  // Create an index or index-data file based on B+ tree;
  bool CreateFile(std::string filename, FileType file_type);

  // Load and save the B+ tree
  bool SaveToDisk() const;
  bool LoadFromDisk();

  // BulkLoading data. Input is a list of Record consisting of various fields
  // defined in Schema/DataTypes. These records should be sorted first based
  // on a key which consists of fields from Record speficed from key_indexes.
  bool BulkLoading(std::vector<Schema::Record>,
                   const std::vector<int>& key_indexes);

  // Sort a list of records based on indexes that specified key.
  static void SortRecords(std::vector<Schema::Record>& records,
                          const std::vector<int>& key_indexes);

 private:
  bool LoadHeaderPage();
  bool LoadRootNode();

  FILE* file_ = nullptr;

  // Header page contains meta data of this B+ tree.
  std::unique_ptr<BplusTreeHeaderPage> header_;

  // PageMap: <page_id => RecordPage>
  // This is a page cache for active pages that are being processed. Root node
  // page should always reside in this cache. Whenever a page is removed from
  // cache, it must be written to disk file.
  using PageMap = std::map<int, std::shared_ptr<RecordPage>>;
  PageMap page_map_;
};

}

#endif  /* STORAGE_BPLUS_TREE_ */
