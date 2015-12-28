#include <sys/stat.h>
#include <string.h>

#include "Base/Log.h"
#include "Base/Utils.h"
#include "BplusTree.h"

namespace DataBaseFiles {

// ************************ BplusTreeHeaderPage ******************************//
BplusTreeHeaderPage::BplusTreeHeaderPage(FileType file_type) :
    HeaderPage(file_type) {
}

BplusTreeHeaderPage::BplusTreeHeaderPage(FILE* file) :
    HeaderPage(file) {
}

BplusTreeHeaderPage::BplusTreeHeaderPage(FILE* file, FileType file_type) :
    HeaderPage(file, file_type) {
}

bool BplusTreeHeaderPage::DumpToMem(byte* buf) const {
  if (!buf) {
    LogERROR("buf nullptr");
    return false;
  }

  if (!ConsistencyCheck("Dump")) {
    LogERROR("ConsistencyCheck failed");
    return false;
  }

  int offset = 0;
  // record type
  memcpy(buf + offset, &file_type_, sizeof(file_type_));
  offset += sizeof(file_type_);
  // num_pages
  memcpy(buf + offset, &num_pages_, sizeof(num_pages_));
  offset += sizeof(num_pages_);
  // num_free_pages
  memcpy(buf + offset, &num_free_pages_, sizeof(num_free_pages_));
  offset += sizeof(num_free_pages_);
  // num_used_pages
  memcpy(buf + offset, &num_used_pages_, sizeof(num_used_pages_));
  offset += sizeof(num_used_pages_);
  // first free_page id
  memcpy(buf + offset, &free_page_, sizeof(free_page_));
  offset += sizeof(free_page_);
  // root_page id
  memcpy(buf + offset, &root_page_, sizeof(root_page_));
  offset += sizeof(root_page_);
  // num_leaves
  memcpy(buf + offset, &num_leaves_, sizeof(num_leaves_));
  offset += sizeof(num_leaves_);
  // depth
  memcpy(buf + offset, &depth_, sizeof(depth_));
  offset += sizeof(depth_);

  return true;
}


bool BplusTreeHeaderPage::ParseFromMem(const byte* buf) {
  if (!buf) {
    LogERROR("nullptr");
    return false;
  }

  int offset = 0;
  // record type
  memcpy(&file_type_, buf + offset, sizeof(file_type_));
  offset += sizeof(file_type_);
  // num_pages
  memcpy(&num_pages_, buf + offset, sizeof(num_pages_));
  offset += sizeof(num_pages_);
  // num_free_pages
  memcpy(&num_free_pages_, buf + offset, sizeof(num_free_pages_));
  offset += sizeof(num_free_pages_);
  // num_used_pages
  memcpy(&num_used_pages_, buf + offset, sizeof(num_used_pages_));
  offset += sizeof(num_used_pages_);
  // first free_page id
  memcpy(&free_page_, buf + offset, sizeof(free_page_));
  offset += sizeof(free_page_);
  // root_page id
  memcpy(&root_page_, buf + offset, sizeof(root_page_));
  offset += sizeof(root_page_);
  // num_leaves
  memcpy(&num_leaves_, buf + offset, sizeof(num_leaves_));
  offset += sizeof(num_leaves_);
  // depth
  memcpy(&depth_, buf + offset, sizeof(depth_));
  offset += sizeof(depth_);

  // Do consistency check.
  if (!ConsistencyCheck("Load")) {
    return false;
  }

  return true;
}

bool BplusTreeHeaderPage::SaveToDisk() const {
  if (!file_) {
    LogERROR("FILE is nullptr");
    return false;
  }

  byte buf[kPageSize];
  if (!DumpToMem(buf)) {
    LogERROR("DumpToMem(buf)");
    return false;
  }

  fseek(file_, 0, SEEK_SET);
  int nwrite = fwrite(buf, 1, kPageSize, file_);
  if (nwrite != kPageSize) {
    LogERROR("nwrite = %d", nwrite);
    return false;
  }
  fflush(file_);
  return true;
}

bool BplusTreeHeaderPage::LoadFromDisk() {
  if (!file_) {
    LogERROR("FILE is nullptr");
    return false;
  }

  byte buf[kPageSize];

  fflush(file_);
  fseek(file_, 0, SEEK_SET);
  int nread = fread(buf, 1, kPageSize, file_);
  if (nread != kPageSize) {
    LogERROR("nread = %d", nread);
    return false;
  }

  if (!ParseFromMem(buf)) {
    LogERROR("ParseFromMem(buf)");
    return false;
  }
  return true;
}

bool BplusTreeHeaderPage::ConsistencyCheck(const char* op) const {
  if (num_pages_ != num_free_pages_ + num_used_pages_) {
    LogERROR("num_pages !=  num_used_pages_ + num_free_pages_, "
             "(%d != %d + %d)", op,
             num_pages_, num_used_pages_, num_free_pages_);
    return false;
  }

  if ((num_free_pages_ > 0 && free_page_ < 0) ||
      (num_free_pages_ <= 0 && free_page_ >= 0)) {
    LogERROR(""
             "num_free_pages_ = %d, first_free_page_id = %d", op,
             num_free_pages_, free_page_);
    return false;
  }

  if (root_page_ < 0 && (num_leaves_ > 0 || depth_ > 0)) {
    LogERROR(""
             "Empty tree, but num_leaves_ = %d, depth_ = %d", op,
             num_leaves_, depth_);
    return false;
  }

  return true;
}


// ****************************** BplusTree **********************************//
BplusTree::~BplusTree() {
  if (file_) {
    fflush(file_);
    fclose(file_);
  }
}

BplusTree::BplusTree(std::string tablename, std::vector<int> key_indexes) :
    tablename_(tablename),
    key_indexes_(key_indexes) {
  std::string btree_filename = GenerateBplusTreeFilename(UNKNOWN_FILETYPE);

  file_ = fopen(btree_filename.c_str(), "a+");
  if (!file_) {
    LogERROR("file name %s", btree_filename.c_str());
    throw std::runtime_error("Can't init B+ tree");
  }

  // Load table schema, header page and root node.
  if (!LoadFromDisk()) {
    throw std::runtime_error("Can't init B+ tree");
  }
}

std::string BplusTree::GenerateBplusTreeFilename(FileType file_type) {
  std::string filename = kDataDirectory + tablename_ + "(";
  for (int index: key_indexes_) {
    filename += std::to_string(index) + "_";
  }
  filename += ")";
  
  if (file_type == UNKNOWN_FILETYPE) {
    // Check file type.
    // Data file.
    std::string fullname = filename + ".indata";
    if (access(fullname.c_str(), F_OK) != -1) {
      file_type_ = INDEX_DATA;
      return fullname;
    }

    // Index file.
    fullname = filename + ".index";
    if (access(fullname.c_str(), F_OK) != -1) {
      file_type_ = INDEX;
      return fullname;
    }
  }
  else if (file_type == INDEX_DATA) {
    return filename + ".indata";
  }
  else if (file_type == INDEX) {
    return filename + ".index";
  }
  return "unknown_filename";
}

bool BplusTree::LoadSchema() {
  std::string schema_filename = kDataDirectory + tablename_ + ".schema.pb";

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

bool BplusTree::LoadHeaderPage() {
  if (!file_) {
    LogERROR("FILE is nullptr");
    return false;
  }

  if (!header_) {
    header_.reset(new BplusTreeHeaderPage(file_));
  }

  if (!header_->LoadFromDisk()) {
    LogERROR("Load from disk failed");
    return false;
  }
  return true;
}

bool BplusTree::LoadRootNode() {
  if (!header_) {
    LogERROR("B+ tree header page not loaded");
    return false;
  }
  int root_page_id = header_->root_page();
  if (root_page_id > 0) {
    page_map_[root_page_id] = std::make_shared<RecordPage>(root_page_id, file_);
    page_map_.at(root_page_id)->LoadPageData();
  }
  else {
    LogINFO("LoadRootNodroot_page_id = %d", root_page_id);
  }
  return true;
}

RecordPage* BplusTree::root() {
  if (!header_) {
    LogERROR("B+ tree header page not loaded");
    return nullptr;
  }
  int root_page_id = header_->root_page();
  if (root_page_id > 0) {
    if (page_map_.find(root_page_id) == page_map_.end()) {
      LogERROR("can't root page id %d in page map", root_page_id);
      return nullptr;
    }
    return page_map_.at(root_page_id).get();
  }
  return nullptr;
}

bool BplusTree::CreateFile(std::string tablename,
                           std::vector<int> key_indexes,
                           FileType file_type) {
  tablename_ = tablename;
  key_indexes_ = key_indexes;
  file_type_ = file_type;

  if (!schema_) {
    if (!LoadSchema()) {
      LogERROR("Failed to load schema while creating new B+ tree for table %s",
               tablename_.c_str());
      return false;
    }
  }

  std::string filename = GenerateBplusTreeFilename(file_type);
  if (file_) {
    LogINFO("Existing B+ tree file, closing it ...");
    fclose(file_);
  }

  file_ = fopen(filename.c_str(), "w+");
  if (!file_) {
    LogERROR("open file %s failed", filename.c_str());
    return false;
  }

  // Create header page.
  header_.reset(new BplusTreeHeaderPage(file_, file_type));

  return true;
}

bool BplusTree::SaveToDisk() const {
  // Save header page
  if (!header_->SaveToDisk()) {
    return false;
  }

  // (TODO) Save B+ tree nodes. Root node, tree node and leaves.
  return true;
}

bool BplusTree::LoadFromDisk() {
  if (!LoadSchema()) {
    return false;
  }

  if (!LoadHeaderPage()) {
    return false;
  }

  // Load root node if exists. Add it to PageMap.
  if (!LoadRootNode()) {
    return false;
  }

  return true;
}

RecordPage* BplusTree::AllocateNewPage() {
  return new RecordPage(next_id++, file_);
}

bool BplusTree::InsertRecordToLeave(const Schema::DataRecord& record,
                                    RecordPage* leave) {
  byte* buf = leave->InsertRecord(record.size());
  if (buf) {
    // Write the record content to page.
    record.DumpToMem(buf);
    return true;
  }
  // Can't add record to leave page. No enough space.
  return false;
}

void BplusTree::InsertPageToParentNode(RecordPage* page, RecordPage* parent) {
  
}

// BulkLoading data
bool BplusTree::BulkLoading(std::vector<Schema::DataRecord>& records,
                            const std::vector<int>& key_indexes) {
  if (records.size() <= 0) {
    return true;
  }

  // Begin writing records to pages.
  for (const auto& record: records) {
    // Allocate a new leave page if necessary, and add to page map to cache.
    if (!crt_leave) {
      crt_leave = AllocateNewPage();
      page_map_[crt_leave->id()] = std::shared_ptr<RecordPage>(crt_leave);
    }
    // Try inserting the record to current leave page. If success, we continue.
    // Otherwise we need to add this leave page to a tree node (current active
    // tree node).
    if (InsertRecordToLeave(record, crt_leave)) {
      continue;
    }
    // Allocate a new tree node if necessary, and add to page map to cache.
    if (!crt_node) {
      crt_node = AllocateNewPage();
      page_map_[crt_node->id()] = std::shared_ptr<RecordPage>(crt_node);
    }
    // Add current leave page to tree node. This function may lead to tree node
    // split, and possibly propagate split to upper tree nodes recursively.
    InsertPageToParentNode(crt_leave, crt_node);
  }

  return true;
}

}  // namespace DataBaseFiles

