#include <string.h>

#include "Base/Log.h"
#include "BplusTree.h"

namespace DataBaseFiles {

// *************************** TreeNodeRecord ********************************//
int TreeNodeRecord::ParseFromMem(const byte* buf) {
  if (!buf) {
    return -1;
  }
  int offset = key_->LoadFromMem(buf);
  memcpy(&page_id_, buf + offset, sizeof(page_id_));
  return offset + sizeof(page_id_);
}

int TreeNodeRecord::DumpToMem(byte* buf) const {
  if (!buf) {
    return -1;
  }
  int offset = key_->DumpToMem(buf);
  memcpy(buf + offset, &page_id_, sizeof(page_id_));
  return offset + sizeof(page_id_);
}

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
    LogERROR("[Can't dump B+ tree header page to memory] - nullptr");
    return false;
  }

  if (!ConsistencyCheck("Dump")) {
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
    LogERROR("[Can't load B+ tree header page from memory] - nullptr");
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

  return false;
}

bool BplusTreeHeaderPage::SaveToDisk() const {
  if (!file_) {
    LogERROR("[Can't save B+ tree header to disk] - FILE is nullptr");
    return false;
  }

  byte buf[kPageSize];
  if (!DumpToMem(buf)) {
    LogERROR("[Can't save B+ tree header to disk] - DumpToMem failed");
    return false;
  }

  fseek(file_, 0, SEEK_SET);
  int nwrite = fwrite(buf, 1, kPageSize, file_);
  if (nwrite != kPageSize) {
    LogERROR("[Write B+ tree header page failed] - nwrite = %d", nwrite);
    return false;
  }
  fflush(file_);
  return true;
}

bool BplusTreeHeaderPage::LoadFromDisk() {
  if (!file_) {
    LogERROR("[Can't Load B+ tree header from disk] - FILE is nullptr");
  }

  byte buf[kPageSize];

  fflush(file_);
  fseek(file_, 0, SEEK_SET);
  int nread = fread(buf, 1, kPageSize, file_);
  if (nread != kPageSize) {
    LogERROR("[Read B+ tree header page failed] - nread = %d", nread);
    return false;
  }

  if (!ParseFromMem(buf)) {
    return false;
  }
  return true;
}

bool BplusTreeHeaderPage::ConsistencyCheck(const char* op) const {
  if (num_pages_ != num_free_pages_ + num_used_pages_) {
    LogERROR("[%s B+ tree header page error] - "
             "num_pages !=  num_used_pages_ + num_free_pages_, "
             "(%d != %d + %d)", op,
             num_pages_, num_used_pages_, num_free_pages_);
    return false;
  }

  if ((num_free_pages_ > 0 && free_page_ < 0) ||
      (num_free_pages_ <= 0 && free_page_ >= 0)) {
    LogERROR("[Save B+ tree header page error] - "
             "num_free_pages_ = %d, first_free_page_id = %d",
             num_free_pages_, free_page_);
    return false;
  }

  if (root_page_ < 0 && (num_leaves_ > 0 || depth_ > 0)) {
    LogERROR("[Save B+ tree header page error] - "
             "Empty tree, but num_leaves_ = %d, depth_ = %d",
             num_leaves_, depth_);
    return false;
  }

  return true;
}


// ****************************** BplusTree **********************************//
BplusTree::~BplusTree() {
  if (!file_) {
    fflush(file_);
    fclose(file_);
  }
}

BplusTree::BplusTree(std::string filename) {
  file_ = fopen(filename.c_str(), "a+");
  if (!file_) {
    LogERROR("[Open B+ tree file failed] - %s", filename.c_str());
    throw std::runtime_error("Can't init B+ tree");
  }

  // Load header page and root node.
  if (!LoadHeaderPage()) {
    throw std::runtime_error("Can't init B+ tree");
  }

  // Load root node if exists. Add it to PageMap.
  if (!LoadRootNode()) {
    throw std::runtime_error("Can't init B+ tree");
  }
}

bool BplusTree::LoadHeaderPage() {
  if (!file_) {
    LogERROR("[Won't load B+ tree header page] - FILE is nullptr");
    return false;
  }

  if (!header_) {
    header_.reset(new BplusTreeHeaderPage(file_));
  }

  if (header_->LoadFromDisk()) {
    return false;
  }
  return true;
}

bool BplusTree::LoadRootNode() {
  if (!header_) {
    LogERROR("[Can't create root node page] - B+ tree header page not loaded");
    return false;
  }
  int root_page_id = header_->root_page();
  if (root_page_id > 0) {
    page_map_[root_page_id] = std::make_shared<RecordPage>(root_page_id, file_);
    page_map_.at(root_page_id)->LoadPageData();
  }
  else {
    LogINFO("[Won't load root node] - root_page_id = %d", root_page_id);
  }
  return true;
}

RecordPage* BplusTree::root() {
  if (!header_) {
    LogERROR("[Can't get root node page] - B+ tree header page not loaded");
    return nullptr;
  }
  int root_page_id = header_->root_page();
  if (root_page_id > 0) {
    if (page_map_.find(root_page_id) == page_map_.end()) {
      LogERROR("[No root node for B+ tree] - root page is %d", root_page_id);
      return nullptr;
    }
    return page_map_.at(root_page_id).get();
  }
  return nullptr;
}

bool BplusTree::CreateFile(std::string filename, FileType file_type) {
  if (file_) {
    LogINFO("[Closing exsiting B+ tree file]");
    fclose(file_);
  }

  file_ = fopen(filename.c_str(), "w+");
  if (!file_) {
    LogERROR("[Create B+ tree file failed] - %s", filename.c_str());
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

  // (TODO) Save B+ tree. Root node, tree node and leaves.
  return true;
}

bool BplusTree::LoadFromDisk() {
  if (!header_->LoadFromDisk()) {
    return false;
  }

  // (TODO) Load B+ tree. Root node, tree node and leaves.
  return true;
}

}  // namespace DataBaseFiles

