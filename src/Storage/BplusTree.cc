#include <string.h>

#include "Base/Log.h"
#include "BplusTree.h"


namespace DataBaseFiles {

bool BplusTreeHeaderPage::DumpToMem(byte* buf) const {
  if (!buf) {
    LogERROR("[Can't dump B+ tree header page to memory] - nullptr");
    return false;
  }

  if (!ConsistencyCheck()) {
    return false;
  }

  int offset = 0;
  // record type
  memcpy(buf + offset, &record_type_, sizeof(record_type_));
  offset += sizeof(record_type_);
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

  return false;
}


bool BplusTreeHeaderPage::LoadFromMem(const byte* buf) {
  if (!buf) {
    LogERROR("[Can't load B+ tree header page from memory] - nullptr");
    return false;
  }

  int offset = 0;
  // record type
  memcpy(&record_type_, buf + offset, sizeof(record_type_));
  offset += sizeof(record_type_);
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
  if (!ConsistencyCheck()) {
    return false;
  }

  return false;
}

bool BplusTreeHeaderPage::ConsistencyCheck() const {
  if (num_pages_ != num_free_pages_ + num_used_pages_) {
    LogERROR("[Save B+ tree header page error] - "
             "num_pages !=  num_used_pages_ + num_free_pages_, "
             "(%d != %d + %d)",
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

}  // namespace DataBaseFiles

