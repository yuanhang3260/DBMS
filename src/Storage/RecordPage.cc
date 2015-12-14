#include "RecordPage.h"

namespace DataBaseFiles {

// ************************** RecordPageMeta ******************************** //
std::vector<SlotDirectoryEntry>& RecordPageMeta::slot_directory() {
  return slot_directory_;
}

int RecordPageMeta::size() const {
  // Slot directory entry size = 2 * sizeof(int)
  return sizeof(int) * 5 + (sizeof(int) * 2) * slot_directory_.size();
}


// **************************** RecordPage ********************************** //
RecordPage::~RecordPage() {
  if (data_) {
    delete data_;
  }
}

int RecordPage::FreeSize() const {
  if (!page_meta_) {
    return -1;
  }
  return PAGE_SIZE - page_meta_->free_start() - page_meta_->size();
}

bool RecordPage::DumpPageData() {
  if (!file_ || !data_) {
    return false;
  }
  // (TODO)Update meta data.
  // (TODO)Write page to file 
  return true;
}

bool RecordPage::LoadPageData() {
  if (!file_ || !data_) {
    return false;
  }
  return true;
}

}  // namespace DataBaseFiles
