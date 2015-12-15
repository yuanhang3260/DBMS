#include "Base/Log.h"

#include "RecordPage.h"

namespace DataBaseFiles {

// ************************** RecordPageMeta ******************************** //
void SlotDirectoryEntry::SaveToMem(byte* buf) const {
  memcpy(buf, &offset_, sizeof(offset_));
  memcpy(buf + sizeof(offset_), &length_, sizeof(length_));
}

void SlotDirectoryEntry::LoadFromMem(const byte* buf) {
  memcpy(&offset_, buf, sizeof(offset_));
  memcpy(&length_, buf + sizeof(offset_), sizeof(length_));
}


// ************************** RecordPageMeta ******************************** //
std::vector<SlotDirectoryEntry>& RecordPageMeta::slot_directory() {
  return slot_directory_;
}

int RecordPageMeta::size() const {
  // Slot directory entry size = 2 * sizeof(int)
  return (sizeof(int) * 5) + (sizeof(int) * 2) * slot_directory_.size();
}

void RecordPageMeta::reset() {
  num_slots_ = 0;
  num_records_ = 0;
  free_start_ = 0;
  next_page_ = -1;
  prev_page_ = -1;
  slot_directory_.clear();
}

// Meta data is stored at the end of each page. The layout is reverse order of
// as shown in RecordPage.h
bool RecordPageMeta::SaveMetaToPage(byte* ppage) const {
  if (num_slots_ != (int)slot_directory_.size()) {
    LogERROR("Page meta data error - num_slots inconsistency");
  }
  int offset = kPageSize - sizeof(num_slots_);
  memcpy(ppage + offset, &num_slots_, sizeof(num_slots_));
  
  offset -= sizeof(num_records_);
  memcpy(ppage + offset, &num_records_, sizeof(num_records_));

  offset -= sizeof(free_start_);
  memcpy(ppage + offset, &free_start_, sizeof(free_start_));

  offset -= sizeof(next_page_);
  memcpy(ppage + offset, &next_page_, sizeof(next_page_));

  offset -= sizeof(prev_page_);
  memcpy(ppage + offset, &prev_page_, sizeof(prev_page_));

  // save slot directory
  int valid_records = 0;
  for (const auto& slot: slot_directory_) {
    offset -= (sizeof(int) * 2);
    slot.SaveToMem(ppage + offset);
    if (slot.offset() >= 0) {
      valid_records ++;
    }
  }

  if (num_records_ != valid_records) {
    LogERROR("[Save page meta data error] - num_records inconsistency");
    return false;
  }
  if (kPageSize - offset != size()) {
    LogERROR("[Save page meta data error] - meta data length inconsistency");
    return false;
  }

  return true;
}

bool RecordPageMeta::LoadMetaFromPage(const byte* ppage) {
  reset();

  int offset = kPageSize - sizeof(num_slots_);
  memcpy(&num_slots_, ppage + offset, sizeof(num_slots_));
  
  offset -= sizeof(num_records_);
  memcpy(&num_records_, ppage + offset, sizeof(num_records_));

  offset -= sizeof(free_start_);
  memcpy(&free_start_, ppage + offset, sizeof(free_start_));

  offset -= sizeof(next_page_);
  memcpy(&next_page_, ppage + offset, sizeof(next_page_));

  offset -= sizeof(prev_page_);
  memcpy(&prev_page_, ppage + offset, sizeof(prev_page_));

  // Load slot directory.
  int valid_records = 0;
  for (int i = 0; i < num_slots_; i++) {
    offset -= sizeof(int) * 2;
    slot_directory_.emplace_back(-1, 0);
    slot_directory_.back().LoadFromMem(ppage + offset);
    if (slot_directory_.back().offset() > 0) {
      valid_records++;
    }
  }

  // Consistency checks for loaded page meta.
  if (num_records_ != valid_records) {
    LogERROR("[Load page meta data error] - num_records inconsistency");
    return false;
  }
  if (kPageSize - offset != size()) {
    LogERROR("[Load page meta data error] - meta data length inconsistency");
    return false;
  }

  return true;
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
  return kPageSize - page_meta_->free_start() - page_meta_->size();
}

bool RecordPage::DumpPageData() {
  if (!file_ || !data_) {
    return false;
  }
  
  // Refresh meta data.
  if (!page_meta_) {
    LogERROR("[Dump Page Error] - No meta available");
    return false;
  }
  if (!page_meta_->SaveMetaToPage(data_)) {
    return false;
  }

  // Write page to file.
  fseek(file_, id_ * kPageSize, SEEK_SET);
  int re = fwrite(data_, 1, kPageSize, file_);
  if (re != kPageSize) {
    LogERROR("[Dump Page Error] - Wrote %d bytes", re);
    return false;
  }
  fflush(file_);

  return true;
}

bool RecordPage::LoadPageData() {
  if (!file_ || !data_) {
    return false;
  }

  // Load page data.
  fflush(file_);
  fseek(file_, id_ * kPageSize, SEEK_SET);
  int re = fread(data_, 1, kPageSize, file_);
  if (re != kPageSize) {
    LogERROR("[Read Page Error] - Load %d bytes", re);
    return false;
  }

  // Parse meta data.
  if (!page_meta_) {
    page_meta_.reset(new RecordPageMeta());
  }
  if (!page_meta_->LoadMetaFromPage(data_)) {
    return false;
  }
  valid_ = true;
  return true;
}

}  // namespace DataBaseFiles
