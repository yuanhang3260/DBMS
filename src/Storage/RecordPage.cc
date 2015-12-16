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

int RecordPageMeta::AllocateSlotAvailable() {
  if (empty_slots_.size() == 0) {
    // We might even have no enough space to allocate a new slot directory enty.
    if (free_start_ + 8 + size() > kPageSize) {
      LogINFO("[Allocate New Slot ID Failed] - No enough space");
      return -1;
    }
    slot_directory_.emplace_back(-1, 0);
    num_slots_++;
    return slot_directory_.size() - 1;
  }
  else {
    int re = empty_slots_.back();
    empty_slots_.pop_back();
    return re;
  }
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
    else {
      empty_slots_.push_back(i);
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
  if (!page_meta_->SaveMetaToPage(data_.get())) {
    return false;
  }

  // Write page to file.
  fseek(file_, id_ * kPageSize, SEEK_SET);
  int re = fwrite(data_.get(), 1, kPageSize, file_);
  if (re != kPageSize) {
    LogERROR("[Dump Page Error] - Wrote %d bytes", re);
    return false;
  }
  fflush(file_);

  return true;
}

bool RecordPage::LoadPageData() {
  if (!file_) {
    return false;
  }

  if (!data_) {
    data_.reset(new byte[kPageSize]);
  }

  // Load page data.
  fflush(file_);
  fseek(file_, id_ * kPageSize, SEEK_SET);
  int re = fread(data_.get(), 1, kPageSize, file_);
  if (re != kPageSize) {
    LogERROR("[Read Page Error] - Load %d bytes", re);
    return false;
  }

  // Parse meta data.
  if (!page_meta_) {
    page_meta_.reset(new RecordPageMeta());
  }
  if (!page_meta_->LoadMetaFromPage(data_.get())) {
    return false;
  }
  valid_ = true;
  return true;
}

void RecordPage::ReorganizeRecords() {
  byte* new_page = new byte[kPageSize];
  int offset = 0;
  for (const auto& slot: page_meta_->slot_directory()) {
    if (slot.offset() >= 0) {
      memcpy(new_page + offset, data_.get() + slot.offset(), slot.length());
      offset += slot.length();
    }
  }
  page_meta_->set_free_start(offset);
  // save meta to the new page.
  page_meta_->SaveMetaToPage(new_page);

  data_.reset(new_page);
}

bool RecordPage::InsertRecord(const byte* content, int length) {
  uint32 slot_id = page_meta_->AllocateSlotAvailable();
  std::vector<SlotDirectoryEntry>& slot_dir = page_meta_->slot_directory();

  if (FreeSize() < length) {
    // Re-organize records and re-try insertion.
    ReorganizeRecords();
    if (FreeSize() < length) {
      // Rollback the possible newly allocated entry at the end of slot
      // directory.
      if (slot_dir.back().offset() < 0) {
        slot_dir.pop_back();
      }
      return false;
    }
  }

  slot_dir[slot_id].set_offset(page_meta_->free_start());
  slot_dir[slot_id].set_length(length);
  page_meta_->increment_free_start(length);
  page_meta_->increment_num_records(1);

  // Write the record content to page.
  memcpy(data_.get() + slot_dir[slot_id].offset(), content, length);

  // (TODO: need this?) Re-write meta data to page.
  if (!page_meta_->SaveMetaToPage(data_.get())) {
    return false;
  }

  return true;
}

}  // namespace DataBaseFiles
