#include "Base/Log.h"
#include "Base/Utils.h"

#include "RecordPage.h"

namespace DataBaseFiles {

// ************************** RecordPageMeta ******************************** //
void SlotDirectoryEntry::SaveToMem(byte* buf) const {
  int entry = (offset_ << 16) | length_;
  memcpy(buf, &entry, sizeof(entry));
}

void SlotDirectoryEntry::LoadFromMem(const byte* buf) {
  int entry;
  memcpy(&entry, buf, sizeof(entry));
  offset_ = (entry >> 16) & 0x0000FFFF;
  length_ = entry & 0x0000FFFF;
}


// ************************** RecordPageMeta ******************************** //
std::vector<SlotDirectoryEntry>& RecordPageMeta::slot_directory() {
  return slot_directory_;
}

int RecordPageMeta::AllocateSlotAvailable() {
  if (empty_slots_.size() == 0) {
    // We might even have no enough space to allocate a new slot directory enty.
    if (free_start_ + kSlotDirectoryEntrySize + size() > kPageSize) {
      return -1;
    }
    slot_directory_.emplace_back(-1, 0);
    num_slots_++;
    return slot_directory_.size() - 1;
  }
  else {
    int re = *(empty_slots_.begin());
    if (slot_directory_[re].offset() >= 0) {
      LogERROR("Error empty slot - should not have offset > 0\n");
    }
    empty_slots_.erase(empty_slots_.begin());
    return re;
  }
}

bool RecordPageMeta::ReleaseSlot(int slot_id) {
  if (slot_directory_[slot_id].offset() >= 0) {
    slot_directory_[slot_id].set_offset(-1);
    num_records_--;
    // Remove the slot entry if at the end of slot directory.
    if (slot_id == (int)slot_directory_.size() - 1) {
      slot_directory_.pop_back();
      num_slots_--;
      // And removing all trailing empty slot entries. We make sure the last
      // slot entry is always a valid one.
      while (!slot_directory_.empty() && slot_directory_.back().offset() < 0) {
        int id = slot_directory_.size() - 1;
        slot_directory_.pop_back();
        // Remove it from empty slot set.
        auto it = empty_slots_.find(id);
        if (it != empty_slots_.end()) {
          empty_slots_.erase(it);
          num_slots_--;
        }
      }
    }
    else {
      empty_slots_.insert(slot_id);
    }
    return true;
  }
  return false;
}

bool RecordPageMeta::AddEmptySlot(int slot_id) {
  if (slot_directory_[slot_id].offset() >= 0) {
    LogERROR("[Can't add to empty slot list] - slot[%d] has offset %d >= 0",
             slot_id, slot_directory_[slot_id].offset());
    return false;
  }
  empty_slots_.insert(slot_id);
  return true;
}

int RecordPageMeta::size() const {
  // Slot directory entry size = 2 * sizeof(int)
  return (sizeof(int) * 5) + kSlotDirectoryEntrySize * slot_directory_.size();
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
    LogERROR("[Save page meta data error] - num_slots inconsistency (%d, %d)",
             num_slots_, slot_directory_.size());
    return false;
  }
  if (num_records_ + (int)empty_slots_.size() != num_slots_) {
    LogERROR("[Save page meta data error] - "
             "records %d + empty_slots %d != num_slots %d",
             num_records_, (int)empty_slots_.size(), num_slots_);
    return false;
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
    offset -= kSlotDirectoryEntrySize;
    slot.SaveToMem(ppage + offset);
    if (slot.offset() >= 0) {
      valid_records ++;
    }
  }

  if (num_records_ != valid_records) {
    LogERROR("[Save page meta data error] - num_records inconsistency (%d, %d)",
             num_records_, valid_records);
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

  if (num_records_ > num_slots_) {
    LogERROR("[Load page meta data error] - num_records_ >  num_slots_ (%d, %d)",
             num_records_, num_slots_);
    return false;
  }

  offset -= sizeof(free_start_);
  memcpy(&free_start_, ppage + offset, sizeof(free_start_));

  offset -= sizeof(next_page_);
  memcpy(&next_page_, ppage + offset, sizeof(next_page_));

  offset -= sizeof(prev_page_);
  memcpy(&prev_page_, ppage + offset, sizeof(prev_page_));

  // Load slot directory.
  int valid_records = 0;
  for (int i = 0; i < num_slots_; i++) {
    offset -= kSlotDirectoryEntrySize;
    slot_directory_.emplace_back(-1, 0);
    slot_directory_.back().LoadFromMem(ppage + offset);
    if (slot_directory_.back().offset() >= 0) {
      valid_records++;
    }
    else {
      empty_slots_.insert(i);
    }
  }

  // Consistency checks for loaded page meta.
  if (num_slots_ != (int)slot_directory_.size()) {
    LogERROR("[Load page meta data error] - num_slots inconsistency (%d, %d)",
             num_slots_, slot_directory_.size());
    return false;
  }
  if (num_records_ + (int)empty_slots_.size() != num_slots_) {
    LogERROR("[Load page meta data error] - "
             "records %d + empty_slots %d != num_slots %d",
             num_records_, (int)empty_slots_.size(), num_slots_);
    return false;
  }
  if (num_records_ != valid_records) {
    LogERROR("[Load page meta data error] - num_records inconsistency (%d, %d)",
             num_records_, valid_records);
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

void RecordPage::InitInMemoryPage() {
  page_meta_.reset(new RecordPageMeta());
  data_.reset(new byte[kPageSize]);
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

bool RecordPage::ReorganizeRecords() {
  byte* new_page = new byte[kPageSize];
  int offset = 0;
  for (auto& slot: page_meta_->slot_directory()) {
    if (slot.offset() >= 0) {
      memcpy(new_page + offset, data_.get() + slot.offset(), slot.length());
      slot.set_offset(offset);
      offset += slot.length();
    }
  }
  data_.reset(new_page);

  page_meta_->set_free_start(offset);
  // (TODO: need this?) : save meta to the new page.
  //bool success = page_meta_->SaveMetaToPage(new_page);
  return true;
}

bool RecordPage::InsertRecord(const byte* content, int length) {
  // Allocate a slot id for the new record.
  int slot_id = page_meta_->AllocateSlotAvailable();
  bool reorganized = false;
  // No space for appending new slot id. Re-organize records and try again.
  if (slot_id < 0) {
    reorganized = ReorganizeRecords();
    slot_id = page_meta_->AllocateSlotAvailable();
    if (slot_id < 0) {
      //LogINFO("Tried best, no space available for new slot id");
      return false;
    }
  }

  std::vector<SlotDirectoryEntry>& slot_dir = page_meta_->slot_directory();

  if (FreeSize() < length) {
    // Re-organize records and re-try inserting new record.
    if (!reorganized) {
      ReorganizeRecords();
    }
    if (FreeSize() < length) {
      // Rollback the newly allocated slot entry.
      if (slot_dir.back().offset() < 0) {
        slot_dir.pop_back();
        page_meta_->decrement_num_slots(1);
      }
      else {
        page_meta_->AddEmptySlot(slot_id);
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

bool RecordPage::DeleteRecord(int slot_id) {
  if (slot_id < 0) {
    return false;
  }
  if (!page_meta_->ReleaseSlot(slot_id)) {
    return false;
  }

  // (TODO: need this?) Re-write meta data to page.
  if (!page_meta_->SaveMetaToPage(data_.get())) {
    return false;
  }

  return true;
}

}  // namespace DataBaseFiles
