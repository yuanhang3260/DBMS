#ifndef STORAGE_RECORDSPAGE_
#define STORAGE_RECORDSPAGE_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "Base/MacroUtils.h"

namespace Storage {

enum PageType {
  UNKNOW_PAGETYPE = 0,
  TREE_ROOT = 1,
  TREE_NODE = 2,
  TREE_LEAVE = 3,
};

// entry of slot directory.
class SlotDirectoryEntry {
 public:
  // Constructors
  SlotDirectoryEntry(int16 offset, int16 length) :
      offset_(offset),
      length_(length) {}

  // Accessors
  DEFINE_ACCESSOR(offset, int16);
  DEFINE_ACCESSOR(length, int16);

  // Save and load
  void SaveToMem(byte* buf) const;
  void LoadFromMem(const byte* buf);

 private:
  int16 offset_ = -1;  // Slot offset in this page
  int16 length_ = 0;  // Slot length
};


// Record page meta data.
class RecordPageMeta {
 public:
  // Constructors
  RecordPageMeta() = default;

  // Accessors
  DEFINE_ACCESSOR(num_slots, int16);
  DEFINE_INCREMENTOR_DECREMENTOR(num_slots, int16);
  DEFINE_ACCESSOR(num_records, int16);
  DEFINE_INCREMENTOR_DECREMENTOR(num_records, int16);
  DEFINE_ACCESSOR(free_start, int16);
  DEFINE_INCREMENTOR_DECREMENTOR(free_start, int16);
  DEFINE_ACCESSOR(next_page, int32);
  DEFINE_ACCESSOR(prev_page, int32);
  DEFINE_ACCESSOR(parent_page, int32);
  DEFINE_ACCESSOR(is_overflow_page, byte);
  DEFINE_ACCESSOR(overflow_page, int32);
  DEFINE_ACCESSOR(space_used, int16);
  DEFINE_INCREMENTOR_DECREMENTOR(space_used, int16);
  DEFINE_ACCESSOR_ENUM(page_type, PageType);

  // Get slot directory.
  const std::vector<SlotDirectoryEntry>& slot_directory() const;
  std::vector<SlotDirectoryEntry>* mutable_slot_directory();

  // Total size of meta data.
  int size() const;
  // Reset all fields.
  void reset();
  // Save meta data to page.
  bool SaveMetaToMem(byte* ppage) const;
  // Load meta data from page.
  bool LoadMetaFromMem(const byte* ppage);

  // Get a unused slot id avaible.
  int16 AllocateSlotAvailable();
  // Release a slot.
  bool ReleaseSlot(int16 slotid);
  // Number of empty slots available.
  int NumEmptySlots() const { return empty_slots_.size(); }
  // Add an empty slot entry.
  bool AddEmptySlot(int16 slot_id);

 private:
  int16 num_slots_ = 0;
  int16 num_records_ = 0;
  int16 free_start_ = 0;  // offset of free space in this page
  int32 next_page_ = -1;
  int32 prev_page_ = -1;
  int32 parent_page_ = -1;
  byte is_overflow_page_ = 0;
  int32 overflow_page_ = -1;
  int16 space_used_ = 0;  // total space that has been used for record.
  PageType page_type_ = UNKNOW_PAGETYPE;  // save as 1 byte
  std::vector<SlotDirectoryEntry> slot_directory_;

  // A set of empty slots id. It is created when loading a page. New record
  // insert operation will first check this list to find a slot available.
  std::set<int16> empty_slots_;
};


// Record page
class RecordPage {
 public:
  RecordPage(int id) : id_(id) {}
  RecordPage(int id, FILE* file) : id_(id), file_(file) {}
  ~RecordPage();

  // Accessors
  DEFINE_ACCESSOR(id, int);
  DEFINE_ACCESSOR(valid, bool);
  RecordPageMeta* Meta() { return page_meta_.get(); }
  byte* data() { return data_.get(); }

  // Init a page in memory.
  void InitInMemoryPage();

  // Dump page data to file.
  bool DumpPageData();
  // Load this page from file.
  bool LoadPageData();

  // Get free size available on this page. This is pre-reorganizing free space,
  // that is, counting from the start of free space pointer to the beginning of
  // meta data area. It won't count in empty record slots among records.
  int FreeSize() const;
  // Space occupied by record data.
  double Occupation() const;
  // Re-organize records.
  bool ReorganizeRecords();
  // Pre-check if some records can be inserted to this page.
  bool PreCheckCanInsert(int num_records, int total_size);
  // Pre-check if some records can be inserted to an empty page.
  //static bool PreCheckCanFitInEmptyPage(int num_records, int total_size);

  // Return the data pointer to a record.
  byte* Record(int16 slot_id) const;

  // Record length.
  int RecordLength(int16 slot_id) const;

  // Insert a record. This function only reserved space in page but no actual
  // record data will be copied yet.
  int16 InsertRecord(int length);
  // Insert a record with data.
  int16 InsertRecord(const byte* content, int length);

  // Delete a record
  bool DeleteRecord(int16 slot_id);
  // Delete a number of records.
  bool DeleteRecords(int16 from, int16 end);

  // Clear this page.
  void Clear();

 private:
  int id_ = -1;  // page id
  bool valid_ = false;
  std::unique_ptr<RecordPageMeta> page_meta_;
  FILE* file_ = nullptr;
  std::unique_ptr<byte> data_;
};

}  // namespace Storage


#endif  /* STORAGE_RECORDSPAGE_ */
