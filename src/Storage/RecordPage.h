#ifndef DATABASEFILES_RECORDSLOT_
#define DATABASEFILES_RECORDSLOT_

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <vector>
#include <set>
#include <memory>
#include <string>

#include "Base/MacroUtils.h"

namespace DataBaseFiles {

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
// | num_slots | num_records | free_start  | slot 0 | slot 1 | slot 2 | ... |
// |  4 bytes  |   4 bytes   |  4 bytes    |          num_slots * 8         |
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
  DEFINE_ACCESSOR(next_page, int16);
  DEFINE_ACCESSOR(prev_page, int16);
  DEFINE_ACCESSOR(parent_page, int16);
  DEFINE_ACCESSOR(is_overflow_page, int16);
  DEFINE_ACCESSOR(overflow_page, int16);
  DEFINE_ACCESSOR(space_used, int16);
  DEFINE_INCREMENTOR_DECREMENTOR(space_used, int16);
  DEFINE_ACCESSOR_ENUM(page_type, PageType);

  // Get slot directory.
  std::vector<SlotDirectoryEntry>& slot_directory();

  // Page meta data dump and load.
  int size() const;
  // Reset all fields.
  void reset();
  // Save meta data to page.
  bool SaveMetaToPage(byte* ppage) const;
  // Load meta data from page.
  bool LoadMetaFromPage(const byte* ppage);

  // Get a unused slot id avaible.
  int AllocateSlotAvailable();
  // Release a slot.
  bool ReleaseSlot(int slotid);
  // Number of empty slots available.
  int NumEmptySlots() const { return empty_slots_.size(); }
  // Add an empty slot entry.
  bool AddEmptySlot(int slot_id);

 private:
  int16 num_slots_ = 0;
  int16 num_records_ = 0;
  int16 free_start_ = 0;  // offset of free space in this page
  int16 next_page_ = -1;
  int16 prev_page_ = -1;
  int16 parent_page_ = -1;
  int16 is_overflow_page_ = 0;
  int16 overflow_page_ = -1;
  int16 space_used_ = 0;  // total space that has been used for record.
  PageType page_type_ = UNKNOW_PAGETYPE;
  std::vector<SlotDirectoryEntry> slot_directory_;

  // A set of empty slots id. It is created when loading a page. New record
  // insert operation will first check this list to find a slot available.
  std::set<int> empty_slots_;
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

  byte* Record(int slot_id) const;

  // Insert a record. This function only reserved space in page but no actual
  // record data will be copied yet.
  byte* InsertRecord(int length);
  // Insert a record with data.
  bool InsertRecord(const byte* content, int length);

  // Delete a record
  bool DeleteRecord(int slotid);
  // Delete a number of records.
  bool DeleteRecords(int from, int end);

 private:
  int id_ = -1;  // page id
  bool valid_ = false;
  std::unique_ptr<RecordPageMeta> page_meta_;
  FILE* file_ = nullptr;
  std::unique_ptr<byte> data_;
};

}  // namespace DataBaseFiles


#endif  /* DATABASEFILES_RECORDSLOT_ */
