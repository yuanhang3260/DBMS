#ifndef DATABASEFILES_RECORDSLOT_
#define DATABASEFILES_RECORDSLOT_

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <vector>
#include <memory>
#include <string>

#include "Common.h"
#include "Record.h"

namespace DataBaseFiles {

// entry of slot directory.
class SlotDirectoryEntry {
 public:
  // Constructors
  SlotDirectoryEntry(int offset, int length) :
      offset_(offset),
      length_(length) {}

  // Accessors
  DEFINE_ACCESSOR(offset, int);
  DEFINE_ACCESSOR(length, int);

 private:
  int offset_ = -1;  // Slot offset in this page
  int length_ = 0;  // Slot length
};


// Record page meta data.
// | num_slots | num_records | free_start  | slot 0 | slot 1 | slot 2 | ... |
// |  4 bytes  |   4 bytes   |  4 bytes    |          num_slots * 8         |
class RecordPageMeta {
 public:
  // Constructors
  RecordPageMeta() = default;

  // Accessors
  DEFINE_ACCESSOR(num_slots, int);
  DEFINE_ACCESSOR(num_records, int);
  DEFINE_ACCESSOR(free_start, int);
  DEFINE_ACCESSOR(next_page, int);
  DEFINE_ACCESSOR(prev_page, int);

  std::vector<SlotDirectoryEntry>& slot_directory();

  // Page meta data dump and load.
  int size() const;

 private:
  int num_slots_ = 0;
  int num_records_ = 0;
  int free_start_ = 0;  // offset of free space in this page
  int next_page_ = -1;
  int prev_page_ = -1;
  std::vector<SlotDirectoryEntry> slot_directory_;
};


// Record page
class RecordPage {
 public:
  RecordPage(int id) : id_(id) {}
  RecordPage(int id, FILE* file) : id_(id), file_(file) {}
  ~RecordPage();

  // Accessors
  DEFINE_ACCESSOR(id, int);
  RecordPageMeta* Meta() { return page_meta_.get(); }
  byte* Data() { return data_; }

  // Get free size available on this page.
  int FreeSize() const;
  // Dump page data to file.
  bool DumpPageData();
  // Load this page from file.
  bool LoadPageData();

 private:
  int id_ = -1;  // page id
  std::unique_ptr<RecordPageMeta> page_meta_;
  FILE* file_ = nullptr;
  byte* data_ = nullptr;
};

}  // namespace DataBaseFiles


#endif  /* DATABASEFILES_RECORDSLOT_ */
