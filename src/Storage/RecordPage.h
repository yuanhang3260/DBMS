#ifndef DATABASEFILES_RECORDSLOT_
#define DATABASEFILES_RECORDSLOT_

#include <vector>

#include "Record.h"

namespace DataBaseFiles {

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


class RecordPageMeta {
 public:
  // Constructors
  RecordPageMeta() = default;

  // Accessors
  DEFINE_ACCESSOR(num_slots, int);
  DEFINE_ACCESSOR(num_records, int);
  DEFINE_ACCESSOR(free_start, int);

  std::vector<SlotDirectoryEntry>& slot_directory();

 private:
  int num_slots_ = 0;
  int num_records_ = 0;
  int free_start_ = 0;  // offset of free space in this page
  std::vector<SlotDirectoryEntry> slot_directory_;
};


}  // namespace DataBaseFiles


#endif  /* DATABASEFILES_RECORDSLOT_ */
