#include "RecordPage.h"

namespace DataBaseFiles {

std::vector<SlotDirectoryEntry>& RecordPageMeta::slot_directory() {
  return slot_directory_;
}

}  // namespace DataBaseFiles
