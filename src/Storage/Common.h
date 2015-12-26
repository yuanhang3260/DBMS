#ifndef DATABASEFILES_COMMON_
#define DATABASEFILES_COMMON_

#include "string.h"

#include "Base/BaseTypes.h"

namespace DataBaseFiles {

const int kPageSize = 4096;
const int kSlotDirectoryEntrySize = 4;
const char* const kDataDirectory = "data/";

// File Type
enum FileType {
  UNKNOWN_FILETYPE,
  INDEX,
  INDEX_DATA,
  HEAPFILE,
};

bool ContentEqual (const byte* data1, const byte* data2, int length);

}  // namespace DataBaseFiles


#endif  /* DATABASEFILES_COMMON_ */
