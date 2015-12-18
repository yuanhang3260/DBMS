#ifndef DATABASEFILES_COMMON_
#define DATABASEFILES_COMMON_

#include "string.h"

#include "Base/BaseTypes.h"

namespace DataBaseFiles {

const int kPageSize = 4096;
const int kSlotDirectoryEntrySize = 4;

bool ContentEqual (const byte* data1, const byte* data2, int length) {
  return strncmp((const char*)data1, (const char*)data2, length) == 0;
}

}  // namespace DataBaseFiles


#endif  /* DATABASEFILES_COMMON_ */