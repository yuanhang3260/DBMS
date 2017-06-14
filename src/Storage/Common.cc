#include "Base/Path.h"

#include "Storage/Common.h"

namespace Storage{

bool ContentEqual (const byte* data1, const byte* data2, int length) {
  return strncmp((const char*)data1, (const char*)data2, length) == 0;
}


std::string DBDataDir(const std::string& db_name) {
  return Path::JoinPath(Storage::kDataDirectory, db_name);
}

}  // namespace Storage
