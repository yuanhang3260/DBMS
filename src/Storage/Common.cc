#include "Common.h"

namespace Storage{

bool ContentEqual (const byte* data1, const byte* data2, int length) {
  return strncmp((const char*)data1, (const char*)data2, length) == 0;
}

}  // namespace Storage
