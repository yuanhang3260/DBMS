#include "Record.h"

namespace DataBaseFiles {

IndexRecord::~IndexRecord() {
  if (key_) {
    delete key_;
  }
}

}  // namespace DataBaseFiles
