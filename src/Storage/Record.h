#ifndef DATABASEFILES_RECORDPAGE_
#define DATABASEFILES_RECORDPAGE_

#include <vector>

#include "Base/BaseTypes.h"
#include "Base/MacroUtils.h"

namespace DataBaseFiles {

class RecordID {
 public:
  RecordID(int page_id, int slot_id) : page_id_(page_id), slot_id_(slot_id) {}

  // Accessors
  DEFINE_ACCESSOR(page_id, int);
  DEFINE_ACCESSOR(slot_id, int);

 private:
  int page_id_ = 0;
  int slot_id_ = 0;
};

class IndexRecord {
 public:
  IndexRecord() = default;
  ~IndexRecord();

 private:
  byte* key_ = nullptr;  // key of this index
  std::vector<RecordID> rids_;  // data entry alternative 2 & 3
};


}  // namespace DataBaseFiles


#endif  /* DATABASEFILES_RECORDPAGE_ */
