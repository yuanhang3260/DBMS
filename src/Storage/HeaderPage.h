#ifndef DATABASEFILES_HEADERPAGE_
#define DATABASEFILES_HEADERPAGE_

#include "Base/MacroUtils.h"
#include "Schema/SchemaType.h"

namespace DataBaseFiles {

// Record Type
enum RecordType {
  INDEX,
  INDEX_DATA,
  HEAPFILE,
};


class HeaderPage {
 public:
  // Constructors
  HeaderPage() = default;

  // Accessors
  DEFINE_ACCESSOR_ENUM(record_type, RecordType);
  DEFINE_ACCESSOR(num_pages, int);
  DEFINE_ACCESSOR(root_page, int);
  DEFINE_ACCESSOR(free_page, int);

 private:
  RecordType record_type_ = INDEX;
  int num_pages_ = 0;
  int root_page_ = -1;  // root page id for B+ tree
  // TODO: meta pages of heapfile and hash-based index file
  int free_page_ = -1;  // header of free pages

  // TOOD: list of fields
};


}  // namespace DataBaseFiles


#endif  /* DATABASEFILES_HEADERPAGE_ */
