#ifndef DATABASEFILES_HEADERPAGE_
#define DATABASEFILES_HEADERPAGE_

#include "Base/MacroUtils.h"
#include "Schema/SchemaType.h"

namespace DataBaseFiles {

// Page Type
enum PageType {
  INDEX,
  INDEX_DATA,
  HEAPFILE,
};


class HeaderPage {
 public:
  // Constructors
  HeaderPage() = default;

  // Accessors
  DEFINE_ACCESSOR_ENUM(record_type, PageType);
  DEFINE_ACCESSOR(num_pages, int);
  DEFINE_ACCESSOR(num_free_pages, int);
  DEFINE_ACCESSOR(num_used_pages, int);
  DEFINE_ACCESSOR(free_page, int);

  virtual bool DumpToMem(byte* buf) const = 0;
  virtual bool LoadFromMem(const byte* buf) = 0;

 protected:
  PageType record_type_ = INDEX;
  int num_pages_ = 0;
  int num_free_pages_ = 0;
  int num_used_pages_ = 0;
  int free_page_ = -1;  // header of free pages

  // TOOD: list of fields ?
};


class FreePage {
 public:
  FreePage() = default;
  FreePage(int prev, int next) : prev_free_page_(prev), next_free_page_(next) {}

  DEFINE_ACCESSOR(prev_free_page, int);
  DEFINE_ACCESSOR(next_free_page, int);

 private:
  int prev_free_page_ = -1;
  int next_free_page_ = -1;
};


}  // namespace DataBaseFiles


#endif  /* DATABASEFILES_HEADERPAGE_ */
