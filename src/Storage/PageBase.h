#ifndef STORAGE_HEADERPAGE_
#define STORAGE_HEADERPAGE_

#include <stdio.h>
#include <stdlib.h>

#include "Base/MacroUtils.h"
#include "Common.h"

namespace Storage {

class HeaderPage {
 public:
  // Constructors
  HeaderPage() = default;
  HeaderPage(FileType file_type) : file_type_(file_type) {}
  HeaderPage(FILE* file): file_(file) {}
  HeaderPage(FILE* file, FileType file_type) :
      file_(file),
      file_type_(file_type) {
  }

  // Accessors
  DEFINE_ACCESSOR(file, FILE*);
  DEFINE_ACCESSOR_ENUM(file_type, FileType);
  DEFINE_ACCESSOR(num_pages, int);
  DEFINE_INCREMENTOR_DECREMENTOR(num_pages, int);
  DEFINE_ACCESSOR(num_free_pages, int);
  DEFINE_INCREMENTOR_DECREMENTOR(num_free_pages, int);
  DEFINE_ACCESSOR(num_used_pages, int);
  DEFINE_INCREMENTOR_DECREMENTOR(num_used_pages, int);
  DEFINE_ACCESSOR(free_page, int);

  // Dump page to memory
  virtual bool DumpToMem(byte* buf) const = 0;
  // Parse page from memory
  virtual bool ParseFromMem(const byte* buf) = 0;

  // Save header page to disk.
  virtual bool SaveToDisk() const = 0;
  // Load header page from disk.
  virtual bool LoadFromDisk() = 0;

 protected:
  FILE* file_ = nullptr;
  FileType file_type_ = INDEX;
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


}  // namespace Storage


#endif  /* STORAGE_HEADERPAGE_ */
