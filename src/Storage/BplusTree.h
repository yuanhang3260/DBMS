#ifndef STORAGE_BPLUS_TREE_
#define STORAGE_BPLUS_TREE_

#include "PageBase.h"

namespace DataBaseFiles {

class BplusTreeHeaderPage: public HeaderPage {
 public:
  BplusTreeHeaderPage(PageType page_type);

  DEFINE_ACCESSOR(root_page, int);
  DEFINE_ACCESSOR(num_leaves, int);
  DEFINE_ACCESSOR(depth, int);

  bool DumpToMem(byte* buf) const override;
  bool LoadFromMem(const byte* buf) override;

 private:
  bool ConsistencyCheck() const;

  int root_page_ = -1;
  int num_leaves_ = 0;
  int depth_ = 0;
};

}

#endif  /* STORAGE_BPLUS_TREE_ */