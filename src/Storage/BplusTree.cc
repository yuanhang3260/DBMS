#include <sys/stat.h>
#include <string.h>

#include "Base/Log.h"
#include "Base/Utils.h"
#include "BplusTree.h"

namespace DataBaseFiles {

// ************************ BplusTreeHeaderPage ******************************//
BplusTreeHeaderPage::BplusTreeHeaderPage(FileType file_type) :
    HeaderPage(file_type) {
}

BplusTreeHeaderPage::BplusTreeHeaderPage(FILE* file) :
    HeaderPage(file) {
}

BplusTreeHeaderPage::BplusTreeHeaderPage(FILE* file, FileType file_type) :
    HeaderPage(file, file_type) {
}

bool BplusTreeHeaderPage::DumpToMem(byte* buf) const {
  if (!buf) {
    LogERROR("buf nullptr");
    return false;
  }

  if (!ConsistencyCheck("Dump")) {
    LogERROR("ConsistencyCheck failed");
    return false;
  }

  int offset = 0;
  // record type
  memcpy(buf + offset, &file_type_, sizeof(file_type_));
  offset += sizeof(file_type_);
  // num_pages
  memcpy(buf + offset, &num_pages_, sizeof(num_pages_));
  offset += sizeof(num_pages_);
  // num_free_pages
  memcpy(buf + offset, &num_free_pages_, sizeof(num_free_pages_));
  offset += sizeof(num_free_pages_);
  // num_used_pages
  memcpy(buf + offset, &num_used_pages_, sizeof(num_used_pages_));
  offset += sizeof(num_used_pages_);
  // first free_page id
  memcpy(buf + offset, &free_page_, sizeof(free_page_));
  offset += sizeof(free_page_);
  // root_page id
  memcpy(buf + offset, &root_page_, sizeof(root_page_));
  offset += sizeof(root_page_);
  // num_leaves
  memcpy(buf + offset, &num_leaves_, sizeof(num_leaves_));
  offset += sizeof(num_leaves_);
  // depth
  memcpy(buf + offset, &depth_, sizeof(depth_));
  offset += sizeof(depth_);

  return true;
}


bool BplusTreeHeaderPage::ParseFromMem(const byte* buf) {
  if (!buf) {
    LogERROR("nullptr");
    return false;
  }

  int offset = 0;
  // record type
  memcpy(&file_type_, buf + offset, sizeof(file_type_));
  offset += sizeof(file_type_);
  // num_pages
  memcpy(&num_pages_, buf + offset, sizeof(num_pages_));
  offset += sizeof(num_pages_);
  // num_free_pages
  memcpy(&num_free_pages_, buf + offset, sizeof(num_free_pages_));
  offset += sizeof(num_free_pages_);
  // num_used_pages
  memcpy(&num_used_pages_, buf + offset, sizeof(num_used_pages_));
  offset += sizeof(num_used_pages_);
  // first free_page id
  memcpy(&free_page_, buf + offset, sizeof(free_page_));
  offset += sizeof(free_page_);
  // root_page id
  memcpy(&root_page_, buf + offset, sizeof(root_page_));
  offset += sizeof(root_page_);
  // num_leaves
  memcpy(&num_leaves_, buf + offset, sizeof(num_leaves_));
  offset += sizeof(num_leaves_);
  // depth
  memcpy(&depth_, buf + offset, sizeof(depth_));
  offset += sizeof(depth_);

  // Do consistency check.
  if (!ConsistencyCheck("Load")) {
    return false;
  }

  return true;
}

bool BplusTreeHeaderPage::SaveToDisk() const {
  if (!file_) {
    LogERROR("FILE is nullptr");
    return false;
  }

  byte buf[kPageSize];
  if (!DumpToMem(buf)) {
    LogERROR("DumpToMem(buf)");
    return false;
  }

  fseek(file_, 0, SEEK_SET);
  int nwrite = fwrite(buf, 1, kPageSize, file_);
  if (nwrite != kPageSize) {
    LogERROR("nwrite = %d", nwrite);
    return false;
  }
  fflush(file_);
  return true;
}

bool BplusTreeHeaderPage::LoadFromDisk() {
  if (!file_) {
    LogERROR("FILE is nullptr");
    return false;
  }

  byte buf[kPageSize];

  fflush(file_);
  fseek(file_, 0, SEEK_SET);
  int nread = fread(buf, 1, kPageSize, file_);
  if (nread != kPageSize) {
    LogERROR("nread = %d", nread);
    return false;
  }

  if (!ParseFromMem(buf)) {
    LogERROR("ParseFromMem(buf)");
    return false;
  }
  return true;
}

bool BplusTreeHeaderPage::ConsistencyCheck(const char* op) const {
  if (num_pages_ != num_free_pages_ + num_used_pages_) {
    LogERROR("num_pages !=  num_used_pages_ + num_free_pages_, "
             "(%d != %d + %d)", op,
             num_pages_, num_used_pages_, num_free_pages_);
    return false;
  }

  if ((num_free_pages_ > 0 && free_page_ < 0) ||
      (num_free_pages_ <= 0 && free_page_ >= 0)) {
    LogERROR(""
             "num_free_pages_ = %d, first_free_page_id = %d", op,
             num_free_pages_, free_page_);
    return false;
  }

  if (root_page_ < 0 && (num_leaves_ > 0 || depth_ > 0)) {
    LogERROR(""
             "Empty tree, but num_leaves_ = %d, depth_ = %d", op,
             num_leaves_, depth_);
    return false;
  }

  return true;
}


// ****************************** BplusTree **********************************//
BplusTree::~BplusTree() {
  if (file_) {
    // Checkout all remaining pages. We don't call CheckoutPage() since it
    // involves map.erase() operation internally which is tricky.
    for (auto& entry: page_map_) {
      if (!entry.second->DumpPageData()) {
        LogERROR("Save page %d to disk failed", entry.first);
      }
    }
    page_map_.clear();
    SaveToDisk();
    fflush(file_);
    fclose(file_);
  }
}

BplusTree::BplusTree(std::string tablename, std::vector<int> key_indexes) :
    tablename_(tablename),
    key_indexes_(key_indexes) {
  std::string btree_filename = GenerateBplusTreeFilename(UNKNOWN_FILETYPE);

  file_ = fopen(btree_filename.c_str(), "a+");
  if (!file_) {
    LogERROR("file name %s", btree_filename.c_str());
    throw std::runtime_error("Can't init B+ tree");
  }

  // Load table schema, header page and root node.
  if (!LoadFromDisk()) {
    throw std::runtime_error("Can't init B+ tree");
  }
}

std::string BplusTree::GenerateBplusTreeFilename(FileType file_type) {
  std::string filename = kDataDirectory + tablename_ + "(";
  for (int index: key_indexes_) {
    filename += std::to_string(index) + "_";
  }
  filename += ")";
  
  if (file_type == UNKNOWN_FILETYPE) {
    // Check file type.
    // Data file.
    std::string fullname = filename + ".indata";
    if (access(fullname.c_str(), F_OK) != -1) {
      file_type_ = INDEX_DATA;
      return fullname;
    }

    // Index file.
    fullname = filename + ".index";
    if (access(fullname.c_str(), F_OK) != -1) {
      file_type_ = INDEX;
      return fullname;
    }
  }
  else if (file_type == INDEX_DATA) {
    return filename + ".indata";
  }
  else if (file_type == INDEX) {
    return filename + ".index";
  }
  return "unknown_filename";
}

bool BplusTree::LoadSchema() {
  std::string schema_filename = kDataDirectory + tablename_ + ".schema.pb";

  struct stat stat_buf;
  int re = stat(schema_filename.c_str(), &stat_buf);
  if (re < 0) {
    LogERROR("Failed to stat schema file %s", schema_filename.c_str());
    return false;
  }

  int size = stat_buf.st_size;
  FILE* file = fopen(schema_filename.c_str(), "r");
  if (!file) {
    LogERROR("Failed to open schema file %s", schema_filename.c_str());
    return false;
  }
  // Read schema file.
  char buf[size];
  re = fread(buf, 1, size, file);
  if (re != size) {
    LogERROR("Read schema file %s error, expect %d bytes, actual %d",
             schema_filename.c_str(), size, re);
    return false;
  }
  fclose(file);
  // Parse TableSchema proto data.
  schema_.reset(new Schema::TableSchema());
  schema_->DeSerialize(buf, size);
  return true;
}

bool BplusTree::LoadHeaderPage() {
  if (!file_) {
    LogERROR("FILE is nullptr");
    return false;
  }

  if (!header_) {
    header_.reset(new BplusTreeHeaderPage(file_));
  }

  if (!header_->LoadFromDisk()) {
    LogERROR("Load from disk failed");
    return false;
  }
  return true;
}

bool BplusTree::LoadRootNode() {
  if (!header_) {
    LogERROR("B+ tree header page not loaded");
    return false;
  }
  int root_page_id = header_->root_page();
  if (root_page_id > 0) {
    page_map_[root_page_id] = std::make_shared<RecordPage>(root_page_id, file_);
    page_map_.at(root_page_id)->LoadPageData();
  }
  else {
    LogINFO("LoadRootNodroot_page_id = %d", root_page_id);
  }
  return true;
}

RecordPage* BplusTree::root() {
  if (!header_) {
    LogERROR("B+ tree header page not loaded");
    return nullptr;
  }
  int root_page_id = header_->root_page();
  if (root_page_id > 0) {
    if (page_map_.find(root_page_id) == page_map_.end()) {
      LogERROR("can't root page id %d in page map", root_page_id);
      return nullptr;
    }
    return page_map_.at(root_page_id).get();
  }
  return nullptr;
}

bool BplusTree::CreateFile(std::string tablename,
                           std::vector<int> key_indexes,
                           FileType file_type) {
  tablename_ = tablename;
  key_indexes_ = key_indexes;
  file_type_ = file_type;

  if (!schema_) {
    if (!LoadSchema()) {
      LogERROR("Failed to load schema while creating new B+ tree for table %s",
               tablename_.c_str());
      return false;
    }
  }

  std::string filename = GenerateBplusTreeFilename(file_type);
  if (file_) {
    LogINFO("Existing B+ tree file, closing it ...");
    fclose(file_);
  }

  file_ = fopen(filename.c_str(), "w+");
  if (!file_) {
    LogERROR("open file %s failed", filename.c_str());
    return false;
  }

  // Create header page.
  header_.reset(new BplusTreeHeaderPage(file_, file_type));

  return true;
}

bool BplusTree::SaveToDisk() const {
  // Save header page
  if (header_ && !header_->SaveToDisk()) {
    return false;
  }

  // (TODO) Save B+ tree nodes. Root node, tree node and leaves.
  return true;
}

bool BplusTree::LoadFromDisk() {
  if (!LoadSchema()) {
    return false;
  }

  if (!LoadHeaderPage()) {
    return false;
  }

  // Load root node if exists. Add it to PageMap.
  if (!LoadRootNode()) {
    return false;
  }

  return true;
}

RecordPage* BplusTree::AllocateNewPage(PageType page_type) {
  RecordPage* page = new RecordPage(next_id++, file_);
  page->InitInMemoryPage();
  page->Meta()->set_page_type(page_type);
  page_map_[page->id()] = std::shared_ptr<RecordPage>(page);
  return page;
}

RecordPage* BplusTree::FetchPage(int page_id) {
  if (page_map_.find(page_id) != page_map_.end()) {
    return page_map_.at(page_id).get();
  }
  else {
    // load page from disk and cache it.
    RecordPage* new_page = new RecordPage(page_id, file_);
    if (!new_page->LoadPageData()) {
      LogERROR("Fetch page %d from disk failed", page_id);
      return nullptr;
    }
    page_map_.emplace(page_id, std::shared_ptr<RecordPage>(new_page));
    return new_page;
  }
}

bool BplusTree::CheckoutPage(int page_id) {
  if (page_map_.find(page_id) == page_map_.end()) {
    LogINFO("page %d not in page map, won't checkout");
    return false;
  }
  if (!page_map_.at(page_id)->DumpPageData()) {
    LogERROR("Save page %d to disk failed", page_id);
    return false;
  }
  page_map_.erase(page_id);
  return true;
}

bool BplusTree::InsertRecordToLeave(const Schema::DataRecord* record,
                                    RecordPage* leave) {
  return record->InsertToRecordPage(leave);
}

bool BplusTree::InsertTreeNodeRecord(Schema::TreeNodeRecord* tn_record,
                                     RecordPage* tn_page) {
  if (!tn_record || !tn_page) {
    LogERROR("nullptr passed to InsertTreeNodeRecord");
    return false;
  }

  if (tn_page->Meta()->page_type() != TREE_NODE) {
    LogERROR("Target RecordPage is not tree node");
    return false;
  }
  
  if (tn_record->InsertToRecordPage(tn_page)) {
    // Success, and we're done. Get the child page related with this new
    // TreeNode record and set its parent page id as this tree node.
    RecordPage* child_page = FetchPage(tn_record->page_id());
    child_page->Meta()->set_parent_page(tn_page->id());
    return true;
  }

  // The parent is full and needs to be split.
  Schema::PageRecordsManager prmanager(tn_page,
                                       schema_.get(), key_indexes_,
                                       file_type_,
                                       tn_page->Meta()->page_type());
  if (!prmanager.LoadRecordsFromPage()) {
    LogFATAL("Load page record failed");
  }

  int mid_index = prmanager.InsertRecordAndSplitPage(tn_record);
  if (mid_index < 0) {
    LogERROR("Failed to add new TreeNodeRecord to page");
    return false;
  }

  // Allocate a new TreeNoe page and move the second half of current TreeNode
  // records to the new one.
  RecordPage* new_tree_node = AllocateNewPage(TREE_NODE);
  const auto& plrecords = prmanager.plrecords();
  // Left-half records stay in the original tree node.
  for (int i = 0; i < mid_index; i++) {
    if (plrecords.at(i).slot_id() < 0) {
      // It's the inserted new TreeNodeRecord. Now we inserted it to RecordPage.
      if (!tn_record->InsertToRecordPage(tn_page)) {
        LogERROR("Insert to TreeNodeRecord to left-half tree node failed");
        return false;
      }
      RecordPage* child_page = FetchPage(tn_record->page_id());
      if (child_page) {
        child_page->Meta()->set_parent_page(tn_page->id());
      }
    }
  }

  // Rigth-half records go to new (split out) tree node.
  for (int i = mid_index; i < (int)plrecords.size(); i++) {
    if (!prmanager.Record(i)->InsertToRecordPage(new_tree_node)) {
      LogERROR("Move slot %d TreeNodeRecord to new tree node failed");
      return false;
    }
    // Set new parent.
    int child_page_id =
        reinterpret_cast<Schema::TreeNodeRecord*>(prmanager.Record(i))
            ->page_id();
    RecordPage* child_page = FetchPage(child_page_id);
    if (child_page) {
      child_page->Meta()->set_parent_page(new_tree_node->id());
    }
    // Remove the TreeNodeRecord from the original tree node (left one).
    if (plrecords.at(i).slot_id() >= 0) {
      tn_page->DeleteRecord(plrecords.at(i).slot_id());
    }
  }

  // Add the middle TreeNodeRecord to parent tree node.
  if (tn_page->Meta()->parent_page() < 0) {
    // Current tree node is root, so create new root and we need to add both
    // tree nodes to the root.
    RecordPage* new_root = AllocateNewPage(TREE_NODE);
    header_->set_root_page(new_root->id());

    Schema::TreeNodeRecord left_upper_tn_record =
        *reinterpret_cast<Schema::TreeNodeRecord*>(prmanager.Record(0));
    left_upper_tn_record.set_page_id(tn_page->id());
    if (!InsertTreeNodeRecord(&left_upper_tn_record, new_root)) {
      LogERROR("Add left half child to new root node failed");
      return false;
    }
  }

  // Add new tree node to parent by inserting a TreeNodeRecord with key of
  // the mid_index record, and page id of the new tree node.
  Schema::TreeNodeRecord right_upper_tn_record =
      *reinterpret_cast<Schema::TreeNodeRecord*>(prmanager.Record(mid_index));
  right_upper_tn_record.set_page_id(new_tree_node->id());
  RecordPage* parent_node = FetchPage(tn_page->Meta()->parent_page());
  if (!InsertTreeNodeRecord(&right_upper_tn_record, parent_node)) {
    LogERROR("Add new tree node to parent node failed");
    return false;
  }

  return true;
}

bool BplusTree::AddLeaveToTree(RecordPage* leave) {
  // Load first record from the page.
  Schema::DataRecord first_data_record;
  first_data_record.InitRecordFields(schema_.get(), key_indexes_,
                                     DataBaseFiles::INDEX_DATA,
                                     DataBaseFiles::TREE_LEAVE);
  if (leave->Meta()->num_records() <= 0) {
    LogFATAL("The leave page %d is empty, won't add it to tree", leave->id());
    return false;
  }
  int load_size = first_data_record.LoadFromMem(leave->Record(0));
  if (load_size != leave->Meta()->slot_directory()[0].length()) {
    LogFATAL("Load first record from leave %d - expect %d bytes, actual %d",
             leave->id(),leave->Meta()->slot_directory()[0].length(),load_size);
  }
  // first_data_record.Print();

  // Create a new TreeNodeRecord to insert to node.
  Schema::TreeNodeRecord new_tn_record;
  first_data_record.ExtractKey(&new_tn_record, key_indexes_);
  new_tn_record.set_page_id(leave->id());

  // Get the tree node to insert.
  RecordPage* tree_node = nullptr;
  if (prev_leave) {
    tree_node = FetchPage(prev_leave->Meta()->parent_page());
    if (!tree_node) {
      LogERROR("Can't find parent of prev leave %d", prev_leave_id);
      return false;
    }
  }
  else {
    // First leave and also first tree node. We need to allocate a tree node
    // which is also root node. The TreeNodeRecord to insert must be reset (all
    // fields clear) as minimum value.
    tree_node = AllocateNewPage(TREE_NODE);
    header_->set_root_page(tree_node->id());
    new_tn_record.reset();
    new_tn_record.set_page_id(leave->id());
  }
  //new_tn_record.Print();
  if (!InsertTreeNodeRecord(&new_tn_record, tree_node)) {
    LogERROR("Insert leave to tree node failed");
    return false;
  }
  return true;
}


// BulkLoading data
bool BplusTree::BulkLoadRecord(Schema::DataRecord* record) {
  // Try inserting the record to current leave page. If success, we continue.
  // Otherwise it is possible that current leave is nullptr (empty B+ tree,
  // no record ever inserted), or current leave is full. In either case, we
  // need to allocate a new leave page and continue inserting.
  if (crt_leave && InsertRecordToLeave(record, crt_leave)) {
    return true;
  }

  // Allocate a new leave node.
  RecordPage* new_leave = AllocateNewPage(TREE_LEAVE);
  if (crt_leave) {
    new_leave->Meta()->set_prev_page(crt_leave->id());
    crt_leave->Meta()->set_next_page(new_leave->id());
    prev_leave = crt_leave;
  }
  crt_leave = new_leave;
  // Now we have a new leave node, we insert the record.
  if (!InsertRecordToLeave(record, crt_leave)) {
    LogFATAL("Failed to insert record to new leave node");
    return false;
  }
  // We add this new leave node with one record, to a upper tree node. Note that
  // we only do this when having allocated a new leave node and had just first
  // record inserted to it.
  if (!AddLeaveToTree(crt_leave)) {
    LogFATAL("Failed to add leave to B+ tree node");
    return false;
  }

  return true;
}

}  // namespace DataBaseFiles

