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
    LogERROR("Parse B+ tree header page failed");
    return false;
  }
  return true;
}

bool BplusTreeHeaderPage::ConsistencyCheck(const char* op) const {
  if (num_pages_ != num_free_pages_ + num_used_pages_) {
    LogERROR("%s: num_pages !=  num_used_pages_ + num_free_pages_, "
             "(%d != %d + %d)", op,
             num_pages_, num_used_pages_, num_free_pages_);
    return false;
  }

  if ((num_free_pages_ > 0 && free_page_ < 0) ||
      (num_free_pages_ <= 0 && free_page_ >= 0)) {
    LogERROR("%s: num_free_pages_ = %d, first_free_page_id = %d", op,
             num_free_pages_, free_page_);
    return false;
  }

  if (root_page_ < 0 && (num_leaves_ > 0 || depth_ > 0)) {
    LogERROR("%s: Empty tree, but num_leaves_ = %d, depth_ = %d", op,
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
    //printf("Checking out %d pags\n", page_map_.size());
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

  file_ = fopen(btree_filename.c_str(), "r+");
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
  RecordPage* page = new RecordPage(bl_status_.next_id++, file_);
  page->InitInMemoryPage();
  page->Meta()->set_page_type(page_type);
  page_map_[page->id()] = std::shared_ptr<RecordPage>(page);
  header_->increment_num_pages(1);
  header_->increment_num_used_pages(1);
  if (page_type == TREE_LEAVE) {
    header_->increment_num_leaves(1);
  }
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
      delete new_page;
      return nullptr;
    }
    page_map_.emplace(page_id, std::shared_ptr<RecordPage>(new_page));
    return new_page;
  }
}

bool BplusTree::CheckoutPage(int page_id, bool write_to_disk) {
  if (page_map_.find(page_id) == page_map_.end()) {
    LogINFO("page %d not in page map, won't checkout");
    return false;
  }
  if (write_to_disk && !page_map_.at(page_id)->DumpPageData()) {
    LogERROR("Save page %d to disk failed", page_id);
    return false;
  }
  page_map_.erase(page_id);
  return true;
}

bool BplusTree::InsertRecordToLeave(const Schema::DataRecord* record) {
  return false;  
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
  
  tn_record->Print();
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

  int mid_index = prmanager.AppendRecordAndSplitPage(tn_record);
  if (mid_index < 0) {
    LogERROR("Failed to add new TreeNodeRecord to page");
    return false;
  }

  // Allocate a new TreeNoe page and move the second half of current TreeNode
  // records to the new one.
  RecordPage* new_tree_node = AllocateNewPage(TREE_NODE);
  const auto& plrecords = prmanager.plrecords();
  // Rigth-half records go to new (split out) tree node.
  for (int i = mid_index; i < (int)plrecords.size(); i++) {
    if (!prmanager.Record(i)->InsertToRecordPage(new_tree_node)) {
      LogERROR("Move slot %d TreeNodeRecord to new tree node failed");
      return false;
    }
    // Reset new parent for right half children nodes.
    auto tn_record = prmanager.GetRecord<Schema::TreeNodeRecord>(i);
    int child_page_id = tn_record->page_id();
    RecordPage* child_page = FetchPage(child_page_id);
    if (child_page) {
      child_page->Meta()->set_parent_page(new_tree_node->id());
    }
    // Remove the child TreeNodeRecord from the original tree node (left one).
    if (plrecords.at(i).slot_id() >= 0) {
      tn_page->DeleteRecord(plrecords.at(i).slot_id());
    }
  }

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
    else {
      // After split, left half child nodes will not be modified later, and we
      // can check them out to disk.
      int child_page_id =
          reinterpret_cast<Schema::TreeNodeRecord*>(prmanager.Record(i))
              ->page_id();
      CheckoutPage(child_page_id);
    }
  }

  // Add the middle TreeNodeRecord to parent tree node.
  if (tn_page->Meta()->parent_page() < 0) {
    // Current tree node is root, so create new root and we need to add both
    // tree nodes to the root.
    RecordPage* new_root = AllocateNewPage(TREE_NODE);
    header_->set_root_page(new_root->id());
    header_->increment_depth(1);
    printf("creating new root %d\n", new_root->id());

    auto left_upper_tn_record = prmanager.GetRecord<Schema::TreeNodeRecord>(0);
    left_upper_tn_record->set_page_id(tn_page->id());
    if (!InsertTreeNodeRecord(left_upper_tn_record, new_root)) {
      LogERROR("Add left half child to new root node failed");
      return false;
    }
  }

  // Add new tree node to parent by inserting a TreeNodeRecord with key of
  // the mid_index record, and page id of the new tree node.
  auto right_upper_tn_record =
      prmanager.GetRecord<Schema::TreeNodeRecord>(mid_index);
  right_upper_tn_record->set_page_id(new_tree_node->id());
  RecordPage* parent_node = FetchPage(tn_page->Meta()->parent_page());
  if (!InsertTreeNodeRecord(right_upper_tn_record, parent_node)) {
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
  //first_data_record.Print();

  // Create a new TreeNodeRecord to insert to node.
  Schema::TreeNodeRecord new_tn_record;
  first_data_record.ExtractKey(&new_tn_record, key_indexes_);
  new_tn_record.set_page_id(leave->id());

  // Get the tree node to insert.
  RecordPage* tree_node = nullptr;
  if (bl_status_.prev_leave) {
    tree_node = FetchPage(bl_status_.prev_leave->Meta()->parent_page());
    if (!tree_node) {
      LogERROR("Can't find parent of prev leave %d",
               bl_status_.prev_leave->id());
      return false;
    }
  }
  else {
    // First leave and also first tree node. We need to allocate a tree node
    // which is root node. The TreeNodeRecord to insert must be reset (all
    // fields clear) as minimum value.
    tree_node = AllocateNewPage(TREE_NODE);
    header_->set_root_page(tree_node->id());
    header_->set_depth(1);
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

RecordPage* BplusTree::AppendNewLeave() {
  RecordPage* new_leave = AllocateNewPage(TREE_LEAVE);
  if (bl_status_.crt_leave) {
    new_leave->Meta()->set_prev_page(bl_status_.crt_leave->id());
    bl_status_.crt_leave->Meta()->set_next_page(new_leave->id());
    bl_status_.prev_leave = bl_status_.crt_leave;
  }
  bl_status_.crt_leave = new_leave;
  return new_leave;
}

RecordPage* BplusTree::AppendNewOverflowLeave() {
  RecordPage* new_leave = AppendNewLeave();
  new_leave->Meta()->
      set_parent_page(bl_status_.prev_leave->Meta()->parent_page());
  new_leave->Meta()->set_is_overflow_page(1);
  bl_status_.prev_leave->Meta()->set_overflow_page(new_leave->id());
  return new_leave;
}

// BulkLoading data
bool BplusTree::BulkLoadRecord(Schema::DataRecord* record) {
  if (!record) {
    LogERROR("Record to insert is nullptr");
    return false;
  }

  // Try inserting the record to current leave page. If success, we continue.
  // Otherwise it is possible that current leave is nullptr (empty B+ tree,
  // no record ever inserted), or current leave is full. In either case, we
  // need to allocate a new leave page and continue inserting.
  if (bl_status_.crt_leave &&
      bl_status_.crt_leave->Meta()->is_overflow_page() == 0 &&
      record->InsertToRecordPage(bl_status_.crt_leave)) {
    bl_status_.last_record.reset(record->Duplicate());
    return true;
  }

  // Check boundary duplication. If new record equals last record at the end of
  // its leave, we need to move these duplicates to new leave.
  if (bl_status_.crt_leave && bl_status_.last_record &&
      Schema::RecordBase::CompareRecordsBasedOnKey(
          record, bl_status_.last_record.get(),
          key_indexes_) == 0) {
    bl_status_.last_record.reset(record->Duplicate());
    return CheckBoundaryDuplication(record);
  }

  // Normal insertion. Allocate a new leave node and insert the record.
  RecordPage* new_leave = AppendNewLeave();
  if (!record->InsertToRecordPage(new_leave)) {
    LogFATAL("Failed to insert record to new leave node");
  }
  // We add this new leave node with one record, to a upper tree node. Note that
  // we only do this when having allocated a new leave node and had just first
  // record inserted to it.
  if (!AddLeaveToTree(new_leave)) {
    LogFATAL("Failed to add leave to B+ tree node");
  }

  // Keep a copy of last inserted record. We need this copy to check record
  // duplication in 
  bl_status_.last_record.reset(record->Duplicate());

  return true;
}

bool BplusTree::CheckBoundaryDuplication(Schema::RecordBase* record) {
  // Try inserting first. This deals with a non-full overflow page.
  if (record->InsertToRecordPage(bl_status_.crt_leave)) {
    return true;
  }

  // Load curret leave records and find the first duplicate with the new record
  // to be insert.
  Schema::PageRecordsManager prmanager(bl_status_.crt_leave,
                                       schema_.get(), key_indexes_,
                                       file_type_, TREE_LEAVE);

  int index = prmanager.NumRecords() - 1;
  for (; index >= 0; index--) {
    if (Schema::RecordBase::CompareRecordsBasedOnKey(
            record, bl_status_.last_record.get(),
            key_indexes_) != 0) {
      break;
    }
  }
  index++;
  // If index == 0, this leave is filled with all same records. Then we don't
  // allocate new leave, but append overflow pages immediately.
  if (index > 0) {
    // From plrecord[index], records of current leave are duplicates.
    RecordPage* new_leave = AppendNewLeave();
    const auto& plrecords = prmanager.plrecords();
    for (int i = index; i < (int)plrecords.size(); i++) {
      if (!prmanager.Record(i)->InsertToRecordPage(new_leave)) {
        LogFATAL("Faield to move slot %d TreeNodeRecord to new tree node");
      }
      // Remove the records from the original leave.
      bl_status_.prev_leave->DeleteRecord(plrecords.at(i).slot_id());
    }

    if (!AddLeaveToTree(bl_status_.crt_leave)) {
      LogFATAL("Failed to add leave to B+ tree");
      return false;
    }
    // Re-try inserting record to new crt_leave.
    if (record->InsertToRecordPage(new_leave)) {
      return true;
    }
  }

  LogINFO("New duplicated record needs a new overflow page.");
  RecordPage* overflow_leave = AppendNewOverflowLeave();
  if (!record->InsertToRecordPage(overflow_leave)) {
    LogFATAL("Insert first duplicated record to overflow page faield - "
             "This should not happen!");
    return false;
  }

  return true;
}

bool BplusTree::ValidityCheck() {
  if (header_->root_page() < 0) {
    LogINFO("No root node. Empty B+ tree");
    return true;
  }

  // Level-traverse the B+ tree.
  std::queue<RecordPage*> page_q;
  RecordPage* root = FetchPage(header_->root_page());
  page_q.push(root);
  vc_status_.count_num_pages = 1;
  vc_status_.count_num_used_pages = 1;
  while (!page_q.empty()) {
    RecordPage* crt_page = page_q.front();
    page_q.pop();

    // Verify this page.
    if (crt_page->Meta()->page_type() == TREE_NODE) {
      if (!CheckTreeNodeValid(crt_page)) {
        LogERROR("CheckTreeNodeValid failed for page %d", crt_page->id());
        return false;
      }
      // Enqueue all child nodes of current node.
      if (!EqueueChildNodes(crt_page, &page_q)) {
        LogERROR("Enqueue children nodes failed for node %d", crt_page->id());
        return false;
      }
    }
    CheckoutPage(crt_page->id(), false);
  }

  if (!MetaConsistencyCheck()) {
    return false;
  }

  return true;
}

bool BplusTree::CheckTreeNodeValid(RecordPage* page) {
  if (page->Meta()->page_type() != TREE_NODE) {
    LogERROR("Wrong node type - not tree node");
    return false;
  }

  Schema::PageRecordsManager prmanager(page, schema_.get(), key_indexes_,
                                       file_type_, TREE_NODE);
  
  for (int i = 0; i < prmanager.NumRecords(); i++) {
    auto tn_record = prmanager.GetRecord<Schema::TreeNodeRecord>(i);
    int child_page_id = tn_record->page_id();

    Schema::TreeNodeRecord* next_record = nullptr;
    if (i < prmanager.NumRecords() - 1) {
      next_record = prmanager.GetRecord<Schema::TreeNodeRecord>(i + 1);
    }
    if (!VerifyChildRecordsRange(FetchPage(child_page_id),
                                 tn_record, next_record)) {
      LogERROR("Verify child page %d range failed", child_page_id);
      return false;
    }
  }

  return true;
}

bool BplusTree::VerifyChildRecordsRange(RecordPage* child_page,
                                        Schema::RecordBase* left_bound,
                                        Schema::RecordBase* right_bound) {
  if (!child_page) {
    LogFATAL("Child page is nullptr, can't verify it's range");
  }

  Schema::PageRecordsManager prmanager(child_page, schema_.get(), key_indexes_,
                                       file_type_,
                                       child_page->Meta()->page_type());

  if (prmanager.NumRecords() <= 0) {
    LogINFO("No records in child page, skip VerifyChildRecordsRange");
    return true;
  }

  // Check the first record >= left bound.
  Schema::RecordBase first_record_key;
  if (file_type_== INDEX_DATA &&
      child_page->Meta()->page_type() == TREE_LEAVE) {
    // If child is leave of index-data file, record is DataRecord. We need to
    // extract key from it.
    auto first_data_record = prmanager.GetRecord<Schema::DataRecord>(0);
    first_data_record->ExtractKey(&first_record_key, key_indexes_);
  }
  else {
    first_record_key = *prmanager.GetRecord<Schema::RecordBase>(0);
  }
  if (first_record_key < *left_bound) {
    LogERROR("First record key of child < left bound");
    return false;
  }

  // Check the last record <= right bound.
  if (!right_bound) {
    // No right bound, it's last child.
    return true;
  }
  Schema::RecordBase last_record_key;
  int last_index = prmanager.NumRecords() - 1;
  if (file_type_== INDEX_DATA &&
      child_page->Meta()->page_type() == TREE_LEAVE) {
    // If child is leave of index-data file, record is DataRecord. We need to
    // extract key from it.
    auto last_data_record = prmanager.GetRecord<Schema::DataRecord>(last_index);
    last_data_record->ExtractKey(&last_record_key, key_indexes_);
  }
  else {
    last_record_key = *prmanager.GetRecord<Schema::RecordBase>(last_index);
  }
  if (last_record_key >= *right_bound) {  // right boundary is open interval
    LogERROR("last record key of child > right bound, "
             "happens on page type %d", child_page->Meta()->page_type());
    last_record_key.Print();
    right_bound->Print();
    return false;
  }

  return true;
}

bool BplusTree::EqueueChildNodes(RecordPage* page,
                                 std::queue<RecordPage*>* page_q) {
  if (page->Meta()->page_type() != TREE_NODE) {
    LogERROR("Wrong node type - not tree node, no child node");
    return false;
  }

  Schema::PageRecordsManager prmanager(page, schema_.get(), key_indexes_,
                                       file_type_, TREE_NODE);
  
  bool children_are_leave = false;
  vc_status_.count_num_pages += prmanager.NumRecords();
  vc_status_.count_num_used_pages += prmanager.NumRecords();
  for (int i = 0; i < prmanager.NumRecords(); i++) {
    auto tn_record = prmanager.GetRecord<Schema::TreeNodeRecord>(i);
    RecordPage* child_page = FetchPage(tn_record->page_id());
    if (!child_page) {
      LogFATAL("Can't fetching child page while enqueuing");
    }
    if (child_page->Meta()->page_type() == TREE_LEAVE) {
      while (child_page) {
        // Don't enqueue tree leave.
        children_are_leave = true;
        vc_status_.count_num_leaves++;
        vc_status_.count_num_records += child_page->Meta()->num_records();
        // Check prev_leave <-- crt_leave.prev
        if (vc_status_.prev_leave_id != child_page->Meta()->prev_page()) {
          LogERROR("Leave connnecting verification failed - "
                   "curent leave's prev = %d, while prev_leave_id = %d",
                   child_page->Meta()->prev_page(), vc_status_.prev_leave_id);
          return false;
        }
        // Check prev_leave.next --> crt_leave
        vc_status_.prev_leave_id = child_page->id();
        if (vc_status_.prev_leave_next > 0 &&
            vc_status_.prev_leave_next != child_page->id()) {
          LogERROR("Leave connnecting verification failed - "
                   "prev leave's next = %d, while crt_leave_id = %d",
                   vc_status_.prev_leave_next, child_page->id());
          return false;
        }
        vc_status_.prev_leave_next = child_page->Meta()->next_page();
        // Find overflow page.
        if (child_page->Meta()->overflow_page() >= 0) {
          child_page = FetchPage(child_page->Meta()->overflow_page());
          if (!child_page) {
            LogFATAL("Can't fetching overflow child page while enqueuing");
          }
          vc_status_.count_num_pages++;
          vc_status_.count_num_used_pages++;
          if (!VerifyOverflowPage(child_page)) {
            LogFATAL("Verify overflow page failed");
          }
        }
        else {
          child_page = nullptr;
        }
      }
      CheckoutPage(tn_record->page_id(), false);
    }
    else {
      if (children_are_leave) {
        LogERROR("Children nodes have inconsistency types");
        return false;
      }
      if (child_page) {
        page_q->push(child_page);
      }
    }
  }
  return true;
}

bool BplusTree::VerifyOverflowPage(RecordPage* page) {
  Schema::PageRecordsManager prmanager(page, schema_.get(), key_indexes_,
                                       file_type_, TREE_LEAVE);
  
  for (int i = 1; i < prmanager.NumRecords(); i++) {
    if (Schema::RecordBase::CompareRecordsBasedOnKey(
            prmanager.Record(0), prmanager.Record(i),
            key_indexes_) != 0) {
      LogERROR("Records in overflow page inconsistent!");
      return false;
    }
  }

  return true;
}

bool BplusTree::MetaConsistencyCheck() const {
  if (vc_status_.count_num_pages != header_->num_pages()) {
    LogERROR("num_pages inconsistent - expect %d, actual %d",
             header_->num_pages(), vc_status_.count_num_pages);
    return false;
  }

  if (vc_status_.count_num_used_pages != header_->num_used_pages()) {
    LogERROR("num_used_pages inconsistent - expect %d, actual %d",
             header_->num_used_pages(), vc_status_.count_num_used_pages);
    return false;
  }

  if (vc_status_.count_num_free_pages != header_->num_free_pages()) {
    LogERROR("num_free_pages inconsistent - expect %d, actual %d",
             header_->num_free_pages(), vc_status_.count_num_free_pages);
    return false;
  }

  if (vc_status_.count_num_leaves != header_->num_leaves()) {
    LogERROR("num_leaves inconsistent - expect %d, actual %d",
             header_->num_leaves(), vc_status_.count_num_leaves);
    return false;
  }

  printf("count_num_records = %d\n", vc_status_.count_num_records);

  return true;
}

int BplusTree::SearchByKey(
         const Schema::RecordBase* key,
         std::vector<std::shared_ptr<Schema::RecordBase>>* result) {
  if (!key || !result) {
    LogERROR("Nullptr input to SearchByKey");
    return -1;
  }
  result->clear();

  RecordPage* crt_page = FetchPage(header_->root_page());
  while (crt_page && crt_page->Meta()->page_type() == TREE_NODE) {
    crt_page = SearchToNextLevel(crt_page, key);
  }

  if (!crt_page) {
    LogERROR("Failed to search for key");
    key->Print();
    return -1;
  }

  FetchResultsFromLeave(crt_page, key, result);
  return result->size();
}

int BplusTree::FetchResultsFromLeave(
         RecordPage* leave,
         const Schema::RecordBase* key,
         std::vector<std::shared_ptr<Schema::RecordBase>>* result) {
  if (!leave || !key || !result) {
    LogERROR("Nullptr input to FetchResultsFromLeave");
    return -1;
  }

  while (leave) {
    Schema::PageRecordsManager prmanager(leave, schema_.get(), key_indexes_,
                                         file_type_,
                                         leave->Meta()->page_type());
    // Fetch all matching records in this leave.
    int index = 0;
    int num_matching_records = 0;
    bool last_is_match = false;
    for (; index < prmanager.NumRecords(); index++) {
      if (Schema::RecordBase::CompareRecordWithKey(
            key, prmanager.Record(index),
            key_indexes_) == 0) {
        result->push_back(prmanager.plrecords().at(index).Record());
        num_matching_records++;
        last_is_match = true;
      }
      else {
        if (leave->Meta()->is_overflow_page()) {
          // Overflow page must have all same records that match the key we're
          // searching for.
          LogFATAL("Overflow page stores inconsistent records!");
        }
        last_is_match = false;
      }
    } 
    // If index reaches the end of all records, check overflow page.
    if (last_is_match && leave->Meta()->overflow_page() >= 0) {
      if (num_matching_records == 0) {
        LogERROR("No records matched found on leave %d", leave->id());
      }
      leave = FetchPage(leave->Meta()->overflow_page());
    }
    else {
      break;
    }
  }

  return result->size();
}

RecordPage* BplusTree::SearchToNextLevel(RecordPage* page,
                                         const Schema::RecordBase* key) {
  Schema::PageRecordsManager prmanager(page, schema_.get(), key_indexes_,
                                       file_type_, page->Meta()->page_type());

  int index = prmanager.SearchForKey(key);
  if (index < 0) {
    LogERROR("Search for key in page %d failed - key is:", page->id());
    return nullptr;
  }

  auto tn_record = prmanager.GetRecord<Schema::TreeNodeRecord>(index);
  RecordPage* next_level_page = FetchPage(tn_record->page_id());
  return next_level_page;
}

bool BplusTree::CheckRecordFieldsType(const Schema::RecordBase* record) const {
  if (record->type() == Schema::DATA_RECORD) {
    if (!record->CheckFieldsType(schema_.get())) {
      return false;
    }
    return true;
  }
  if (record->type() == Schema::INDEX_RECORD) {
    if (!record->CheckFieldsType(schema_.get(), key_indexes_)) {
      return false;
    }
    return true;
  }

  LogERROR("Invalid ReocrdType %d to insert to B+ tree", record->type());
  return false;
}

bool BplusTree::InsertRecord(const Schema::DataRecord* record) {
  if (!record) {
    LogERROR("record to insert is nullptr");
    return false;
  }

  // Verify this data record fields matches schema.
  if (!CheckRecordFieldsType(record)) {
    LogERROR("Record fields type mismatch table schema");
    return false;
  }
  return true;
}

bool BplusTree::InsertRecord(const Schema::IndexRecord* record) {
  return false;
}

bool BplusTree::InsertRecord(const Schema::RecordBase* record) {
  return false;
}

}  // namespace DataBaseFiles

