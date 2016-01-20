#include <sys/stat.h>
#include <string.h>

#include "Base/Log.h"
#include "Base/Utils.h"
#include "Schema/PageRecordsManager.h"
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
  // next_id
  memcpy(buf + offset, &next_id_, sizeof(next_id_));
  offset += sizeof(next_id_);

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
  // depth
  memcpy(&next_id_, buf + offset, sizeof(next_id_));
  offset += sizeof(next_id_);

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
    LogERROR("DumpToMem failed before saving B+ tree to disk.");
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
  int new_page_id = 0;
  RecordPage* page = nullptr;
  // Look up free page list first.
  if (header_->num_free_pages() > 0) {
    new_page_id = header_->free_page();
    header_->decrement_num_free_pages(1);
    RecordPage* page = FetchPage(header_->free_page());
    if (!page) {
      LogFATAL("Fetch free page %d failed", header_->free_page());
    }
    header_->set_free_page(page->Meta()->next_page());
    page->Meta()->reset();
  }
  else {
    // Append new page at the end of file.
    new_page_id = header_->next_id();
    header_->increment_next_id(1);
    header_->increment_num_pages(1);
    page = new RecordPage(new_page_id, file_);
    page->InitInMemoryPage();
  }

  page->Meta()->set_page_type(page_type);
  page_map_[page->id()] = std::shared_ptr<RecordPage>(page);
  header_->increment_num_used_pages(1);
  if (page_type == TREE_LEAVE) {
    header_->increment_num_leaves(1);
    //printf("Allocated new leave %d\n", page->id());
  }
  return page;
}

RecordPage* BplusTree::FetchPage(int page_id) {
  if (page_id < 0) {
    return nullptr;
  }
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

bool BplusTree::InsertTreeNodeRecord(Schema::TreeNodeRecord* tn_record,
                                     RecordPage* tn_page) {
  //printf("Inserting to tree node to node %d\n", tn_page->id());
  if (!tn_record || !tn_page) {
    LogERROR("nullptr passed to InsertTreeNodeRecord");
    return false;
  }

  if (tn_page->Meta()->page_type() != TREE_NODE) {
    LogERROR("Target RecordPage is not tree node");
    return false;
  }
  
  // printf("Inserting new TreeNodeRecord:\n");
  // tn_record->Print();
  if (tn_record->InsertToRecordPage(tn_page) >= 0) {
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
    if (prmanager.Record(i)->InsertToRecordPage(new_tree_node) < 0) {
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
      if (tn_record->InsertToRecordPage(tn_page) < 0) {
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
      // int child_page_id =
      //     reinterpret_cast<Schema::TreeNodeRecord*>(prmanager.Record(i))
      //         ->page_id();
      // CheckoutPage(child_page_id);
    }
  }

  // Add the middle TreeNodeRecord to parent tree node.
  if (tn_page->Meta()->parent_page() < 0) {
    // Current tree node is root, so create new root and we need to add both
    // tree nodes to the root.
    RecordPage* new_root = AllocateNewPage(TREE_NODE);
    LogINFO("Creating new root %d", new_root->id());
    header_->set_root_page(new_root->id());
    header_->increment_depth(1);

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

bool BplusTree::AddFirstLeaveToTree(RecordPage* leave) {
  Schema::TreeNodeRecord min_tn_record;
  min_tn_record.InitRecordFields(schema_.get(), key_indexes_,
                                 file_type_, DataBaseFiles::TREE_NODE);
  RecordPage* tree_node = AllocateNewPage(TREE_NODE);
  header_->set_root_page(tree_node->id());
  header_->set_depth(1);
  min_tn_record.reset();
  min_tn_record.set_page_id(leave->id());
  if (!InsertTreeNodeRecord(&min_tn_record, tree_node)) {
    LogERROR("Insert leave to tree node failed");
    return false;
  }
  return false;
}

// Only used in bulk loading.
bool BplusTree::AddLeaveToTree(RecordPage* leave,
                               Schema::TreeNodeRecord* tn_record) {
  if (!leave || !tn_record) {
    LogERROR("nullptr input to AddLeaveToTree");
    return false;
  }

  // Create a new TreeNodeRecord to insert to node.
  tn_record->set_page_id(leave->id());

  // Get the tree node to insert.
  Schema::TreeNodeRecord min_tn_record;
  RecordPage* tree_node = nullptr;
  if (bl_status_.prev_leave) {
    tree_node = FetchPage(bl_status_.prev_leave->Meta()->parent_page());
    if (!tree_node) {
      LogFATAL("Can't find parent of prev leave %d",
               bl_status_.prev_leave->id());
    }
    //tn_record->Print();
    if (!InsertTreeNodeRecord(tn_record, tree_node)) {
      LogERROR("Insert leave to tree node failed");
      return false;
    }
  }
  else {
    // First leave and also first tree node. We need to allocate a tree node
    // which is root node. The TreeNodeRecord to insert must be reset (all
    // fields clear) as minimum value.
    AddFirstLeaveToTree(leave);
  }
  
  return true;
}

RecordPage* BplusTree::AppendNewLeave() {
  RecordPage* new_leave = AllocateNewPage(TREE_LEAVE);
  if (bl_status_.crt_leave) {
    ConnectLeaves(bl_status_.crt_leave, new_leave);
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

RecordPage* BplusTree::AppendOverflowPageTo(RecordPage* page) {
  if (!page) {
    LogERROR("Can't append overflow_page to page nullptr");
    return nullptr;
  }
  RecordPage* new_leave = AllocateNewPage(TREE_LEAVE);
  new_leave->Meta()->
      set_parent_page(page->Meta()->parent_page());
  new_leave->Meta()->set_is_overflow_page(1);
  page->Meta()->set_overflow_page(new_leave->id());
  ConnectLeaves(page, new_leave);
  return new_leave;
}

bool BplusTree::ConnectLeaves(RecordPage* page1, RecordPage* page2) {
  if (!page1 || !page2) {
    LogERROR("Can't connect nullptr leaves %p and %p", page1, page2);
    return false;
  }
  //printf("Connecting %d and %d\n", page1->id(), page2->id());
  page1->Meta()->set_next_page(page2->id());
  page2->Meta()->set_prev_page(page1->id());
  return true;
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
      record->InsertToRecordPage(bl_status_.crt_leave) >= 0) {
    bl_status_.last_record.reset(record->Duplicate());
    return true;
  }

  // Check boundary duplication. If new record equals last record at the end of
  // its leave, we need to move these duplicates to new leave.
  if (bl_status_.crt_leave && bl_status_.last_record &&
      Schema::RecordBase::CompareRecordsBasedOnIndex(
          record, bl_status_.last_record.get(),
          key_indexes_) == 0) {
    bl_status_.last_record.reset(record->Duplicate());
    return CheckBoundaryDuplication(record);
  }

  // Normal insertion. Allocate a new leave node and insert the record.
  RecordPage* new_leave = AppendNewLeave();
  if (record->InsertToRecordPage(new_leave) < 0) {
    LogFATAL("Failed to insert record to new leave node");
  }
  // We add this new leave node with one record, to a upper tree node. Note that
  // we only do this when having allocated a new leave node and had just first
  // record inserted to it.
  Schema::TreeNodeRecord tn_record;
  ProduceKeyRecordFromLeaveRecord(record, &tn_record);
  if (!AddLeaveToTree(new_leave, &tn_record)) {
    LogFATAL("Failed to add leave to B+ tree node");
  }

  // Keep a copy of last inserted record. We need this copy to check record
  // duplication in CheckBoundaryDuplication.
  bl_status_.last_record.reset(record->Duplicate());

  return true;
}

bool BplusTree::CheckBoundaryDuplication(Schema::RecordBase* record) {
  // Try inserting first. This deals with a non-full overflow page.
  if (record->InsertToRecordPage(bl_status_.crt_leave) >= 0) {
    return true;
  }

  // Load curret leave records and find the first duplicate with the new record
  // to be insert.
  Schema::PageRecordsManager prmanager(bl_status_.crt_leave,
                                       schema_.get(), key_indexes_,
                                       file_type_, TREE_LEAVE);

  int index = prmanager.NumRecords() - 1;
  for (; index >= 0; index--) {
    if (Schema::RecordBase::CompareRecordsBasedOnIndex(
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
      if (prmanager.Record(i)->InsertToRecordPage(new_leave) < 0) {
        LogFATAL("Faield to move slot %d TreeNodeRecord to new tree node");
      }
      // Remove the records from the original leave.
      bl_status_.prev_leave->DeleteRecord(plrecords.at(i).slot_id());
    }

    // Add the new leave to parent node.
    Schema::RecordBase* mid_record = prmanager.Record(index);
    Schema::TreeNodeRecord tn_record;
    ProduceKeyRecordFromLeaveRecord(mid_record, &tn_record);
    if (!AddLeaveToTree(new_leave, &tn_record)) {
      LogFATAL("Failed to add leave to B+ tree");
      return false;
    }
    // Re-try inserting record to new crt_leave.
    if (record->InsertToRecordPage(new_leave) >= 0) {
      return true;
    }
  }

  //LogINFO("New duplicated record needs a new overflow page.");
  RecordPage* overflow_leave = AppendNewOverflowLeave();
  if (record->InsertToRecordPage(overflow_leave) < 0) {
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
    first_record_key.Print();
    left_bound->Print();
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
        //printf("child_page = %d\n", child_page->id());
        // Don't enqueue tree leave.
        children_are_leave = true;
        vc_status_.count_num_leaves++;
        vc_status_.count_num_records += child_page->Meta()->num_records();
        // Check prev_leave <-- crt_leave.prev
        if (vc_status_.prev_leave_id != child_page->Meta()->prev_page()) {
          LogERROR("Leave connecting verification failed - "
                   "curent leave's prev = %d, while prev_leave_id = %d",
                   child_page->Meta()->prev_page(), vc_status_.prev_leave_id);
          printf("%d <-- current\n", child_page->Meta()->prev_page());
          return false;
        }
        // Check prev_leave.next --> crt_leave
        vc_status_.prev_leave_id = child_page->id();
        if (vc_status_.prev_leave_next > 0 &&
            vc_status_.prev_leave_next != child_page->id()) {
          LogERROR("Leave connecting verification failed - "
                   "prev leave's next = %d, while crt_child_id = %d",
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

  auto record = prmanager.Record(0);
  for (int i = 1; i < prmanager.NumRecords(); i++) {
    if (prmanager.CompareRecords(record, prmanager.Record(i)) != 0) {
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

  printf("count_num_pages = %d\n", vc_status_.count_num_pages);
  printf("count_num_leaves = %d\n", vc_status_.count_num_leaves);
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

  if (!CheckKeyFieldsType(key)) {
    LogERROR("Key fields type mismatch table schema of this B+ tree");
    return -1;
  }

  RecordPage* crt_page = FetchPage(header_->root_page());
  while (crt_page && crt_page->Meta()->page_type() == TREE_NODE) {
    auto result = SearchInTreeNode(crt_page, key);
    crt_page = FetchPage(result.child_id);
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
    bool last_is_match = false;
    for (int index = 0; index < prmanager.NumRecords(); index++) {
      if (prmanager.CompareRecordWithKey(key, prmanager.Record(index)) == 0) {
        result->push_back(prmanager.plrecords().at(index).Record());
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
      leave = FetchPage(leave->Meta()->overflow_page());
    }
    else {
      break;
    }
  }

  return result->size();
}

BplusTree::SearchTreeNodeResult
BplusTree::SearchInTreeNode(RecordPage* page, const Schema::RecordBase* key) {
  SearchTreeNodeResult result;
  if (page->Meta()->page_type() != TREE_NODE) {
    LogERROR("Can't search to next level on a non-TreeNode page");
    return result;
  }

  Schema::PageRecordsManager prmanager(page, schema_.get(), key_indexes_,
                                       file_type_, page->Meta()->page_type());

  int index = prmanager.SearchForKey(key);
  if (index < 0) {
    LogFATAL("Search for key in page %d failed - key is:", page->id());
  }

  result.slot = prmanager.RecordSlotID(index);
  result.record = prmanager.plrecords().at(index).Record();

  auto tn_record = prmanager.GetRecord<Schema::TreeNodeRecord>(index);
  result.child_id = tn_record->page_id();
  CheckLogFATAL(result.child_id >= 0, "Get child_id < 0 in SearchInTreeNode");

  if (index < prmanager.NumRecords() - 1) {
    result.next_slot = prmanager.RecordSlotID(index + 1);
    result.next_record = prmanager.plrecords().at(index + 1).Record();
    tn_record = prmanager.GetRecord<Schema::TreeNodeRecord>(index + 1);
    result.next_child_id = tn_record->page_id();
    CheckLogFATAL(result.next_child_id >= 0,
                  "Get nullptr next_child_page in SearchInTreeNode");
    result.next_leave_id = result.next_child_id;
  }
  else {
    // Get next leave id. It's not always next_child because next leave may have
    // different parent.
    auto child_page = FetchPage(result.child_id);
    CheckLogFATAL(child_page, "child page is nullptr");
    if (child_page->Meta()->next_page() >= 0) {
      child_page = GotoOverflowChainEnd(child_page);
      result.next_leave_id = child_page->Meta()->next_page();
    }
  }

  return result;
}

bool BplusTree::CheckKeyFieldsType(const Schema::RecordBase* key) const {
  return key->CheckFieldsType(schema_.get(), key_indexes_);
}

bool BplusTree::CheckRecordFieldsType(const Schema::RecordBase* record) const {
  if (record->type() == Schema::DATA_RECORD) {
    if (file_type_ == INDEX) {
      return false;
    }
    if (!record->CheckFieldsType(schema_.get())) {
      return false;
    }
    return true;
  }
  if (record->type() == Schema::INDEX_RECORD) {
    if (file_type_ == INDEX_DATA) {
      return false;
    }
    if (!record->CheckFieldsType(schema_.get(), key_indexes_)) {
      return false;
    }
    return true;
  }

  LogERROR("Invalid ReocrdType %d to insert to B+ tree", record->type());
  return false;
}


bool BplusTree::ProduceKeyRecordFromLeaveRecord(
         const Schema::RecordBase* leave_record, Schema::RecordBase* tn_record) {
  if (!leave_record || !tn_record) {
    LogERROR("Nullptr input to ProduceKeyRecordFromLeaveRecord");
    return false;
  }
  if (file_type_ == INDEX_DATA &&
      leave_record->type() == Schema::DATA_RECORD) {
    (reinterpret_cast<const Schema::DataRecord*>(leave_record))
        ->ExtractKey(tn_record, key_indexes_);
  }
  else if (file_type_ == INDEX &&
           leave_record->type() == Schema::INDEX_RECORD){
    tn_record->CopyFieldsFrom(leave_record);
  }
  else {
    LogFATAL("Inconsistent B+ tree type and leave record type (%d, %d)",
             file_type_, leave_record->type());
  }
  return true;
}

bool BplusTree::InsertNewRecordToNextLeave(RecordPage* leave,
                                           SearchTreeNodeResult* search_result,
                                           const Schema::RecordBase* record) {
  // Check next leave is valid, and has same parent.
  if (search_result->next_child_id < 0 &&
      FetchPage(search_result->next_child_id)->Meta()->overflow_page() >= 0) {
    return false;
  }

  auto next_leave = FetchPage(search_result->next_child_id);
  CheckLogFATAL(next_leave, "Failed to fetch next leave");
  auto parent = FetchPage(leave->Meta()->parent_page());
  CheckLogFATAL(parent, "Failed to fetch parent node");

  if (record->InsertToRecordPage(next_leave) < 0) {
    return false;
  }

  // Delete next leave's tree node record from parent.
  CheckLogFATAL(parent->DeleteRecord(search_result->next_slot),
                "Failed to delete next leave's original tree node record");
  Schema::TreeNodeRecord tn_record;
  ProduceKeyRecordFromLeaveRecord(record, &tn_record);
  tn_record.set_page_id(next_leave->id());
  CheckLogFATAL(InsertTreeNodeRecord(&tn_record, parent),
                "Failed to insert new tree node record for next leave");

  return true;
}

bool BplusTree::ReDistributeRecordsFromNextLeave(
         RecordPage* leave,
         SearchTreeNodeResult* search_result) {
  auto next_leave = FetchPage(search_result->next_child_id);
  CheckLogFATAL(next_leave, "Failed to fetch next child leave");

  Schema::PageRecordsManager prmanager(next_leave, schema_.get(), key_indexes_,
                                       file_type_, TREE_LEAVE);
  std::vector<Schema::RecordGroup> rgroups;
  prmanager.GroupRecords(&rgroups);

  int min_gap = std::abs(leave->Meta()->space_used() -
                         next_leave->Meta()->space_used());
  int gindex = 0;
  for (; gindex < (int)rgroups.size(); gindex++) {
    if (std::abs(leave->Meta()->space_used() + rgroups[gindex].size -
                 next_leave->Meta()->space_used() - rgroups[gindex].size)
        > min_gap) {
      break;
    }
    if (!leave->PreCheckCanInsert(rgroups[gindex].num_records,
                                  rgroups[gindex].size)) {
      break;
    }
    // Move from next_leave to leave.
    for (int index = rgroups[gindex].start_index;
         index < rgroups[gindex].start_index + rgroups[gindex].num_records;
         index++) {
      int slot_id = prmanager.RecordSlotID(index);
      next_leave->DeleteRecord(slot_id);
      if (prmanager.Record(index)->InsertToRecordPage(leave) < 0) {
        LogFATAL("Failed to insert record to page2");
      }
    }
  }
  // No record is re-distributed.
  if (gindex == 0) {
    return true;
  }

  // Delete original tree node record of next leave from parent.
  auto parent = FetchPage(next_leave->Meta()->parent_page());
  CheckLogFATAL(parent, "Failed to fetch parent of next leave");
  CheckLogFATAL(parent->DeleteRecord(search_result->next_slot),
                "Failed to delete next leave's original tree node record");

  // Insert new tree node record of next leave to parent.
  Schema::TreeNodeRecord tn_record;
  auto new_min_record = prmanager.Record(rgroups[gindex].start_index);
  ProduceKeyRecordFromLeaveRecord(new_min_record, &tn_record);
  tn_record.set_page_id(next_leave->id());
  CheckLogFATAL(InsertTreeNodeRecord(&tn_record, parent),
                "Failed to insert new tree node record for next leave");

  return true;
}

bool BplusTree::InsertAfterOverflowLeave(RecordPage* leave,
                                         SearchTreeNodeResult* search_result,
                                         const Schema::RecordBase* record) {
  // Try inserting to this overflow page. If the new record happens to match
  // this overflow page we're done in this special case.
  if (InsertNewRecordToOverFlowChain(leave, record)) {
    return true;
  }
  // Special case 2 - It is the most left leave, which has different tree
  // node key with the records in this leave. This new record is possibly
  // 'less' than records of the overflow page, which means the new leave
  // should reside left to current leave.
  if (leave->Meta()->prev_page() < 0) {
    Schema::PageRecordsManager prmanager(leave, schema_.get(),
                                         key_indexes_,
                                         file_type_, TREE_LEAVE);
    if (prmanager.CompareRecords(record, prmanager.Record(0)) < 0) {
      auto parent = FetchPage(leave->Meta()->parent_page());
      // Replace the page_id field of left most TreeNodeRecord of parent page
      // with the new left-most leave.
      RecordPage* new_leave = CreateNewLeaveWithRecord(record);
      new_leave->Meta()->set_parent_page(parent->id());
      *(parent->Record(search_result->slot) +
        parent->RecordLength(search_result->slot) - 
        sizeof(int)) = new_leave->id();

      // Now the previous left-most leave (crt_leave) is the second leave to
      // the left, and it's tree node record in parent has been replaced by
      // new leave. We need to re-insert its tree node record.
      Schema::TreeNodeRecord tn_record;
      ProduceKeyRecordFromLeaveRecord(prmanager.Record(0), &tn_record);
      tn_record.set_page_id(leave->id());
      if (!InsertTreeNodeRecord(&tn_record, parent)) {
        LogERROR("Inserting new leave to B+ tree failed");
        return false;
      }
      ConnectLeaves(new_leave, leave);
      return true;
    }      
  }

  // Otherwise we check next leave. If next leave has the same parent, and is
  // not overflow page, we can do some redistribution.
  if (search_result->next_child_id >= 0 &&
      FetchPage(search_result->next_child_id)->Meta()->overflow_page() < 0) {
    // First try inserting the new record to next leave.
    if (InsertNewRecordToNextLeave(leave, search_result, record)) {
      return true;
    }

    // Otherwise we need to create a new leave with the new record to insert,
    // and try re-distributing records with the next_leave if possible.
    Schema::TreeNodeRecord tn_record;
    RecordPage* new_leave = CreateNewLeaveWithRecord(record, &tn_record);
    tn_record.set_page_id(new_leave->id());

    // Re-distribute records from next leave to balance.
    ReDistributeRecordsFromNextLeave(new_leave, search_result);

    // Insert tree node record of the new leave to parent.
    auto parent = FetchPage(leave->Meta()->parent_page());
    if (!InsertTreeNodeRecord(&tn_record, parent)) {
      LogERROR("Inserting new leave to B+ tree failed");
      return false;
    }
    RecordPage* page2 = FetchPage(search_result->next_leave_id);
    RecordPage* page1 = FetchPage(page2->Meta()->prev_page());
    ConnectLeaves(new_leave, page2);
    ConnectLeaves(page1, new_leave);
  }
  else {
    // Next leave has different parent, or is overflowed too. We have no
    // choice but creating new leave and just inserting it to parent node.
    Schema::TreeNodeRecord tn_record;
    RecordPage* new_leave = CreateNewLeaveWithRecord(record, &tn_record);
    tn_record.set_page_id(new_leave->id());
    
    // Insert tree node record of the new leave to parent.
    auto parent = FetchPage(leave->Meta()->parent_page());
    if (!InsertTreeNodeRecord(&tn_record, parent)) {
      LogERROR("Inserting new leave to B+ tree failed");
      return false;
    }
    RecordPage* page1 = leave;
    if (search_result->next_leave_id >= 0) {
      RecordPage* page2 = FetchPage(search_result->next_leave_id);
      page1 = FetchPage(page2->Meta()->prev_page());
      ConnectLeaves(new_leave, page2);
    }
    else {
      page1 = GotoOverflowChainEnd(page1);
    }
    ConnectLeaves(page1, new_leave);
  }
  return true;
}

bool BplusTree::ReDistributeWithNextLeave(RecordPage* leave,
                                          SearchTreeNodeResult* search_result,
                                          const Schema::RecordBase* record) {
  // Check next child is valid.
  if (search_result->next_child_id < 0 ||
      FetchPage(search_result->next_child_id)->Meta()->overflow_page() >= 0) {
    return false;
  }

  auto next_leave = FetchPage(search_result->next_child_id);
  CheckLogFATAL(next_leave, "Failed to fetch next child leave");
  // If next leave is even more 'full' than current leave, can't redistribute.
  if (leave->Meta()->space_used() <= next_leave->Meta()->space_used()) {
    return false;
  }

  Schema::PageRecordsManager prmanager(next_leave, schema_.get(), key_indexes_,
                                       file_type_, TREE_LEAVE);
  // Insert new record, sort and group records.
  if (!prmanager.InsertNewRecord(record)) {
    LogFATAL("Can't insert new record to PageRecordsManager");
  }
  std::vector<Schema::RecordGroup> rgroups;
  prmanager.GroupRecords(&rgroups);

  // Find new which record group new record belongs to.
  int new_record_gindex = 0;
  for (; new_record_gindex < (int)rgroups.size(); new_record_gindex++) {
    for (int index = rgroups[new_record_gindex].start_index;
         index < rgroups[new_record_gindex].start_index +
                 rgroups[new_record_gindex].num_records;
         index++) {
      if (prmanager.RecordSlotID(index) < 0) {
        break;
      }
    }
  }

  int records_to_move = 0;
  int size_to_move = 0;
  bool can_insert_new_record = false;
  int gindex = rgroups.size() - 1;
  for (; gindex >= new_record_gindex; gindex--) {
    records_to_move += rgroups[gindex].num_records;
    size_to_move += rgroups[gindex].size;
    if (!next_leave->PreCheckCanInsert(records_to_move, size_to_move)) {
      records_to_move -= rgroups[gindex].num_records;
      size_to_move -= rgroups[gindex].size;
      break;
    }
    if (size_to_move >= record->size()) {
      can_insert_new_record = true;
      break;
    }
  }

  if (!can_insert_new_record) {
    return false;
  }

  int left_size = leave->Meta()->space_used() - size_to_move + record->size();
  int right_size = next_leave->Meta()->space_used() + size_to_move;
  CheckLogFATAL(prmanager.total_size_ - size_to_move == left_size,
                "left leave size error");
  CheckLogFATAL(next_leave->Meta()->space_used() + size_to_move == right_size,
                "right leave size error");

  int min_gap = std::abs(left_size - right_size);
  gindex--;
  for (; left_size > right_size && gindex >= 0; gindex--) {
    records_to_move += rgroups[gindex].num_records;
    size_to_move += rgroups[gindex].size;
    if (!next_leave->PreCheckCanInsert(records_to_move, size_to_move)) {
      break;
    }

    left_size -= rgroups[gindex].size;
    right_size += rgroups[gindex].size;
    if (std::abs(left_size - right_size) > min_gap) {
      break;
    }
  }
  gindex++;

  // Move from next_leave to leave.
  for (int index = rgroups[gindex].start_index;
       index < (int)rgroups.size();
       index++) {
    int slot_id = prmanager.RecordSlotID(index);
    if (slot_id >= 0) {
      CheckLogFATAL(leave->DeleteRecord(slot_id),
                    "Failed to remove record from current leave");
    }
    if (prmanager.Record(index)->InsertToRecordPage(next_leave) < 0) {
      LogFATAL("Failed to insert record to next page");
    }
  }

  // Delete original tree node record of next leave from parent.
  auto parent = FetchPage(next_leave->Meta()->parent_page());
  CheckLogFATAL(parent, "Failed to fetch parent of next leave");
  CheckLogFATAL(parent->DeleteRecord(search_result->next_slot),
                "Failed to delete next leave's original tree node record");

  // Insert new tree node record of next leave to parent.
  Schema::TreeNodeRecord tn_record;
  auto new_min_record = prmanager.Record(rgroups[gindex].start_index);
  ProduceKeyRecordFromLeaveRecord(new_min_record, &tn_record);
  tn_record.set_page_id(next_leave->id());
  CheckLogFATAL(InsertTreeNodeRecord(&tn_record, parent),
                "Failed to insert new tree node record for next leave");

  return true;
}

bool BplusTree::InsertRecord(const Schema::RecordBase* record) {
  if (!record) {
    LogERROR("record to insert is nullptr");
    return false;
  }

  // Verify this data record fields matches schema.
  if (!CheckRecordFieldsType(record)) {
    LogERROR("Record fields type mismatch table schema");
    return false;
  }

  // If tree is empty, add first leave to it.
  if (header_->root_page() < 0) {
    auto leave = CreateNewLeaveWithRecord(record);
    return AddFirstLeaveToTree(leave);
  }

  // B+ tree is not empty. Search key to find the leave to insert new record.
  // Produce the key of the record to search.
  Schema::RecordBase search_key;
  ProduceKeyRecordFromLeaveRecord(record, &search_key);

  // Search the leave to insert.
  RecordPage* crt_page = FetchPage(header_->root_page());
  SearchTreeNodeResult search_result;
  while (crt_page && crt_page->Meta()->page_type() == TREE_NODE) {
    search_result = SearchInTreeNode(crt_page, &search_key);
    crt_page = FetchPage(search_result.child_id);
  }
  CheckLogFATAL(crt_page, "Failed to search for key");
  CheckLogFATAL(crt_page->Meta()->page_type() == TREE_LEAVE,
                "Key search ending at non-leave node");

  // If current leave is overflowed, we check if the new record is same with
  // existing ones in it. If yes, we can insert this record to overflow pages.
  // If not, we have go to next leave.
  if (crt_page->Meta()->overflow_page() >= 0) {
    return InsertAfterOverflowLeave(crt_page, &search_result, record);
  }

  // Try inserting the new record to leave.
  if (record->InsertToRecordPage(crt_page) >= 0) {
    return true;
  }

  // Try to re-organize records with next leave, it applicable (next leave
  // must exists, not an overflow leave, and has the same parent).
  if (ReDistributeWithNextLeave(crt_page, &search_result, record)) {
    return true;
  }

  // We have to insert and split current leave.
  int next_page_id = search_result.next_leave_id;
  if (InsertNewRecordToLeaveWithSplit(crt_page, next_page_id, record)) {
    return true;
  }

  LogERROR("Failed to insert new record to B+ tree");
  return false;
}

RecordPage* BplusTree::GotoOverflowChainEnd(RecordPage* leave) {
  if (!leave) {
    LogERROR("Nullptr input to GotoOverflowChainEnd");
    return nullptr;
  }
  if (leave->Meta()->overflow_page() < 0) {
    //LogERROR("Leave %d not overflowed, skipping", leave->id());
    return leave;
  }

  RecordPage* crt_leave = leave;
  while (crt_leave) {
    int next_of_id = crt_leave->Meta()->overflow_page();
    if (next_of_id >= 0) {
      RecordPage* next_of_page = FetchPage(next_of_id);
      CheckLogFATAL(next_of_page, "Get nullptr overflow page");
      crt_leave = next_of_page;
    }
    else {
      break;
    }
  }
  return crt_leave;
}

bool BplusTree::InsertNewRecordToOverFlowChain(
         RecordPage* leave, const Schema::RecordBase* record) {
  if (!leave || !record) {
    LogERROR("Nullptr input to CreateNewLeaveWithRecord");
    return false;
  }

  // First verify that the new record is same with existing records.
  Schema::PageRecordsManager prmanager(leave, schema_.get(), key_indexes_,
                                       file_type_, TREE_LEAVE);
  if (prmanager.CompareRecords(record, prmanager.Record(0)) != 0) {
    return false;
  }

  // record->Print();
  // prmanager.Record(0)->Print();
  // prmanager.Record(prmanager.NumRecords() - 1)->Print();

  RecordPage* crt_leave = GotoOverflowChainEnd(leave);
  // Either insert record to last overflow leave, or append new overflow leave
  // if last overflow leave is full.
  if (record->InsertToRecordPage(crt_leave) < 0) {
    // Append new overflow page.
    auto next_leave = FetchPage(crt_leave->Meta()->next_page());
    auto new_of_leave = AppendOverflowPageTo(crt_leave);
    CheckLogFATAL(record->InsertToRecordPage(new_of_leave) >= 0,
                  "Failed to insert record to new overflow leave");
    if (next_leave) {
      ConnectLeaves(new_of_leave, next_leave);
    }
  }
  return true;
}

RecordPage* BplusTree::CreateNewLeaveWithRecord(
    const Schema::RecordBase* record, Schema::TreeNodeRecord* tn_record) {
  if (!record) {
    LogERROR("Nullptr input to CreateNewLeaveWithRecord");
    return nullptr;
  }

  RecordPage* new_leave = AllocateNewPage(TREE_LEAVE);
  CheckLogFATAL(record->InsertToRecordPage(new_leave) >= 0,
                "Failed to insert record to empty leave");
  if (tn_record) {
    ProduceKeyRecordFromLeaveRecord(record, tn_record);
  }
  return new_leave;
}

bool BplusTree::InsertNewRecordToLeaveWithSplit(
         RecordPage* leave, int next_leave_id,
         const Schema::RecordBase* record) {
  //printf("Splitting leave %d\n", leave->id());
  if (!leave || !record) {
    LogERROR("Nullptr input to InsertNewRecordToLeaveWithSplit");
    return false;
  }

  // Need to split the page.
  Schema::PageRecordsManager prmanager(leave, schema_.get(), key_indexes_,
                                       file_type_, TREE_LEAVE);
  prmanager.set_tree(this);
  auto leaves = prmanager.InsertRecordAndSplitPage(record);

  // Insert back split leave pages to parent tree node(s).
  RecordPage* parent = FetchPage(leave->Meta()->parent_page());
  CheckLogFATAL(parent, "Failed to fetch parent page of current leave");
  if (leaves.size() > 1) {
    //printf("%d split leaves returned\n", (int)leaves.size());
    for (int i = 1; i < (int)leaves.size(); i++) {
      Schema::TreeNodeRecord tn_record;
      ProduceKeyRecordFromLeaveRecord(leaves[i].record.get(), &tn_record);
      tn_record.set_page_id(leaves[i].page->id());
      
      if (!InsertTreeNodeRecord(&tn_record, parent)) {
        LogFATAL("Failed to add split leave %d into parent");
      }
      // Reset parent because upper node may have been split after last leave
      // was inserted. We always add next leave to the same parent as of its
      // prev leave.
      parent = FetchPage(leaves[i].page->Meta()->parent_page());
      CheckLogFATAL(parent, "Failed to fetch parent page of current leave");
    }
    // Connect last split leave with following leaves.
    if (next_leave_id >= 0) {
      ConnectLeaves(leaves[leaves.size()-1].page, FetchPage(next_leave_id));
    }
  }
  else {
    //printf("overflow self\n");
    // Only one leave - original leave with a overflow page.
    auto of_leave = FetchPage(leaves[0].page->Meta()->overflow_page());
    CheckLogFATAL(of_leave, "Failed to fetch overflow page of non-split leave");
    if (next_leave_id >= 0) {
      ConnectLeaves(of_leave, FetchPage(next_leave_id));
    }
  }
  return true;
}


}  // namespace DataBaseFiles

