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

void BplusTreeHeaderPage::reset() {
  root_page_ = -1;
  num_leaves_ = 0;
  depth_ = 0;
  num_pages_ = 0;
  num_free_pages_ = 0;
  num_used_pages_ = 0;
  free_page_ = -1;
}

// ****************************** BplusTree **********************************//
BplusTree::~BplusTree() {
  if (file_) {
    // Checkout all remaining pages. We don't call CheckoutPage() since it
    // involves map.erase() operation internally which is tricky.
    //printf("Checking out %d pags\n", page_map_.size());
    SaveToDisk();
    page_map_.clear();
    fflush(file_);
    fclose(file_);
  }
}

BplusTree::BplusTree(DataBase::Table* table,
                     FileType file_type,
                     std::vector<int> key_indexes,
                     bool create) :
    table_(table),
    file_type_(file_type),
    key_indexes_(key_indexes) {
  if (create) {
    CheckLogFATAL(CreateBplusTreeFile(), "Failed to create new B+ tree file");
  }
  else {
    CheckLogFATAL(LoadTree(), "Failed to load B+ tree");
  }
}

bool BplusTree::LoadTree() {
  std::string filename = table_->BplusTreeFileName(file_type_, key_indexes_);
  file_ = fopen(filename.c_str(), "r+");
  if (file_) {
    if (!LoadFromDisk()) {
      LogFATAL("Can't init B+ tree file %s", filename.c_str());
    }
    return true;
  }
  return false;
}

bool BplusTree::CreateBplusTreeFile() {
  if (!table_) {
    LogERROR("No database table for this B+ tree");
    return false;
  }

  std::string filename = table_->BplusTreeFileName(file_type_, key_indexes_);
  if (file_) {
    fclose(file_);
  }

  file_ = fopen(filename.c_str(), "w+");
  if (!file_) {
    LogERROR("open file %s failed", filename.c_str());
    return false;
  }

  // Create header page.
  header_.reset(new BplusTreeHeaderPage(file_, file_type_));
  header_->set_num_pages(1);
  header_->set_num_used_pages(1);

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
    LogINFO("LoadRootNode() root_page_id = %d", root_page_id);
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

Schema::TableSchema* BplusTree::schema() const {
  return table_->schema();
}

bool BplusTree::SaveToDisk() const {
  // Save header page
  if (header_ && !header_->SaveToDisk()) {
    LogERROR("Failed to saven B+ tree meta to disk");
    return false;
  }

  // Save B+ tree nodes. Root node, tree node and leaves.
  for (auto& entry: page_map_) {
    if (!entry.second->DumpPageData()) {
      LogERROR("Save page %d to disk failed", entry.first);
    }
  }

  return true;
}

bool BplusTree::LoadFromDisk() {
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
    header_->set_free_page(page->Meta()->next_page());
  }
  else {
    // Append new page at the end of file.
    new_page_id = header_->num_pages();
    header_->increment_num_pages(1);
  }

  page = new RecordPage(new_page_id, file_);
  page->InitInMemoryPage();

  page->Meta()->set_page_type(page_type);
  page_map_[page->id()] = std::shared_ptr<RecordPage>(page);
  header_->increment_num_used_pages(1);
  if (page_type == TREE_LEAVE) {
    header_->increment_num_leaves(1);
    printf("Allocated new leave %d\n", page->id());
  }
  return page;
}

bool BplusTree::RecyclePage(int page_id) {
  // Fetch page in case it's been swapped to disk. We need to write one field
  // in the page - next_page, to link to the free page list.
  //printf("Recycling page %d\n", page_id);
  auto page = Page(page_id);
  auto type = page->Meta()->page_type();
  if (!page) {
    LogERROR("Failed to fetch page %d", page_id);
    return false;
  }
  page->Meta()->set_next_page(header_->free_page());
  header_->set_free_page(page_id);
  if (!page->DumpPageData()) {
    LogERROR("Save page %d to disk failed", page_id);
    return false;
  }
  page_map_.erase(page_id);

  header_->decrement_num_used_pages(1);
  header_->increment_num_free_pages(1);
  if (type == TREE_LEAVE) {
    header_->decrement_num_leaves(1);
  }
  return true;
}

RecordPage* BplusTree::Page(int page_id) {
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

byte* BplusTree::Record(Schema::RecordID rid) {
  auto page = Page(rid.page_id());
  if (!page) {
    return nullptr;
  }
  return page->Record(rid.slot_id());
}

bool BplusTree::VerifyRecord(const Schema::RecordID& rid,
                             const Schema::RecordBase* record) {
  auto page = Page(rid.page_id());
  if (!page) {
    return false;
  }

  if (page->RecordLength(rid.slot_id()) != record->size()) {
    return false;
  }

  byte buf[record->size()];
  record->DumpToMem(buf);
  if (strncmp((const char*)page->Record(rid.slot_id()),
              (const char*)buf,
              record->size()) != 0) {
    return false;
  }
  //record->Print();

  return true;
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
    RecordPage* child_page = Page(tn_record->page_id());
    child_page->Meta()->set_parent_page(tn_page->id());
    return true;
  }

  // The parent is full and needs to be split.
  Schema::PageRecordsManager prmanager(tn_page,
                                       schema(), key_indexes_,
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
    RecordPage* child_page = Page(child_page_id);
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
      RecordPage* child_page = Page(tn_record->page_id());
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
  RecordPage* parent_node = Page(tn_page->Meta()->parent_page());
  if (!InsertTreeNodeRecord(right_upper_tn_record, parent_node)) {
    LogERROR("Add new tree node to parent node failed");
    return false;
  }

  return true;
}

bool BplusTree::AddFirstLeaveToTree(RecordPage* leave) {
  Schema::TreeNodeRecord min_tn_record;
  min_tn_record.InitRecordFields(schema(), key_indexes_,
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
  return true;
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
    tree_node = Page(bl_status_.prev_leave->Meta()->parent_page());
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
  int page1_id = page1 ? page1->id() : -1;
  int page2_id = page2 ? page2->id() : -1;
  printf("Connecting %d and %d\n", page1_id, page2_id);
  if (page1) {
    page1->Meta()->set_next_page(page2_id);
  }
  if (page2) {
    page2->Meta()->set_prev_page(page1_id);
  }
  return true;
}

std::vector<int> BplusTree::IndexesToCompareLeaveRecords() const {
  std::vector<int> indexes;
  if (file_type_ == DataBaseFiles::INDEX_DATA) {
    indexes = key_indexes_;
  }
  else {
    for (int i = 0; i < (int)key_indexes_.size(); i++) {
      indexes.push_back(i);
    }
  }
  return indexes;
}

bool BplusTree::BlukLoadInsertRecordToLeave(RecordPage* leave,
                                            Schema::RecordBase* record) {
  int slot_id = record->InsertToRecordPage(leave);
  if (slot_id >= 0) {
    bl_status_.last_record.reset(record->Duplicate());
    bl_status_.rid.set_page_id(leave->id());
    bl_status_.rid.set_slot_id(slot_id);
    return true;
  }
  return false;
}

// BulkLoading records.
bool BplusTree::BulkLoad(
         std::vector<std::shared_ptr<Schema::RecordBase>>& records) {
  CheckLogFATAL(CreateBplusTreeFile(),
                "Failed to create B+ tree file for BulkLoad");
  for (int i = 0; i < (int)records.size(); i++) {
    if (!BulkLoadRecord(records[i].get())) { 
      LogERROR("Load record %d failed, stop loading ...", i);
      return false;
    }
  }
  return true;
}

bool BplusTree::BulkLoadRecord(Schema::RecordBase* record) {
  if (!record) {
    LogERROR("Record to insert is nullptr");
    return false;
  }

  bl_status_.rid.reset();

  // Try inserting the record to current leave page. If success, we continue.
  // Otherwise it is possible that current leave is nullptr (empty B+ tree,
  // no record ever inserted), or current leave is full. In either case, we
  // need to allocate a new leave page and continue inserting.
  if (bl_status_.crt_leave &&
      bl_status_.crt_leave->Meta()->is_overflow_page() == 0 &&
      BlukLoadInsertRecordToLeave(bl_status_.crt_leave, record)) {
    return true;
  }

  // Check boundary duplication. If new record equals last record at the end of
  // its leave, we need to move these duplicates to new leave.
  if (bl_status_.crt_leave && bl_status_.last_record &&
      Schema::RecordBase::CompareRecordsBasedOnIndex(
          record, bl_status_.last_record.get(),
          IndexesToCompareLeaveRecords()) == 0) {
    bl_status_.last_record.reset(record->Duplicate());
    return CheckBoundaryDuplication(record);
  }

  // Normal insertion. Allocate a new leave node and insert the record.
  RecordPage* new_leave = AppendNewLeave();
  if (!BlukLoadInsertRecordToLeave(new_leave, record)) {
    LogFATAL("Failed to insert record to new leave node");
  }

  // We add this new leave node with one record, to a upper tree node. Note that
  // we only do this when having allocated a new leave node and had just first
  // record inserted to it.
  Schema::TreeNodeRecord tn_record;
  ProduceKeyRecordFromNodeRecord(record, &tn_record);
  if (!AddLeaveToTree(new_leave, &tn_record)) {
    LogFATAL("Failed to add leave to B+ tree node");
  }

  return true;
}

bool BplusTree::CheckBoundaryDuplication(Schema::RecordBase* record) {
  // Try inserting first. This deals with a non-full overflow page.
  if (BlukLoadInsertRecordToLeave(bl_status_.crt_leave, record)) {
    return true;
  }

  // Load curret leave records and find the first duplicate with the new record
  // to be insert.
  Schema::PageRecordsManager prmanager(bl_status_.crt_leave,
                                       schema(), key_indexes_,
                                       file_type_, TREE_LEAVE);

  int index = prmanager.NumRecords() - 1;
  for (; index >= 0; index--) {
    if (Schema::RecordBase::CompareRecordsBasedOnIndex(
            record, prmanager.Record(index),
            IndexesToCompareLeaveRecords()) != 0) {
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
    ProduceKeyRecordFromNodeRecord(mid_record, &tn_record);
    if (!AddLeaveToTree(new_leave, &tn_record)) {
      LogFATAL("Failed to add leave to B+ tree");
    }
    // Re-try inserting record to new crt_leave.
    if (BlukLoadInsertRecordToLeave(new_leave, record)) {
      return true;
    }
  }

  //LogINFO("New duplicated record needs a new overflow page.");
  RecordPage* overflow_leave = AppendNewOverflowLeave();
  if (!BlukLoadInsertRecordToLeave(overflow_leave, record)) {
    LogFATAL("Insert first duplicated record to overflow page faield - "
             "This should not happen!");
  }

  return true;
}

bool BplusTree::ValidityCheck() {
  if (header_->root_page() < 0) {
    LogINFO("No root node. Empty B+ tree");
    return true;
  }

  std::queue<RecordPage*> page_q;
  RecordPage* root = Page(header_->root_page());
  if (!root) {
    return VerifyEmptyTree();
  }

  // Level-traverse the B+ tree.
  page_q.push(root);
  vc_status_.count_num_pages = 2; // header page + root
  vc_status_.count_num_used_pages = 2;
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

  // Collect free pages
  auto free_page = Page(header_->free_page());
  while (free_page) {
    vc_status_.count_num_free_pages++;
    free_page = Page(free_page->Meta()->next_page());
  }

  if (!MetaConsistencyCheck()) {
    return false;
  }

  return true;
}

bool BplusTree::VerifyEmptyTree() const {
  if (header_->root_page() != -1) {
    LogERROR("Expect root page = 1, actual %d", header_->root_page());
    return false;
  }
  if (header_->num_used_pages() != 0) {
    LogERROR("Expect num_used_pages = 0, actual %d", header_->num_used_pages());
    return false;
  }
  if (header_->depth() != 0) {
    LogERROR("Expect depth = 0, actual %d", header_->depth());
    return false;
  }
  return true;
}

bool BplusTree::CheckTreeNodeValid(RecordPage* page) {
  if (page->Meta()->page_type() != TREE_NODE) {
    LogERROR("Wrong node type - not tree node");
    return false;
  }

  Schema::PageRecordsManager prmanager(page, schema(), key_indexes_,
                                       file_type_, TREE_NODE);
  
  for (int i = 0; i < prmanager.NumRecords(); i++) {
    auto tn_record = prmanager.GetRecord<Schema::TreeNodeRecord>(i);
    int child_page_id = tn_record->page_id();

    Schema::TreeNodeRecord* next_record = nullptr;
    if (i < prmanager.NumRecords() - 1) {
      next_record = prmanager.GetRecord<Schema::TreeNodeRecord>(i + 1);
    }
    if (!VerifyChildRecordsRange(Page(child_page_id),
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

  if (left_bound && right_bound && *left_bound >= *right_bound) {
    LogERROR("left_bound should be no greater than right_bound, but:");
    left_bound->Print();
    right_bound->Print();
    return false;
  }

  Schema::PageRecordsManager prmanager(child_page, schema(), key_indexes_,
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
    LogERROR("last record key of child >= right bound, "
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

  Schema::PageRecordsManager prmanager(page, schema(), key_indexes_,
                                       file_type_, TREE_NODE);
  
  bool children_are_leave = false;
  vc_status_.count_num_pages += prmanager.NumRecords();
  vc_status_.count_num_used_pages += prmanager.NumRecords();
  for (int i = 0; i < prmanager.NumRecords(); i++) {
    auto tn_record = prmanager.GetRecord<Schema::TreeNodeRecord>(i);
    RecordPage* child_page = Page(tn_record->page_id());
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
          child_page = Page(child_page->Meta()->overflow_page());
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
  Schema::PageRecordsManager prmanager(page, schema(), key_indexes_,
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
  if (vc_status_.count_num_pages !=
      header_->num_pages() - header_->num_free_pages()) {
    LogERROR("num valid pages inconsistent - expect %d, actual %d",
             header_->num_pages() - header_->num_free_pages(),
             vc_status_.count_num_pages);
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

int BplusTree::SearchRecords(
         const Schema::RecordBase* key,
         std::vector<std::shared_ptr<Schema::RecordBase>>* result) {
  auto leave = SearchByKey(key);
  if (!leave) {
    LogERROR("Can't search to leave by this key");
    return -1;
  }

  result->clear();
  FetchResultsFromLeave(leave, key, result);
  return result->size();
}

RecordPage* BplusTree::SearchByKey(const Schema::RecordBase* key) {
  if (!key) {
    LogERROR("Nullptr key to SearchByKey");
    return nullptr;
  }

  if (!CheckKeyFieldsType(key)) {
    LogERROR("Key fields type mismatch table schema of this B+ tree");
    return nullptr;
  }

  RecordPage* crt_page = Page(header_->root_page());
  while (crt_page && crt_page->Meta()->page_type() == TREE_NODE) {
    auto result = SearchInTreeNode(crt_page, key);
    crt_page = Page(result.child_id);
  }

  if (!crt_page) {
    LogERROR("Failed to search for key");
    key->Print();
    return nullptr;
  }

  return crt_page;
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
    Schema::PageRecordsManager prmanager(leave, schema(), key_indexes_,
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
      leave = Page(leave->Meta()->overflow_page());
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

  Schema::PageRecordsManager prmanager(page, schema(), key_indexes_,
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

  // Get next child & leave.
  if (index < prmanager.NumRecords() - 1) {
    result.next_slot = prmanager.RecordSlotID(index + 1);
    result.next_record = prmanager.plrecords().at(index + 1).Record();
    tn_record = prmanager.GetRecord<Schema::TreeNodeRecord>(index + 1);
    result.next_child_id = tn_record->page_id();
    CheckLogFATAL(result.next_child_id >= 0,
                  "Get nullptr next_child_page in SearchInTreeNode");
  }
  // Get next leave id. It's not always next_child because next leave may have
  // different parent.
  auto child_page = Page(result.child_id);
  CheckLogFATAL(child_page, "child page is nullptr");
  child_page = GotoOverflowChainEnd(child_page);
  if (child_page->Meta()->next_page() >= 0) {
    result.next_leave_id = child_page->Meta()->next_page();
  }

  // Get prev child & leave.
  if (index > 0) {
    result.prev_slot = prmanager.RecordSlotID(index - 1);
    result.prev_record = prmanager.plrecords().at(index - 1).Record();
    tn_record = prmanager.GetRecord<Schema::TreeNodeRecord>(index - 1);
    result.prev_child_id = tn_record->page_id();
    CheckLogFATAL(result.prev_child_id >= 0,
                  "Get nullptr next_child_page in SearchInTreeNode");
  }
  child_page = Page(result.child_id);
  CheckLogFATAL(child_page, "child page is nullptr");
  if (child_page->Meta()->prev_page() >= 0) {
    result.prev_leave_id = child_page->Meta()->prev_page();
  }

  return result;
}

BplusTree::SearchTreeNodeResult
BplusTree::LookUpTreeNodeInfoForPage(RecordPage* page) {
  SearchTreeNodeResult result;
  if (!page) {
    LogERROR("input page is nullptr");
    return result;
  }

  auto parent = Page(page->Meta()->parent_page());
  if (!parent) {
    LogERROR("Can't get parent for page %d", page->id());
    return result;
  }

  Schema::PageRecordsManager prmanager(parent, schema(), key_indexes_,
                                       file_type_, TREE_NODE);
  bool tn_record_found = false;
  for (int i = 0; i < (int)prmanager.NumRecords(); i++) {
    auto tn_record = prmanager.GetRecord<Schema::TreeNodeRecord>(i);
    int child_page_id = tn_record->page_id();
    if (child_page_id == page->id()) {
      tn_record_found = true;
      result.slot = prmanager.RecordSlotID(i);
      result.child_id = child_page_id;
      result.record = prmanager.plrecords().at(i).Record();
      // prev child.
      if (i > 0) {
        result.prev_slot = prmanager.RecordSlotID(i - 1);
        result.prev_child_id =
            (prmanager.GetRecord<Schema::TreeNodeRecord>(i - 1))->page_id();
        result.prev_record = prmanager.plrecords().at(i - 1).Record();
      }
      // next child.
      if (i < prmanager.NumRecords() - 1) {
        result.next_slot = prmanager.RecordSlotID(i + 1);
        result.next_child_id =
            (prmanager.GetRecord<Schema::TreeNodeRecord>(i + 1))->page_id();
        result.next_record = prmanager.plrecords().at(i + 1).Record();
      }
    }
  }

  CheckLogFATAL(tn_record_found,"Can't find TreeNodeRecord for page %d, type%d",
                                page->id(), page->Meta()->page_type());
  return result;
}

bool BplusTree::CheckKeyFieldsType(const Schema::RecordBase* key) const {
  return key->CheckFieldsType(schema(), key_indexes_);
}

bool BplusTree::CheckRecordFieldsType(const Schema::RecordBase* record) const {
  if (record->type() == Schema::DATA_RECORD) {
    if (file_type_ == INDEX) {
      return false;
    }
    if (!record->CheckFieldsType(schema())) {
      return false;
    }
    return true;
  }
  if (record->type() == Schema::INDEX_RECORD) {
    if (file_type_ == INDEX_DATA) {
      return false;
    }
    if (!record->CheckFieldsType(schema(), key_indexes_)) {
      return false;
    }
    return true;
  }

  LogERROR("Invalid ReocrdType %d to insert to B+ tree", record->type());
  return false;
}

bool BplusTree::ProduceKeyRecordFromNodeRecord(
         const Schema::RecordBase* node_record, Schema::RecordBase* tn_record) {
  if (!node_record || !tn_record) {
    LogERROR("Nullptr input to ProduceKeyRecordFromNodeRecord");
    return false;
  }
  if (file_type_ == INDEX_DATA &&
      node_record->type() == Schema::DATA_RECORD) {
    (reinterpret_cast<const Schema::DataRecord*>(node_record))
        ->ExtractKey(tn_record, key_indexes_);
  }
  else if (file_type_ == INDEX &&
           node_record->type() == Schema::INDEX_RECORD){
    tn_record->CopyFieldsFrom(node_record);
  }
  else if (node_record->type() == Schema::TREENODE_RECORD) {
    tn_record->CopyFieldsFrom(node_record);
  }
  else {
    LogFATAL("Unsupported B+ tree type and leave record type (%d, %d)",
         file_type_, node_record->type());
  }
  return true;
}

Schema::RecordID
BplusTree::InsertNewRecordToNextLeave(RecordPage* leave,
                                      SearchTreeNodeResult* search_result,
                                      const Schema::RecordBase* record) {
  // Check next leave is valid, and has same parent.
  if (search_result->next_child_id < 0 &&
      Page(search_result->next_child_id)->Meta()->overflow_page() >= 0) {
    return Schema::RecordID();
  }

  auto next_leave = Page(search_result->next_child_id);
  CheckLogFATAL(next_leave, "Failed to fetch next leave");
  auto parent = Page(leave->Meta()->parent_page());
  CheckLogFATAL(parent, "Failed to fetch parent node");

  int slot_id = record->InsertToRecordPage(next_leave);
  if (slot_id < 0) {
    return Schema::RecordID();
  }

  // Delete next leave's tree node record from parent.
  CheckLogFATAL(parent->DeleteRecord(search_result->next_slot),
                "Failed to delete next leave's original tree node record");
  Schema::TreeNodeRecord tn_record;
  ProduceKeyRecordFromNodeRecord(record, &tn_record);
  tn_record.set_page_id(next_leave->id());
  CheckLogFATAL(InsertTreeNodeRecord(&tn_record, parent),
                "Failed to insert new tree node record for next leave");

  return Schema::RecordID(next_leave->id(), slot_id);
}

bool BplusTree::ReDistributeRecordsWithinTwoPages(
         RecordPage* page1, RecordPage* page2, int page2_slot_id_in_parent,
         std::vector<Schema::DataRecordRidMutation>* rid_mutations,
         bool force_redistribute) {
  if (!page1 || !page2) {
    LogERROR("Nullptr page input to ReDistributeRecordsWithinTwoPages");
    return false;
  }

  if (page1->Meta()->overflow_page() > 0 ||
      page2->Meta()->overflow_page() > 0) {
    LogERROR("Can't re-distribute records between overflow pages");
    return false;
  }

  // Check if two pages can be merged. If mergable and not force redistribute,
  // do not redistribute. This is the case in record deletion when two pages
  // should be merged rather than redistributed.
  if (!force_redistribute &&
      page1->PreCheckCanInsert(page2->Meta()->num_records(),
                               page2->Meta()->space_used())) {
    return false;
  }

  int min_gap = page1->Meta()->space_used() - page2->Meta()->space_used();
  if (min_gap == 0) {
    return true;
  }

  int move_direction = min_gap < 0 ? -1 : 1;
  min_gap = std::abs(min_gap);
  RecordPage *src_page, *dest_page;
  if (move_direction < 0) {
    // move records: page1 <-- page2
    src_page = page2,
    dest_page = page1;
  }
  else {
    // move records: page1 --> page2
    src_page = page1,
    dest_page = page2;
  }
  Schema::PageRecordsManager prmanager(src_page, schema(), key_indexes_,
                                       file_type_, page1->Meta()->page_type());
  std::vector<Schema::RecordGroup> rgroups;
  prmanager.GroupRecords(&rgroups);

  int gindex_start = move_direction < 0 ? 0 : (rgroups.size() - 1);
  int gindex_end = move_direction < 0 ? rgroups.size() : -1;
  int gindex = gindex_start;
  for (; gindex != gindex_end; gindex -= move_direction) {
    if (std::abs(dest_page->Meta()->space_used() + rgroups[gindex].size -
                 src_page->Meta()->space_used() - rgroups[gindex].size)
        > min_gap) {
      break;
    }
    if (!dest_page->PreCheckCanInsert(rgroups[gindex].num_records,
                                      rgroups[gindex].size)) {
      break;
    }
    // Move from src_page to current dest_page.
    for (int index = rgroups[gindex].start_index;
         index < rgroups[gindex].start_index + rgroups[gindex].num_records;
         index++) {
      int slot_id = prmanager.RecordSlotID(index);

      // If it's tree node, update parent info for those child nodes
      // corrosponding tree node records that are redistributed.
      if (dest_page->Meta()->page_type() == TREE_NODE) {
        auto tn_record = prmanager.GetRecord<Schema::TreeNodeRecord>(index);
        int child_page_id = tn_record->page_id();
        RecordPage* child_page = Page(child_page_id);
        if (child_page) {
          child_page->Meta()->set_parent_page(dest_page->id());
        }        
      }
      src_page->DeleteRecord(slot_id);

      int new_slot_id = prmanager.Record(index)->InsertToRecordPage(dest_page);
      CheckLogFATAL(new_slot_id >= 0,
                    "Failed to move next page's record forward to new page");
      // If it's data tree leave, add rid mutations.
      if (file_type_ == INDEX_DATA &&
          dest_page->Meta()->page_type() == TREE_LEAVE &&
          rid_mutations) {
        rid_mutations->emplace_back(prmanager.Shared_Record(index),
                           Schema::RecordID(src_page->id(), slot_id),
                           Schema::RecordID(dest_page->id(), new_slot_id));
      }
    }
  }
  // No record is re-distributed.
  if (gindex == gindex_start) {
    printf("No record moved\n");
    return true;
  }

  printf("gindex = %d, gindex_start = %d\n", gindex, gindex_start);
  if (move_direction > 0) {
    gindex++;
  }

  // Delete original tree node record of page2 from parent.
  auto parent = Page(page2->Meta()->parent_page());
  CheckLogFATAL(parent, "Failed to fetch parent of next leave");
  CheckLogFATAL(parent->DeleteRecord(page2_slot_id_in_parent),
                "Failed to delete next leave's original tree node record");

  // Insert new tree node record of page2 to parent.
  Schema::TreeNodeRecord tn_record;
  auto new_min_record = prmanager.Record(rgroups[gindex].start_index);
  ProduceKeyRecordFromNodeRecord(new_min_record, &tn_record);
  tn_record.set_page_id(page2->id());
  CheckLogFATAL(InsertTreeNodeRecord(&tn_record, parent),
                "Failed to insert new tree node record for next leave");

  return true;
}

bool BplusTree::MergeTwoNodes(
         RecordPage* page1, RecordPage* page2,
         int page2_slot_id_in_parent,
         std::vector<Schema::DataRecordRidMutation>* rid_mutations) {
  if (!page1 || !page2) {
    LogERROR("nullptr page1/page2 input to MergeTwoNodes");
    return false;
  }

  printf("merging %d and %d\n", page1->id(), page2->id());

  if (page1->Meta()->overflow_page() > 0 ||
      page2->Meta()->overflow_page() > 0) {
    LogERROR("Can't re-distribute records between overflow pages");
    return false;
  }

  // We always merge forward - page1 <-- page2.
  // Page2 is deleted so that we don't need to update tree node record of page1
  // in parent.
  if (!page1->PreCheckCanInsert(page2->Meta()->num_records(),
                                page2->Meta()->space_used())) {
    return false;
  }

  Schema::PageRecordsManager prmanager(page2, schema(), key_indexes_,
                                       file_type_, page2->Meta()->page_type());

  for (int i = 0; i < prmanager.NumRecords(); i++) {
    int slot_id = prmanager.RecordSlotID(i);
    if (page2->Meta()->page_type() == TREE_NODE) {
      auto tn_record = prmanager.GetRecord<Schema::TreeNodeRecord>(i);
      int child_page_id = tn_record->page_id();
      RecordPage* child_page = Page(child_page_id);
      if (child_page) {
        child_page->Meta()->set_parent_page(page1->id());
      }        
    }

    // Move to page1.
    int new_slot_id = prmanager.Record(i)->InsertToRecordPage(page1);
    if (new_slot_id < 0) {
      LogFATAL("Failed to move record from page2 to page1");
    }
    if (file_type_ == INDEX_DATA &&
        page2->Meta()->page_type() == TREE_LEAVE &&
        rid_mutations) {
      rid_mutations->emplace_back(prmanager.Shared_Record(i),
                         Schema::RecordID(page2->id(), slot_id),
                         Schema::RecordID(page1->id(), new_slot_id));
    }
  }

  // Delete tree node record of page2 from parent.
  DeleteNodeFromTree(page2, page2_slot_id_in_parent);

  return true;
}

bool BplusTree::DeleteNodeFromTree(RecordPage* page, int slot_id_in_parent) {
  if (!page) {
    LogERROR("can't delete nullptr page");
    return false;
  }

  // delete root page - tree will become empty and reset.
  if (page->id() == header_->root_page()) {
    header_->reset();
    page_map_.clear();
    if (ftruncate(fileno(file_), kPageSize) != 0) {
      return false;
    }
    return true;
  }

  auto parent = Page(page->Meta()->parent_page());
  CheckLogFATAL(parent, "can't find parent for non-root page %d", page->id());

  if (slot_id_in_parent < 0) {
    auto result = LookUpTreeNodeInfoForPage(page);
    slot_id_in_parent = result.slot;
  }
  CheckLogFATAL(slot_id_in_parent >= 0, "Invalid slot id of page %d in parent");

  // Parse the TreeNodeRecord in parent and check it matches this page id.
  int page_id = *(reinterpret_cast<int32*>(
                      parent->Record(slot_id_in_parent) +
                      parent->RecordLength(slot_id_in_parent) - sizeof(int32))
                  );
  if (page_id != page->id()) {
    LogFATAL("tree node record %d inconsistent with child page id %d",
             page_id, page->id());
  }

  // Delete the tree node record of this page from its parent.
  CheckLogFATAL(parent->DeleteRecord(slot_id_in_parent),
                "Failed to delete tree node record of page %d", page->id());

  // Connect leaves after deleting.
  if (page->Meta()->page_type() == TREE_LEAVE) {
    ConnectLeaves(Page(page->Meta()->prev_page()),
                  Page(page->Meta()->next_page()));
  }

  // put the deleted page into free page pool.
  CheckLogFATAL(RecyclePage(page->id()),
                "Failed to recycle page %d", page->id());

  // Parent node need to re-distribute records or merge nodes because of tree
  // node record deletion.
  ProcessNodeAfterRecordDeletion(parent, nullptr);

  return true;
}

bool BplusTree::ProcessNodeAfterRecordDeletion(
         RecordPage* page,
         std::vector<Schema::DataRecordRidMutation>* rid_mutations) {
  if (!page) {
    LogERROR("nullptr page input to ProcessNodeAfterRecordDeletion");
    return false;
  }

  // Check space occupation < 0.5
  if (page->Occupation() >= 0.5) {
    return true;
  }

  // If current page becomes empty, delete this page from B+ tree and call
  // DeleteNodeFromTree() recursively.
  if (page->Meta()->num_records() == 0) {
    // Pass -1 to recursive calls so that page's parent will be looked up.
    debug(0);
    return DeleteNodeFromTree(page, -1);
  }

  // When page is root, and if it has only one record left after deleting this
  // child page, the root should be deleted. Instead, the remaining child
  // becomes new root.
  if (page->id() == header_->root_page()) {
    if (page->Meta()->num_records() > 1) {
      return true;
      debug(1);
    }
    if (header_->depth() <= 1) {
      debug(10);
      return true;
    }

    debug(2);
    printf("root remained %d records\n", page->Meta()->num_records());
    header_->decrement_depth(1);
    int page_id = -1;
    Schema::PageRecordsManager prmanager(page, schema(), key_indexes_,
                                         file_type_, page->Meta()->page_type());
    for (int i = 0 ; i < (int)prmanager.NumRecords(); i++) {
      if (prmanager.RecordSlotID(i) < 0) {
        continue;
      }
      page_id = (prmanager.GetRecord<Schema::TreeNodeRecord>(i))->page_id();
      printf("page_id = %d\n", page_id);
    }
    CheckLogFATAL(page_id > 0,  // new root id must > 0 (page 0 is meta page).
                  "Failed to find new root, current root is %d", page->id());
    header_->set_root_page(page_id);
    CheckLogFATAL(RecyclePage(page->id()),
                  "Failed to recycle root %d", page->id());
    return true;
  }

  // Parse current page's parent and tries to find its siblings.
  // We prefer re-distribute records rather than merging, since merging might
  // propagate into upper nodes, and make nodes more occupied.
  debug(3);
  SearchTreeNodeResult result = LookUpTreeNodeInfoForPage(page);
  CheckLogFATAL(result.slot >= 0, "Invalid slot id of page %d in parent");
  if (result.next_child_id >= 0) {
    if (ReDistributeRecordsWithinTwoPages(page, Page(result.next_child_id),
                                          result.next_slot, rid_mutations)) {
      debug(4);
      return true;
    }
  }
  if (result.prev_child_id >= 0) {
    if (ReDistributeRecordsWithinTwoPages(Page(result.prev_child_id), page,
                                          result.slot, rid_mutations)) {
      debug(5);
      return true;
    }
  }
  if (result.next_child_id >= 0) {
    if (MergeTwoNodes(page, Page(result.next_child_id),
                      result.next_slot, rid_mutations)) {
      debug(6);
      return true;
    }
  }
  if (result.prev_child_id >= 0) {
    if (MergeTwoNodes(Page(result.prev_child_id), page,
                      result.slot, rid_mutations)) {
      debug(7);
      return true;
    }
  }

  return true;
}

bool BplusTree::Do_DeleteRecordByKey(
         const std::vector<std::shared_ptr<Schema::RecordBase>>& keys,
         DataBase::DeleteResult* result) {
  // TODO: only implement single key deletion.
  auto leave = SearchByKey(keys[0].get());
  if (!leave) {
    LogERROR("Can't search to leave by this key:");
    keys[0]->Print();
    return false;
  }

  // Delete records.
  int num = DeleteMatchedRecordsFromLeave(leave, keys[0].get(),
                                          &result->rid_deleted);
  //printf("deleted %d matching records\n", num);
  (void)num;

  // Post process for the node after record deletion.
  ProcessNodeAfterRecordDeletion(leave, &result->rid_mutations);

  return true;
}

int BplusTree::DeleteMatchedRecordsFromLeave(
         RecordPage* leave,
         const Schema::RecordBase* key,
         std::vector<Schema::DataRecordRidMutation>* rid_deleted) {
  if (!leave || !key || !rid_deleted) {
    LogERROR("Nullptr input to DeleteMatchedRecordsFromLeave");
    return -1;
  }

  while (leave) {
    Schema::PageRecordsManager prmanager(leave, schema(), key_indexes_,
                                         file_type_, TREE_LEAVE);
    // Fetch all matching records in this leave.
    bool last_is_match = false;
    for (int index = 0; index < prmanager.NumRecords(); index++) {
      if (prmanager.CompareRecordWithKey(key, prmanager.Record(index)) == 0) {
        int slot_id = prmanager.RecordSlotID(index);
        CheckLogFATAL(leave->DeleteRecord(slot_id),
                      "Failed to delete record from leave %d", leave->id());
        rid_deleted->emplace_back(prmanager.plrecords().at(index).Record(),
                                  Schema::RecordID(leave->id(), slot_id),
                                  Schema::RecordID());
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

    auto processed_leave = leave;
    // If index reaches the end of all records, check overflow page.
    if (last_is_match && leave->Meta()->overflow_page() >= 0) {
      leave = Page(leave->Meta()->overflow_page());
    }
    else {
      leave = nullptr;
    }
    // Delete overflow page if it becomes empty.
    if (processed_leave->Meta()->is_overflow_page() &&
        processed_leave->Meta()->num_records() <= 0) {
      DeleteOverflowLeave(processed_leave);
    }
  }

  return rid_deleted->size();
}

bool BplusTree::DeleteOverflowLeave(RecordPage* leave) {
  if (!leave) {
    return false;
  }

  auto prev_leave = Page(leave->Meta()->prev_page());
  auto next_leave = Page(leave->Meta()->next_page());

  if (next_leave->Meta()->is_overflow_page()) {
    prev_leave->Meta()->set_overflow_page(next_leave->id());
  }
  ConnectLeaves(prev_leave, next_leave);
  RecyclePage(leave->id());
  return true;
}

Schema::RecordID BplusTree::InsertAfterOverflowLeave(
         RecordPage* leave,
         SearchTreeNodeResult* search_result,
         const Schema::RecordBase* record,
         std::vector<Schema::DataRecordRidMutation>& rid_mutations) {
  // Try inserting to this overflow page. If the new record happens to match
  // this overflow page we're done in this special case.
  Schema::RecordID rid = InsertNewRecordToOverFlowChain(leave, record);
  if (rid.IsValid()) {
    return rid;
  }
  // Special case 2 - It is the most left leave, which has different tree
  // node key with the records in this leave. This new record is possibly
  // 'less' than records of the overflow page, which means the new leave
  // should reside left to current leave.
  if (leave->Meta()->prev_page() < 0) {
    Schema::PageRecordsManager prmanager(leave, schema(),
                                         key_indexes_,
                                         file_type_, TREE_LEAVE);
    if (prmanager.CompareRecords(record, prmanager.Record(0)) < 0) {
      auto parent = Page(leave->Meta()->parent_page());
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
      ProduceKeyRecordFromNodeRecord(prmanager.Record(0), &tn_record);
      tn_record.set_page_id(leave->id());
      if (!InsertTreeNodeRecord(&tn_record, parent)) {
        LogERROR("Inserting new leave to B+ tree failed");
        return Schema::RecordID();
      }
      ConnectLeaves(new_leave, leave);
      return Schema::RecordID(new_leave->id(), 0);
    }      
  }

  // Otherwise we check next leave. If next leave has the same parent, and is
  // not overflow page, we can do some redistribution.
  if (search_result->next_child_id >= 0 &&
      Page(search_result->next_child_id)->Meta()->overflow_page() < 0) {
    // First try inserting the new record to next leave.
    rid = InsertNewRecordToNextLeave(leave, search_result, record);
    if (rid.IsValid()) {
      return rid;
    }

    // Otherwise we need to create a new leave with the new record to insert,
    // and try re-distributing records with the next_leave if possible.
    Schema::TreeNodeRecord tn_record;
    RecordPage* new_leave = CreateNewLeaveWithRecord(record, &tn_record);
    tn_record.set_page_id(new_leave->id());
    rid = Schema::RecordID(new_leave->id(), 0);

    // Re-distribute records from next leave to balance.
    auto next_page = Page(search_result->next_child_id);
    CheckLogFATAL(next_page, "Failed to fetch next child leave");
    ReDistributeRecordsWithinTwoPages(new_leave, next_page,
                                      search_result->next_slot, &rid_mutations,
                                      true);

    // Insert tree node record of the new leave to parent.
    auto parent = Page(leave->Meta()->parent_page());
    if (!InsertTreeNodeRecord(&tn_record, parent)) {
      LogERROR("Inserting new leave to B+ tree failed");
      return Schema::RecordID();;
    }
    RecordPage* page2 = Page(search_result->next_leave_id);
    RecordPage* page1 = Page(page2->Meta()->prev_page());
    ConnectLeaves(new_leave, page2);
    ConnectLeaves(page1, new_leave);
  }
  else {
    // Next leave has different parent, or is overflowed too. We have no
    // choice but creating new leave and just inserting it to parent node.
    Schema::TreeNodeRecord tn_record;
    RecordPage* new_leave = CreateNewLeaveWithRecord(record, &tn_record);
    tn_record.set_page_id(new_leave->id());
    rid = Schema::RecordID(new_leave->id(), 0);
    
    // Insert tree node record of the new leave to parent.
    auto parent = Page(leave->Meta()->parent_page());
    if (!InsertTreeNodeRecord(&tn_record, parent)) {
      LogERROR("Inserting new leave to B+ tree failed");
      return Schema::RecordID();
    }
    RecordPage* page1 = leave;
    if (search_result->next_leave_id >= 0) {
      RecordPage* page2 = Page(search_result->next_leave_id);
      page1 = Page(page2->Meta()->prev_page());
      ConnectLeaves(new_leave, page2);
    }
    else {
      page1 = GotoOverflowChainEnd(page1);
    }
    ConnectLeaves(page1, new_leave);
  }
  return rid;
}

Schema::RecordID BplusTree::ReDistributeToNextLeave(
         RecordPage* leave,
         SearchTreeNodeResult* search_result,
         const Schema::RecordBase* record,
         std::vector<Schema::DataRecordRidMutation>& rid_mutations) {
  // Check next child is valid.
  if (search_result->next_child_id < 0 ||
      Page(search_result->next_child_id)->Meta()->overflow_page() >= 0) {
    return Schema::RecordID();
  }

  auto next_leave = Page(search_result->next_child_id);
  CheckLogFATAL(next_leave, "Failed to fetch next child leave");
  // printf("(%d, %d)\n", leave->Meta()->space_used(),
  //        next_leave->Meta()->space_used());
  // If next leave is even more 'full' than current leave, can't redistribute.
  if (leave->Meta()->space_used() <= next_leave->Meta()->space_used()) {
    return Schema::RecordID();
  }

  Schema::PageRecordsManager prmanager(leave, schema(), key_indexes_,
                                       file_type_, TREE_LEAVE);
  // Insert new record, sort and group records.
  if (!prmanager.InsertNewRecord(record)) {
    LogFATAL("Can't insert new record to PageRecordsManager");
  }
  std::vector<Schema::RecordGroup> rgroups;
  prmanager.GroupRecords(&rgroups);

  // Find new which record group new record belongs to.
  int new_record_gindex = 0;
  bool found_group = false;
  // printf("rgroup size = %d\n", (int)rgroups.size());
  for (; new_record_gindex < (int)rgroups.size(); new_record_gindex++) {
    for (int index = rgroups[new_record_gindex].start_index;
         index < rgroups[new_record_gindex].start_index +
                 rgroups[new_record_gindex].num_records;
         index++) {
      if (prmanager.RecordSlotID(index) < 0) {
        found_group = true;
        break;
      }
    }
    if (found_group) {
      break;
    }
  }

  int records_to_move = 0;
  int size_to_move = 0;
  bool can_insert_new_record = false;
  int gindex = rgroups.size() - 1;
  // printf("size to insert = %d\n", record->size());
  // printf("new_record_gindex = %d\n", new_record_gindex);
  // printf("gindex = %d\n", gindex);
  for (; gindex >= new_record_gindex; gindex--) {
    records_to_move += rgroups[gindex].num_records;
    size_to_move += rgroups[gindex].size;
    // printf("records_to_move = %d\n", records_to_move);
    // printf("size_to_move = %d\n", size_to_move);
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
    return Schema::RecordID();
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

  Schema::RecordID rid;
  // Move from next_leave to leave.
  for (int index = rgroups[gindex].start_index;
       index < (int)prmanager.NumRecords();
       index++) {
    int new_id = prmanager.Record(index)->InsertToRecordPage(next_leave);
    CheckLogFATAL(new_id >= 0, "Failed to insert record to next page");

    int slot_id = prmanager.RecordSlotID(index);
    if (slot_id >= 0) {
      CheckLogFATAL(leave->DeleteRecord(slot_id),
                    "Failed to remove record from current leave");
      if (file_type_ == INDEX_DATA) {
        rid_mutations.emplace_back(prmanager.Shared_Record(index),
                                   Schema::RecordID(leave->id(), slot_id),
                                   Schema::RecordID(next_leave->id(), new_id));
      }
    }
    else {
      rid = Schema::RecordID(next_leave->id(), new_id);
    }
  }

  // If new record is not in next leave, it needs to be inserted to current
  // leave.
  if (gindex > new_record_gindex) {
    int slot_id = record->InsertToRecordPage(leave);
    if (slot_id < 0) {
      LogFATAL("Failed to insert new record to current leave");
    }
    rid = Schema::RecordID(leave->id(), slot_id);
  }

  // Delete original tree node record of next leave from parent.
  auto parent = Page(next_leave->Meta()->parent_page());
  CheckLogFATAL(parent, "Failed to fetch parent of next leave");
  CheckLogFATAL(parent->DeleteRecord(search_result->next_slot),
                "Failed to delete next leave's original tree node record");

  // Insert new tree node record of next leave to parent.
  Schema::TreeNodeRecord tn_record;
  auto new_min_record = prmanager.Record(rgroups[gindex].start_index);
  ProduceKeyRecordFromNodeRecord(new_min_record, &tn_record);
  tn_record.set_page_id(next_leave->id());
  CheckLogFATAL(InsertTreeNodeRecord(&tn_record, parent),
                "Failed to insert new tree node record for next leave");

  // printf("(%d, %d)\n", leave->Meta()->space_used(),
  //        next_leave->Meta()->space_used());

  return rid;
}

Schema::RecordID BplusTree::Do_InsertRecord(
         const Schema::RecordBase* record,
         std::vector<Schema::DataRecordRidMutation>& rid_mutations) {
  if (!record) {
    LogERROR("record to insert is nullptr");
    return Schema::RecordID();
  }

  // Verify this data record fields matches schema.
  if (!CheckRecordFieldsType(record)) {
    LogERROR("Record fields type mismatch table schema");
    return Schema::RecordID();
  }

  // If tree is empty, add first leave to it.
  if (header_->root_page() < 0) {
    auto leave = CreateNewLeaveWithRecord(record);
    if (!AddFirstLeaveToTree(leave)) {
      return Schema::RecordID();
    }
    return Schema::RecordID(leave->id(), 0);
  }

  // B+ tree is not empty. Search key to find the leave to insert new record.
  // Produce the key of the record to search.
  Schema::RecordBase search_key;
  ProduceKeyRecordFromNodeRecord(record, &search_key);

  // Search the leave to insert.
  RecordPage* crt_page = Page(header_->root_page());
  SearchTreeNodeResult search_result;
  while (crt_page && crt_page->Meta()->page_type() == TREE_NODE) {
    search_result = SearchInTreeNode(crt_page, &search_key);
    crt_page = Page(search_result.child_id);
  }
  CheckLogFATAL(crt_page, "Failed to search for key");
  CheckLogFATAL(crt_page->Meta()->page_type() == TREE_LEAVE,
                "Key search ending at non-leave node");

  // If current leave is overflowed, we check if the new record is same with
  // existing ones in it. If yes, we can insert this record to overflow pages.
  // If not, we have go to next leave.
  if (crt_page->Meta()->overflow_page() >= 0) {
    return InsertAfterOverflowLeave(crt_page, &search_result, record,
                                    rid_mutations);
  }

  // Try inserting the new record to leave.
  int slot_id = record->InsertToRecordPage(crt_page);
  if (slot_id >= 0) {
    return Schema::RecordID(crt_page->id(), slot_id);
  }

  // Try to re-organize records with next leave, it applicable (next leave
  // must exists, not an overflow leave, and has the same parent).
  Schema::RecordID rid = ReDistributeToNextLeave(crt_page, &search_result,
                                                 record, rid_mutations);
  if (rid.IsValid()) {
    return rid;
  }

  // We have to insert and split current leave.
  int next_page_id = search_result.next_leave_id;
  rid = InsertNewRecordToLeaveWithSplit(crt_page, next_page_id, record,
                                        rid_mutations);
  if (rid.IsValid()) {
    return rid;
  }

  LogERROR("Failed to insert new record to B+ tree");
  return Schema::RecordID();
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
      RecordPage* next_of_page = Page(next_of_id);
      CheckLogFATAL(next_of_page, "Get nullptr overflow page");
      crt_leave = next_of_page;
    }
    else {
      break;
    }
  }
  return crt_leave;
}

Schema::RecordID BplusTree::InsertNewRecordToOverFlowChain(
         RecordPage* leave, const Schema::RecordBase* record) {
  if (!leave || !record) {
    LogERROR("Nullptr input to CreateNewLeaveWithRecord");
    return Schema::RecordID();
  }

  // First verify that the new record is same with existing records.
  Schema::PageRecordsManager prmanager(leave, schema(), key_indexes_,
                                       file_type_, TREE_LEAVE);
  if (prmanager.CompareRecords(record, prmanager.Record(0)) != 0) {
    return Schema::RecordID();
  }

  // record->Print();
  // prmanager.Record(0)->Print();
  // prmanager.Record(prmanager.NumRecords() - 1)->Print();

  RecordPage* crt_leave = GotoOverflowChainEnd(leave);
  // Either insert record to last overflow leave, or append new overflow leave
  // if last overflow leave is full.
  int slot_id = record->InsertToRecordPage(crt_leave);
  if (slot_id < 0) {
    // Append new overflow page.
    auto next_leave = Page(crt_leave->Meta()->next_page());
    auto new_of_leave = AppendOverflowPageTo(crt_leave);
    crt_leave = new_of_leave;
    slot_id = record->InsertToRecordPage(crt_leave);
    CheckLogFATAL(slot_id >= 0,"Failed to insert record to new overflow leave");
    if (next_leave) {
      ConnectLeaves(new_of_leave, next_leave);
    }
  }
  return Schema::RecordID(crt_leave->id(), slot_id);
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
    ProduceKeyRecordFromNodeRecord(record, tn_record);
  }
  return new_leave;
}

Schema::RecordID BplusTree::InsertNewRecordToLeaveWithSplit(
         RecordPage* leave, int next_leave_id,
         const Schema::RecordBase* record,
         std::vector<Schema::DataRecordRidMutation>& rid_mutations) {
  //printf("Splitting leave %d\n", leave->id());
  if (!leave || !record) {
    LogERROR("Nullptr input to InsertNewRecordToLeaveWithSplit");
    return Schema::RecordID();
  }

  // Need to split the page.
  Schema::PageRecordsManager prmanager(leave, schema(), key_indexes_,
                                       file_type_, TREE_LEAVE);
  prmanager.set_tree(this);
  auto leaves = prmanager.InsertRecordAndSplitPage(record, rid_mutations);

  // Insert back split leave pages to parent tree node(s).
  RecordPage* parent = Page(leave->Meta()->parent_page());
  CheckLogFATAL(parent, "Failed to fetch parent page of current leave");
  if (leaves.size() > 1) {
    //printf("%d split leaves returned\n", (int)leaves.size());
    for (int i = 1; i < (int)leaves.size(); i++) {
      Schema::TreeNodeRecord tn_record;
      ProduceKeyRecordFromNodeRecord(leaves[i].record.get(), &tn_record);
      tn_record.set_page_id(leaves[i].page->id());

      if (!InsertTreeNodeRecord(&tn_record, parent)) {
        LogFATAL("Failed to add split leave %d into parent");
      }
      // Reset parent because upper node may have been split after last leave
      // was inserted. We always add next leave to the same parent as of its
      // prev leave.
      parent = Page(leaves[i].page->Meta()->parent_page());
      CheckLogFATAL(parent, "Failed to fetch parent page of current leave");
    }
    // Connect last split leave with following leaves.
    if (next_leave_id >= 0) {
      ConnectLeaves(leaves[leaves.size()-1].page, Page(next_leave_id));
    }
  }
  else {
    //printf("overflow self\n");
    // Only one leave - original leave with a overflow page.
    auto of_leave = Page(leaves[0].page->Meta()->overflow_page());
    CheckLogFATAL(of_leave, "Failed to fetch overflow page of non-split leave");
    if (next_leave_id >= 0) {
      ConnectLeaves(of_leave, Page(next_leave_id));
    }
  }
  return leaves[0].rid;
}


}  // namespace DataBaseFiles

