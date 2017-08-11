#include <sys/stat.h>
#include <string.h>

#include "Base/Log.h"
#include "Base/Utils.h"

#include "Database/Table.h"
#include "Storage/PageRecordsManager.h"
#include "Storage/BplusTree.h"

namespace Storage {

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

BplusTree::BplusTree(DB::Table* table,
                     FileType file_type,
                     std::vector<uint32> key_indexes,
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

  new_page_id_list = Utils::RandomListFromRange(2,1000);
  new_page_id_list.insert(new_page_id_list.begin(), 1);
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
  LogERROR("Can't load B+ tree %s", filename.c_str());
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

const DB::TableInfo& BplusTree::schema() const {
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
  //int new_page_id = new_page_id_list[next_index++];
  //header_->increment_num_pages(1);
  
  int new_page_id = 0;
  RecordPage* page = nullptr;
  // Look up free page list first.
  if (header_->num_free_pages() > 0) {
    new_page_id = header_->free_page();
    header_->decrement_num_free_pages(1);
    page = Page(new_page_id);
    header_->set_free_page(page->meta().next_page());  
  }
  else {
    // Append new page at the end of file.
    new_page_id = header_->num_pages();
    header_->increment_num_pages(1);
    page = new RecordPage(new_page_id, file_);
  }

  page->InitInMemoryPage();

  page->Meta()->set_page_type(page_type);
  if (page_map_.find(page->id()) == page_map_.end()) {
    page_map_[page->id()] = std::shared_ptr<RecordPage>(page);
  }
  header_->increment_num_used_pages(1);
  if (page_type == TREE_LEAVE) {
    header_->increment_num_leaves(1);
    //printf("Allocated new leave %d\n", page->id());
  }
  return page;
}

bool BplusTree::RecyclePage(int page_id) {
  // Fetch page in case it's been swapped to disk. We need to write one field
  // in the page - next_page, to link to the free page list.
  //printf("Recycling page %d\n", page_id);
  auto page = Page(page_id);
  auto type = page->meta().page_type();
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

bool BplusTree::Empty() const {
  return header_->num_pages() <= 1;
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

byte* BplusTree::Record(RecordID rid) {
  auto page = Page(rid.page_id());
  if (!page) {
    return nullptr;
  }
  return page->Record(rid.slot_id());
}

bool BplusTree::VerifyRecord(const RecordID& rid,
                             const RecordBase& record) {
  auto page = Page(rid.page_id());
  if (!page) {
    return false;
  }

  if (page->RecordLength(rid.slot_id()) != (int)record.size()) {
    return false;
  }

  byte buf[record.size()];
  record.DumpToMem(buf);
  if (strncmp((const char*)page->Record(rid.slot_id()),
              (const char*)buf,
              record.size()) != 0) {
    return false;
  }
  //record->Print();

  return true;
}

void BplusTree::PrintNodeRecords(RecordPage* page) {
  printf("Printing records on page %d\n", page->id());
  printf("page(%d):\n", page->id());
  PageRecordsManager prmanager(page, schema(), key_indexes_,
                               file_type_, page->meta().page_type());
  for (uint32 i = 0; i < prmanager.NumRecords(); i++) {
    prmanager.record(i).Print();
  }

  if (page->meta().page_type() == TREE_LEAVE) {
    int of_num = 1;
    auto of_page = Page(page->meta().overflow_page());
    while (of_page) {
      CheckLogFATAL(of_page->meta().page_type() == TREE_LEAVE,
                    "invalid overflow leave");
      printf("overflow %d, page(%d)\n", of_num++, of_page->id());
      PageRecordsManager of_prmanager(of_page, schema(), key_indexes_,
                                      file_type_, TREE_LEAVE);
      for (uint32 i = 0; i < of_prmanager.NumRecords(); i++) {
        of_prmanager.record(i).Print();
      }
      of_page = Page(of_page->meta().overflow_page());
    }
  }
  printf("Print page %d done.\n", page->id());
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

bool BplusTree::InsertTreeNodeRecord(const Storage::TreeNodeRecord& tn_record,
                                     RecordPage* tn_page) {
  //printf("Inserting to tree node to node %d\n", tn_page->id());
  if (!tn_page) {
    LogERROR("nullptr passed to InsertTreeNodeRecord");
    return false;
  }

  if (tn_page->meta().page_type() != TREE_NODE) {
    LogERROR("Target RecordPage is not tree node");
    return false;
  }
  
  // printf("Inserting new TreeNodeRecord:\n");
  if (tn_record.InsertToRecordPage(tn_page) >= 0) {
    // Success, and we're done. Get the child page related with this new
    // TreeNode record and set its parent page id as this tree node.
    RecordPage* child_page = Page(tn_record.page_id());
    child_page->Meta()->set_parent_page(tn_page->id());
    return true;
  }

  // The parent is full and needs to be split.
  PageRecordsManager prmanager(tn_page, schema(), key_indexes_,
                               file_type_, tn_page->meta().page_type());

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
  for (uint32 i = mid_index; i < plrecords.size(); i++) {
    if (prmanager.record(i).InsertToRecordPage(new_tree_node) < 0) {
      LogERROR("Move slot %d TreeNodeRecord to new tree node failed");
      return false;
    }
    // Reset new parent for right half children nodes.
    auto tn_record = prmanager.GetRecord<Storage::TreeNodeRecord>(i);
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
      if (tn_record.InsertToRecordPage(tn_page) < 0) {
        LogERROR("Insert to TreeNodeRecord to left-half tree node failed");
        return false;
      }
      RecordPage* child_page = Page(tn_record.page_id());
      if (child_page) {
        child_page->Meta()->set_parent_page(tn_page->id());
      }
    }
    else {
      // After split, left half child nodes will not be modified later, and we
      // can check them out to disk.
      // int child_page_id =
      //     reinterpret_cast<Storage::TreeNodeRecord*>(prmanager.Record(i))
      //         ->page_id();
      // CheckoutPage(child_page_id);
    }
  }

  // Add the middle TreeNodeRecord to parent tree node.
  if (tn_page->meta().parent_page() < 0 &&
      tn_page->id() == header_->root_page()) {
    // Current tree node is root, so create new root and we need to add both
    // tree nodes to the root.
    RecordPage* new_root = AllocateNewPage(TREE_NODE);
    //LogINFO("Creating new root %d", new_root->id());
    header_->set_root_page(new_root->id());
    header_->increment_depth(1);

    auto left_upper_tn_record = prmanager.GetRecord<Storage::TreeNodeRecord>(0);
    left_upper_tn_record->set_page_id(tn_page->id());
    if (!InsertTreeNodeRecord(*left_upper_tn_record, new_root)) {
      LogERROR("Add left half child to new root node failed");
      return false;
    }
  }

  // Add new tree node to parent by inserting a TreeNodeRecord with key of
  // the mid_index record, and page id of the new tree node.
  auto right_upper_tn_record =
      prmanager.GetRecord<Storage::TreeNodeRecord>(mid_index);
  right_upper_tn_record->set_page_id(new_tree_node->id());
  RecordPage* parent_node = Page(tn_page->meta().parent_page());
  if (!InsertTreeNodeRecord(*right_upper_tn_record, parent_node)) {
    LogERROR("Add new tree node to parent node failed");
    return false;
  }

  return true;
}

bool BplusTree::AddFirstLeaveToTree(RecordPage* leave) {
  Storage::TreeNodeRecord min_tn_record;
  min_tn_record.InitRecordFields(schema(), key_indexes_);
  RecordPage* tree_node = AllocateNewPage(TREE_NODE);
  header_->set_root_page(tree_node->id());
  header_->set_depth(1);
  min_tn_record.reset();
  min_tn_record.set_page_id(leave->id());
  if (!InsertTreeNodeRecord(min_tn_record, tree_node)) {
    LogERROR("Insert leave to tree node failed");
    return false;
  }
  return true;
}

// Only used in bulk loading.
bool BplusTree::AddLeaveToTree(RecordPage* leave,
                               Storage::TreeNodeRecord* tn_record) {
  if (!leave || !tn_record) {
    LogERROR("nullptr input to AddLeaveToTree");
    return false;
  }

  // Create a new TreeNodeRecord to insert to node.
  tn_record->set_page_id(leave->id());

  // Get the tree node to insert.
  Storage::TreeNodeRecord min_tn_record;
  RecordPage* tree_node = nullptr;
  if (bl_status_.prev_leave) {
    tree_node = Page(bl_status_.prev_leave->meta().parent_page());
    if (!tree_node) {
      LogFATAL("Can't find parent of prev leave %d",
               bl_status_.prev_leave->id());
    }
    //tn_record->Print();
    if (!InsertTreeNodeRecord(*tn_record, tree_node)) {
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
      set_parent_page(bl_status_.prev_leave->meta().parent_page());
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
  new_leave->Meta()->set_parent_page(page->meta().parent_page());
  new_leave->Meta()->set_is_overflow_page(1);
  page->Meta()->set_overflow_page(new_leave->id());
  ConnectLeaves(page, new_leave);
  return new_leave;
}

bool BplusTree::ConnectLeaves(RecordPage* page1, RecordPage* page2) {
  int page1_id = page1 ? page1->id() : -1;
  int page2_id = page2 ? page2->id() : -1;
  //printf("Connecting %d and %d\n", page1_id, page2_id);
  if (page1) {
    page1->Meta()->set_next_page(page2_id);
  }
  if (page2) {
    page2->Meta()->set_prev_page(page1_id);
  }
  return true;
}

RecordPage* BplusTree::FirstLeave() {
  if (Empty()) {
    return nullptr;
  }

  RecordPage* crt_page = Page(header_->root_page());
  CheckLogFATAL(crt_page, "Failed to fetch tree root");
  while (crt_page->meta().page_type() == TREE_NODE) {
    PageRecordsManager prmanager(crt_page, schema(), key_indexes_,
                                 file_type_, TREE_NODE);
    int first_child =prmanager.GetRecord<Storage::TreeNodeRecord>(0)->page_id();
    CheckLogFATAL(first_child,
                  "Failed to get first child of tree node %d", crt_page->id());
    crt_page = Page(first_child);
  }

  return crt_page;
}

std::vector<uint32> BplusTree::IndexesToCompareLeaveRecords() const {
  std::vector<uint32> indexes;
  if (file_type_ == INDEX_DATA) {
    indexes = key_indexes_;
  }
  else {
    for (uint32 i = 0; i < key_indexes_.size(); i++) {
      indexes.push_back(i);
    }
  }
  return indexes;
}

bool BplusTree::BlukLoadInsertRecordToLeave(const RecordBase& record,
                                            RecordPage* leave) {
  int slot_id = record.InsertToRecordPage(leave);
  if (slot_id >= 0) {
    bl_status_.last_record.reset(record.Duplicate());
    bl_status_.rid.set_page_id(leave->id());
    bl_status_.rid.set_slot_id(slot_id);
    return true;
  }
  return false;
}

// BulkLoading records.
bool BplusTree::BulkLoad(
         std::vector<std::shared_ptr<RecordBase>>& records) {
  CheckLogFATAL(CreateBplusTreeFile(),
                "Failed to create B+ tree file for BulkLoad");
  for (uint32 i = 0; i < records.size(); i++) {
    if (!BulkLoadRecord(*records.at(i))) { 
      LogERROR("Load record %d failed, stop loading ...", i);
      return false;
    }
  }
  return true;
}

bool BplusTree::BulkLoadRecord(const RecordBase& record) {
  bl_status_.rid.reset();

  // Try inserting the record to current leave page. If success, we continue.
  // Otherwise it is possible that current leave is nullptr (empty B+ tree,
  // no record ever inserted), or current leave is full. In either case, we
  // need to allocate a new leave page and continue inserting.
  if (bl_status_.crt_leave &&
      bl_status_.crt_leave->meta().is_overflow_page() == 0 &&
      BlukLoadInsertRecordToLeave(record, bl_status_.crt_leave)) {
    return true;
  }

  // Check boundary duplication. If new record equals last record at the end of
  // its leave, we need to move these duplicates to new leave.
  if (bl_status_.crt_leave && bl_status_.last_record &&
      RecordBase::CompareRecordsBasedOnIndex(
          record, *bl_status_.last_record.get(),
          IndexesToCompareLeaveRecords()) == 0) {
    bl_status_.last_record.reset(record.Duplicate());
    return CheckBoundaryDuplication(record);
  }

  // Normal insertion. Allocate a new leave node and insert the record.
  RecordPage* new_leave = AppendNewLeave();
  if (!BlukLoadInsertRecordToLeave(record, new_leave)) {
    LogFATAL("Failed to insert record to new leave node");
  }

  // We add this new leave node with one record, to a upper tree node. Note that
  // we only do this when having allocated a new leave node and had just first
  // record inserted to it.
  Storage::TreeNodeRecord tn_record;
  ProduceKeyRecordFromNodeRecord(record, &tn_record);
  if (!AddLeaveToTree(new_leave, &tn_record)) {
    LogFATAL("Failed to add leave to B+ tree node");
  }

  return true;
}

bool BplusTree::CheckBoundaryDuplication(const RecordBase& record) {
  // Try inserting first. This deals with a non-full overflow page.
  if (BlukLoadInsertRecordToLeave(record, bl_status_.crt_leave)) {
    return true;
  }

  // Load curret leave records and find the first duplicate with the new record
  // to be insert.
  PageRecordsManager prmanager(bl_status_.crt_leave, schema(), key_indexes_,
                               file_type_, TREE_LEAVE);

  int index = prmanager.NumRecords() - 1;
  for (; index >= 0; index--) {
    if (RecordBase::CompareRecordsBasedOnIndex(
            record, prmanager.record(index),
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
      if (prmanager.record(i).InsertToRecordPage(new_leave) < 0) {
        LogFATAL("Faield to move slot %d TreeNodeRecord to new tree node");
      }
      // Remove the records from the original leave.
      bl_status_.prev_leave->DeleteRecord(plrecords.at(i).slot_id());
    }

    // Add the new leave to parent node.
    const RecordBase& mid_record = prmanager.record(index);
    Storage::TreeNodeRecord tn_record;
    ProduceKeyRecordFromNodeRecord(mid_record, &tn_record);
    if (!AddLeaveToTree(new_leave, &tn_record)) {
      LogFATAL("Failed to add leave to B+ tree");
    }
    // Re-try inserting record to new crt_leave.
    if (BlukLoadInsertRecordToLeave(record, new_leave)) {
      return true;
    }
  }

  //LogINFO("New duplicated record needs a new overflow page.");
  RecordPage* overflow_leave = AppendNewOverflowLeave();
  if (!BlukLoadInsertRecordToLeave(record, overflow_leave)) {
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
  vc_status_ = ValidityCheckStatus();
  vc_status_.count_num_pages = 2;  // header page + root
  vc_status_.count_num_used_pages = 2;
  while (!page_q.empty()) {
    RecordPage* crt_page = page_q.front();
    page_q.pop();

    // Verify this page.
    if (crt_page->meta().page_type() == TREE_NODE) {
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
    free_page = Page(free_page->meta().next_page());
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
  if (page->meta().page_type() != TREE_NODE) {
    LogERROR("Wrong node type - not tree node");
    return false;
  }

  PageRecordsManager prmanager(page, schema(), key_indexes_,
                               file_type_, TREE_NODE);
  
  for (uint32 i = 0; i < prmanager.NumRecords(); i++) {
    auto tn_record = prmanager.GetRecord<Storage::TreeNodeRecord>(i);
    int child_page_id = tn_record->page_id();

    Storage::TreeNodeRecord* next_record = nullptr;
    if (i < prmanager.NumRecords() - 1) {
      next_record = prmanager.GetRecord<Storage::TreeNodeRecord>(i + 1);
    }

    if (!VerifyChildRecordsRange(Page(child_page_id),
                                 tn_record, next_record)) {
      LogERROR("Verify child page %d range failed, tree index is %d",
               child_page_id, key_indexes_[0]);
      return false;
    }
  }

  return true;
}

bool BplusTree::VerifyChildRecordsRange(RecordPage* child_page,
                                        const RecordBase* left_bound,
                                        const RecordBase* right_bound) {
  if (!child_page) {
    LogFATAL("Child page is nullptr, can't verify it's range");
  }

  if (left_bound && right_bound && *left_bound >= *right_bound) {
    LogERROR("left_bound should be no greater than right_bound, but:");
    left_bound->Print();
    right_bound->Print();
    return false;
  }

  PageRecordsManager prmanager(child_page, schema(), key_indexes_,
                               file_type_, child_page->meta().page_type());

  if (prmanager.NumRecords() <= 0) {
    LogINFO("No records in child page, skip VerifyChildRecordsRange");
    return true;
  }

  // Check the first record >= left bound.
  RecordBase first_record_key;
  if (file_type_== INDEX_DATA &&
      child_page->meta().page_type() == TREE_LEAVE) {
    // If child is leave of index-data file, record is DataRecord. We need to
    // extract key from it.
    auto first_data_record = prmanager.GetRecord<DataRecord>(0);
    first_data_record->ExtractKey(&first_record_key, key_indexes_);
  }
  else {
    first_record_key = *prmanager.GetRecord<RecordBase>(0);
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
  RecordBase last_record_key;
  int last_index = prmanager.NumRecords() - 1;
  if (file_type_== INDEX_DATA &&
      child_page->meta().page_type() == TREE_LEAVE) {
    // If child is leave of index-data file, record is DataRecord. We need to
    // extract key from it.
    auto last_data_record = prmanager.GetRecord<DataRecord>(last_index);
    last_data_record->ExtractKey(&last_record_key, key_indexes_);
  }
  else {
    last_record_key = *prmanager.GetRecord<RecordBase>(last_index);
  }
  if (last_record_key >= *right_bound) {  // right boundary is open interval
    LogERROR("last record key of child >= right bound, on page %d, type %d",
             child_page->id(), child_page->meta().page_type());
    prmanager.record(last_index).Print();
    right_bound->Print();
    return false;
  }

  return true;
}

bool BplusTree::EqueueChildNodes(RecordPage* page,
                                 std::queue<RecordPage*>* page_q) {
  if (page->meta().page_type() != TREE_NODE) {
    LogERROR("Wrong node type - not tree node, no child node");
    return false;
  }

  PageRecordsManager prmanager(page, schema(), key_indexes_,
                               file_type_, TREE_NODE);
  
  bool children_are_leave = false;
  vc_status_.count_num_pages += prmanager.NumRecords();
  vc_status_.count_num_used_pages += prmanager.NumRecords();
  for (uint32 i = 0; i < prmanager.NumRecords(); i++) {
    auto tn_record = prmanager.GetRecord<Storage::TreeNodeRecord>(i);
    RecordPage* child_page = Page(tn_record->page_id());
    if (!child_page) {
      LogFATAL("Can't fetching child page while enqueuing");
    }
    if (child_page->meta().page_type() == TREE_LEAVE) {
      while (child_page) {
        //printf("child_page = %d\n", child_page->id());
        // Don't enqueue tree leave.
        children_are_leave = true;
        vc_status_.count_num_leaves++;
        vc_status_.count_num_records += child_page->meta().num_records();
        // Check prev_leave <-- crt_leave.prev
        if (vc_status_.prev_leave_id != child_page->meta().prev_page()) {
          LogERROR("Leave connecting verification failed - "
                   "curent leave's prev = %d, while prev_leave_id = %d",
                   child_page->meta().prev_page(), vc_status_.prev_leave_id);
          printf("%d <-- current\n", child_page->meta().prev_page());
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
        vc_status_.prev_leave_next = child_page->meta().next_page();
        // Find overflow page.
        if (child_page->meta().overflow_page() >= 0) {
          child_page = Page(child_page->meta().overflow_page());
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
  PageRecordsManager prmanager(page, schema(), key_indexes_,
                               file_type_, TREE_LEAVE);

  const auto& record = prmanager.record(0);
  for (uint32 i = 1; i < prmanager.NumRecords(); i++) {
    if (prmanager.CompareRecords(record, prmanager.record(i)) != 0) {
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

std::shared_ptr<RecordBase> BplusTree::GetRecord(RecordID rid) {
  auto page = Page(rid.page_id());
  if (!page) {
    return std::shared_ptr<RecordBase>();
  }

  int slot_id = rid.slot_id();
  PageLoadedRecord plrecord(slot_id);
  plrecord.GenerateRecordPrototype(schema(), key_indexes_,
                                   file_type_, page->meta().page_type());
  int load_size = plrecord.mutable_record()->LoadFromMem(page->Record(slot_id));

  int expect_len = page->RecordLength(slot_id);
  SANITY_CHECK(load_size == expect_len,
               "Error load slot %d from page %d - expect %d byte, actual %d ",
               page->id(), slot_id, expect_len, load_size);
  
  return plrecord.shared_record();
}

RecordPage* BplusTree::SearchByKey(const RecordBase& key) {
  RecordPage* crt_page = Page(header_->root_page());
  while (crt_page && crt_page->meta().page_type() == TREE_NODE) {
    auto result = SearchInTreeNode(key, crt_page);
    crt_page = Page(result.child_id);
  }

  if (!crt_page) {
    LogERROR("Failed to search for key");
    key.Print();
    return nullptr;
  }

  return crt_page;
}

std::shared_ptr<TreeRecordIterator>
BplusTree::RecordIterator(const DB::SearchOp* op) {
  return std::make_shared<TreeRecordIterator>(this, op);
}

void TreeRecordIterator::Init() {
  if (search_op->key) {
    // Single key search.
    CHECK(tree->CheckKeyFieldsType(*search_op->key),
          "Key fields type mismatch table schema of this B+ tree");

    leave = tree->SearchByKey(*search_op->key);
    if (!leave) {
      LogERROR("Can't search to leave by this key");
      end = true;
      return;
    }
  } else if (search_op->left_key || search_op->right_key) {
    // Range search.
    if (search_op->left_key) {
      CHECK(tree->CheckKeyFieldsType(*search_op->left_key),
            "Left key fields type mismatch table schema of this B+ tree");
    }
    if (search_op->right_key) {
      CHECK(tree->CheckKeyFieldsType(*search_op->right_key),
            "Right key fields type mismatch table schema of this B+ tree");
    }
    if (!search_op->left_key && !search_op->right_key) {
      LogERROR("No key range to search");
      end = true;
      return;
    }

    if (search_op->left_key) {
      leave = tree->SearchByKey(*search_op->left_key);
      if (!leave) {
        LogERROR("Can't search to leave by this key");
        end = true;
        return;
      }
    } else {
      leave = tree->FirstLeave();
    }
  } else {
    leave = tree->FirstLeave();
  }

  prmanager.reset(
      new PageRecordsManager(leave, tree->schema(), tree->key_indexes(),
                             tree->file_type(), leave->meta().page_type()));
  ready = true;
}

std::shared_ptr<RecordBase> TreeRecordIterator::GetNextRecord() {
  if (!ready) {
    Init();
  }

  if (end) {
    return nullptr;
  }

  if (search_op->key) {
    // Single key search.
    while (true) {
      if (page_record_index >= prmanager->NumRecords()) {
        if (last_is_match && leave->meta().overflow_page() >= 0) {
          leave = tree->Page(leave->meta().overflow_page());
          prmanager.reset(new PageRecordsManager(leave, tree->schema(),
                                                 tree->key_indexes(),
                                                 tree->file_type(),
                                                 leave->meta().page_type()));
          page_record_index = 0;
        } else {
          end = true;
          return nullptr;
        }
      }

      while (page_record_index < prmanager->NumRecords()) {
        int re = prmanager->CompareRecordWithKey(
                    prmanager->record(page_record_index), *search_op->key);
        if (re < 0) {
          page_record_index++;
          continue;
        } else if (re == 0) {
          auto record = prmanager->shared_record(page_record_index);
          page_record_index++;
          last_is_match = true;
          return record;
        } else {
          if (leave->meta().is_overflow_page()) {
            // Overflow page must have all same records that match the key we're
            // searching for.
            LogFATAL("Overflow page stores inconsistent records!");
          }
          last_is_match = false;
          end = true;
          return nullptr;
        }
      }
    }
  } else if (search_op->left_key || search_op->right_key) {
    // Key range search.
    while (true) {
      if (page_record_index >= prmanager->NumRecords()) {
        if (leave->meta().next_page() >= 0) {
          leave = tree->Page(leave->meta().next_page());
          prmanager.reset(new PageRecordsManager(leave, tree->schema(),
                                                 tree->key_indexes(),
                                                 tree->file_type(),
                                                 leave->meta().page_type()));
          page_record_index = 0;
        } else {
          end = true;
          return nullptr;
        }
      }

      while (page_record_index < prmanager->NumRecords()) {
        // Compare with left bound.
        bool left_match = false, right_match = false;

        if (search_op->left_key) {
          int re = prmanager->CompareRecordWithKey(
                                  prmanager->record(page_record_index),
                                  *search_op->left_key);
          left_match = (re > 0 && search_op->left_open) ||
                       (re >= 0 && !search_op->left_open);
        } else {
          left_match = true;
        }

        if (!left_match) {
          page_record_index++;
          continue;
        }

        // Compare with right bound.
        if (search_op->right_key) {
          int re = prmanager->CompareRecordWithKey(
                                  prmanager->record(page_record_index),
                                  *search_op->right_key);
          right_match = (re < 0 && search_op->right_open) ||
                        (re <= 0 && !search_op->right_open);

          if (!right_match) {
            end = true;
            return nullptr;
          }
        } else {
          right_match = true;
        }

        if (left_match && right_match) {
          auto record = prmanager->shared_record(page_record_index);
          page_record_index++;
          return record;
        }
      }
    }
  } else {
    // Scan records.
    while (true) {
      if (page_record_index >= prmanager->NumRecords()) {
        if (leave->meta().next_page() >= 0) {
          leave = tree->Page(leave->meta().next_page());
          prmanager.reset(new PageRecordsManager(leave, tree->schema(),
                                                 tree->key_indexes(),
                                                 tree->file_type(),
                                                 leave->meta().page_type()));
          page_record_index = 0;
        } else {
          end = true;
          return nullptr;
        }
      }
      auto record = prmanager->shared_record(page_record_index);
      page_record_index++;
      return record;
    }
  }

  return nullptr;
}

int BplusTree::SearchRecords(
      const DB::SearchOp& op,
      std::vector<std::shared_ptr<RecordBase>>* result) {
  auto iter = RecordIterator(&op);
  std::shared_ptr<RecordBase> record;
  while (true) {
    record = iter->GetNextRecord();
    if (!record) {
      break;
    }
    result->push_back(record);
  }

  return result->size();
}

int BplusTree::ScanRecords(std::vector<std::shared_ptr<RecordBase>>* result) {
  DB::SearchOp op;
  return SearchRecords(op, result);
}

BplusTree::SearchTreeNodeResult
BplusTree::SearchInTreeNode(const RecordBase& key, RecordPage* page) {
  SearchTreeNodeResult result;
  if (page->meta().page_type() != TREE_NODE) {
    LogERROR("Can't search to next level on a non-TreeNode page");
    return result;
  }

  PageRecordsManager prmanager(page, schema(), key_indexes_,
                               file_type_, page->meta().page_type());

  uint32 index = prmanager.SearchForKey(key);
  SANITY_CHECK(index >= 0,
               "Search for key in page %d failed - key is:", page->id());

  result.slot = prmanager.RecordSlotID(index);
  result.record = prmanager.plrecords().at(index).shared_record();

  auto tn_record = prmanager.GetRecord<Storage::TreeNodeRecord>(index);
  result.child_id = tn_record->page_id();
  CheckLogFATAL(result.child_id >= 0, "Get child_id < 0 in SearchInTreeNode");

  // Get next child & leave.
  if (index < prmanager.NumRecords() - 1) {
    result.next_slot = prmanager.RecordSlotID(index + 1);
    result.next_record = prmanager.plrecords().at(index + 1).shared_record();
    tn_record = prmanager.GetRecord<Storage::TreeNodeRecord>(index + 1);
    result.next_child_id = tn_record->page_id();
    CheckLogFATAL(result.next_child_id >= 0,
                  "Get nullptr next_child_page in SearchInTreeNode");
  }
  // Get next leave id. It's not always next_child because next leave may have
  // different parent.
  auto child_page = Page(result.child_id);
  CheckLogFATAL(child_page, "child page is nullptr");
  child_page = GotoOverflowChainEnd(child_page);
  if (child_page->meta().next_page() >= 0) {
    result.next_leave_id = child_page->meta().next_page();
  }

  // Get prev child & leave.
  if (index > 0) {
    result.prev_slot = prmanager.RecordSlotID(index - 1);
    result.prev_record = prmanager.plrecords().at(index - 1).shared_record();
    tn_record = prmanager.GetRecord<Storage::TreeNodeRecord>(index - 1);
    result.prev_child_id = tn_record->page_id();
    CheckLogFATAL(result.prev_child_id >= 0,
                  "Get nullptr next_child_page in SearchInTreeNode");
  }
  child_page = Page(result.child_id);
  CheckLogFATAL(child_page, "child page is nullptr");
  if (child_page->meta().prev_page() >= 0) {
    result.prev_leave_id = child_page->meta().prev_page();
  }

  return result;
}

BplusTree::SearchTreeNodeResult
BplusTree::LookUpTreeNodeInfoForPage(const RecordPage& page) {
  SearchTreeNodeResult result;

  auto parent = Page(page.meta().parent_page());
  if (!parent) {
    LogERROR("Can't get parent for page %d", page.id());
    return result;
  }

  PageRecordsManager prmanager(parent, schema(), key_indexes_,
                               file_type_, TREE_NODE);
  bool tn_record_found = false;
  for (uint32 i = 0; i < prmanager.NumRecords(); i++) {
    auto tn_record = prmanager.GetRecord<Storage::TreeNodeRecord>(i);
    int child_page_id = tn_record->page_id();
    if (child_page_id == page.id()) {
      tn_record_found = true;
      result.slot = prmanager.RecordSlotID(i);
      result.child_id = child_page_id;
      result.record = prmanager.plrecords().at(i).shared_record();
      // prev child.
      if (i > 0) {
        result.prev_slot = prmanager.RecordSlotID(i - 1);
        result.prev_child_id =
            (prmanager.GetRecord<Storage::TreeNodeRecord>(i - 1))->page_id();
        result.prev_record = prmanager.plrecords().at(i - 1).shared_record();
      }
      // next child.
      if (i < prmanager.NumRecords() - 1) {
        result.next_slot = prmanager.RecordSlotID(i + 1);
        result.next_child_id =
            (prmanager.GetRecord<Storage::TreeNodeRecord>(i + 1))->page_id();
        result.next_record = prmanager.plrecords().at(i + 1).shared_record();
      }
    }
  }

  CheckLogFATAL(tn_record_found,
                "Can't find TreeNodeRecord for page %d, type%d, tree %d",
                page.id(), page.meta().page_type(), key_indexes_[0]);
  return result;
}

bool BplusTree::CheckKeyFieldsType(const RecordBase& key) const {
  return key.CheckFieldsType(schema(), key_indexes_);
}

bool BplusTree::CheckRecordFieldsType(const RecordBase& record) const {
  if (record.type() == DATA_RECORD) {
    if (file_type_ == INDEX) {
      return false;
    }
    if (!record.CheckFieldsType(schema())) {
      return false;
    }
    return true;
  }
  if (record.type() == INDEX_RECORD) {
    if (file_type_ == INDEX_DATA) {
      return false;
    }
    if (!record.CheckFieldsType(schema(), key_indexes_)) {
      return false;
    }
    return true;
  }

  LogERROR("Invalid ReocrdType %d to insert to B+ tree", record.type());
  return false;
}

bool BplusTree::ProduceKeyRecordFromNodeRecord(
         const RecordBase& node_record, RecordBase* tn_record) {
  if (!tn_record) {
    LogERROR("Nullptr input to ProduceKeyRecordFromNodeRecord");
    return false;
  }
  if (file_type_ == INDEX_DATA &&
      node_record.type() == DATA_RECORD) {
    (dynamic_cast<const DataRecord&>(node_record))
        .ExtractKey(tn_record, key_indexes_);
  }
  else if (file_type_ == INDEX &&
           node_record.type() == INDEX_RECORD){
    tn_record->CopyFieldsFrom(node_record);
  }
  else if (node_record.type() == TREENODE_RECORD) {
    tn_record->CopyFieldsFrom(node_record);
  }
  else {
    LogFATAL("Unsupported B+ tree type and leave record type (%d, %d)",
             file_type_, node_record.type());
  }
  return true;
}

RecordID
BplusTree::InsertNewRecordToNextLeave(const RecordBase& record,
                                      RecordPage* leave,
                                      SearchTreeNodeResult* search_result) {
  // Check next leave is valid, and has same parent.
  if (search_result->next_child_id < 0 &&
      Page(search_result->next_child_id)->meta().overflow_page() >= 0) {
    return RecordID();
  }

  auto next_leave = Page(search_result->next_child_id);
  CheckLogFATAL(next_leave, "Failed to fetch next leave");
  auto parent = Page(leave->meta().parent_page());
  CheckLogFATAL(parent, "Failed to fetch parent node");

  int slot_id = record.InsertToRecordPage(next_leave);
  if (slot_id < 0) {
    return RecordID();
  }

  // Delete next leave's tree node record from parent.
  CheckLogFATAL(parent->DeleteRecord(search_result->next_slot),
                "Failed to delete next leave's original tree node record");
  Storage::TreeNodeRecord tn_record;
  ProduceKeyRecordFromNodeRecord(record, &tn_record);
  tn_record.set_page_id(next_leave->id());
  CheckLogFATAL(InsertTreeNodeRecord(tn_record, parent),
                "Failed to insert new tree node record for next leave");

  return RecordID(next_leave->id(), slot_id);
}

bool BplusTree::ReDistributeRecordsWithinTwoPages(
         RecordPage* page1, RecordPage* page2, int page2_slot_id_in_parent,
         std::vector<DataRecordRidMutation>* rid_mutations,
         bool force_redistribute) {
  if (!page1 || !page2) {
    LogERROR("Nullptr page input to ReDistributeRecordsWithinTwoPages");
    return false;
  }

  if (page1->meta().overflow_page() > 0 ||
      page2->meta().overflow_page() > 0) {
    return false;
  }

  // Check if two pages can be merged. If mergable and not force redistribute,
  // do not redistribute. This is the case in record deletion when two pages
  // should be merged rather than redistributed.
  if (!force_redistribute &&
      page1->PreCheckCanInsert(page2->meta().num_records(),
                               page2->meta().space_used())) {
    return false;
  }

  int min_gap = page1->meta().space_used() - page2->meta().space_used();
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
  PageRecordsManager prmanager(src_page, schema(), key_indexes_,
                               file_type_, page1->meta().page_type());
  std::vector<RecordGroup> rgroups;
  prmanager.GroupRecords(&rgroups);

  // A very, very rare case that we force page2 move at least 1 record to page1
  // when a tree node has only 1 record and it still has right siblings.
  force_redistribute = (move_direction < 0) &&
                       (page1->meta().page_type() == TREE_NODE) &&
                       (page1->meta().num_records() == 1);
  if (force_redistribute) {
    LogINFO("Can't believe tihs happens!");
  }

  int gindex_start = move_direction < 0 ? 0 : (rgroups.size() - 1);
  int gindex_end = move_direction < 0 ? rgroups.size() : -1;
  int gindex = gindex_start;
  for (; gindex != gindex_end; gindex -= move_direction) {
    if (!force_redistribute &&
        std::abs(dest_page->meta().space_used() + (int)rgroups[gindex].size -
                 src_page->meta().space_used() + (int)rgroups[gindex].size)
        > min_gap) {
      break;
    }
    force_redistribute = false;
    if (!dest_page->PreCheckCanInsert(rgroups[gindex].num_records,
                                      rgroups[gindex].size)) {
      break;
    }
    // Move from src_page to current dest_page.
    for (uint index = rgroups[gindex].start_index;
         index < rgroups[gindex].start_index + rgroups[gindex].num_records;
         index++) {
      int slot_id = prmanager.RecordSlotID(index);

      // If it's tree node, update parent info for those child nodes
      // corrosponding tree node records that are redistributed.
      if (dest_page->meta().page_type() == TREE_NODE) {
        auto tn_record = prmanager.GetRecord<Storage::TreeNodeRecord>(index);
        int child_page_id = tn_record->page_id();
        RecordPage* child_page = Page(child_page_id);
        if (child_page) {
          child_page->Meta()->set_parent_page(dest_page->id());
        }        
      }
      src_page->DeleteRecord(slot_id);

      int new_slot_id = prmanager.record(index).InsertToRecordPage(dest_page);
      CheckLogFATAL(new_slot_id >= 0,
                    "Failed to move next page's record forward to new page");
      // If it's data tree leave, add rid mutations.
      if (file_type_ == INDEX_DATA &&
          dest_page->meta().page_type() == TREE_LEAVE &&
          rid_mutations) {
        rid_mutations->emplace_back(prmanager.shared_record(index),
                           RecordID(src_page->id(), slot_id),
                           RecordID(dest_page->id(), new_slot_id));
      }
    }
    // Update min_map
    min_gap = std::abs(page1->meta().space_used() -
                       page2->meta().space_used());
  }
  // No record is re-distributed.
  if (gindex == gindex_start) {
    //printf("No record moved\n");
    return true;
  }

  //printf("gindex = %d, gindex_start = %d\n", gindex, gindex_start);
  if (move_direction > 0) {
    gindex++;
  }

  // Delete original tree node record of page2 from parent.
  auto parent = Page(page2->meta().parent_page());
  CheckLogFATAL(parent, "Failed to fetch parent of next leave");
  CheckLogFATAL(parent->DeleteRecord(page2_slot_id_in_parent),
                "Failed to delete next leave's original tree node record");

  // Insert new tree node record of page2 to parent.
  Storage::TreeNodeRecord tn_record;
  const auto& new_min_record = prmanager.record(rgroups[gindex].start_index);
  ProduceKeyRecordFromNodeRecord(new_min_record, &tn_record);
  tn_record.set_page_id(page2->id());
  CheckLogFATAL(InsertTreeNodeRecord(tn_record, parent),
                "Failed to insert new tree node record for next leave");

  return true;
}

bool BplusTree::MergeTwoNodes(
         RecordPage* page1, RecordPage* page2,
         int page2_slot_id_in_parent,
         std::vector<DataRecordRidMutation>* rid_mutations) {
  if (!page1 || !page2) {
    LogERROR("nullptr page1/page2 input to MergeTwoNodes");
    return false;
  }

  //printf("merging %d and %d\n", page1->id(), page2->id());

  if (page1->meta().overflow_page() > 0 ||
      page2->meta().overflow_page() > 0) {
    return false;
  }

  // We always merge forward - page1 <-- page2.
  // Page2 is deleted so that we don't need to update tree node record of page1
  // in parent.
  if (!page1->PreCheckCanInsert(page2->meta().num_records(),
                                page2->meta().space_used())) {
    return false;
  }

  PageRecordsManager prmanager(page2, schema(), key_indexes_,
                               file_type_, page2->meta().page_type());

  for (uint32 i = 0; i < prmanager.NumRecords(); i++) {
    int slot_id = prmanager.RecordSlotID(i);
    if (page2->meta().page_type() == TREE_NODE) {
      auto tn_record = prmanager.GetRecord<Storage::TreeNodeRecord>(i);
      int child_page_id = tn_record->page_id();
      RecordPage* child_page = Page(child_page_id);
      if (child_page) {
        child_page->Meta()->set_parent_page(page1->id());
      }        
    }

    // Move to page1.
    int new_slot_id = prmanager.record(i).InsertToRecordPage(page1);
    if (new_slot_id < 0) {
      LogFATAL("Failed to move record from page2 to page1");
    }
    if (file_type_ == INDEX_DATA &&
        page2->meta().page_type() == TREE_LEAVE &&
        rid_mutations) {
      rid_mutations->emplace_back(prmanager.shared_record(i),
                         RecordID(page2->id(), slot_id),
                         RecordID(page1->id(), new_slot_id));
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

  bool need_lookup_parent = slot_id_in_parent < 0;

  // delete root page - tree will become empty and reset.
  if (page->id() == header_->root_page()) {
    header_->reset();
    page_map_.clear();
    if (ftruncate(fileno(file_), kPageSize) != 0) {
      return false;
    }
    return true;
  }

  auto parent = Page(page->meta().parent_page());
  CheckLogFATAL(parent, "can't find parent for non-root page %d", page->id());

  SearchTreeNodeResult result;
  if (slot_id_in_parent < 0) {
    result = LookUpTreeNodeInfoForPage(*page);
    slot_id_in_parent = result.slot;
  }
  CheckLogFATAL(slot_id_in_parent >= 0, "Invalid slot id of page %d in parent");

  // Parse the TreeNodeRecord in parent and check it matches this page id.
  int page_id = *(ParseRecordField<PageID>(parent, slot_id_in_parent));
  if (page_id != page->id()) {
    LogFATAL("tree node record %d inconsistent with child page id %d",
             page_id, page->id());
  }

  // TODO: remove this
  if (page->meta().page_type() == TREE_NODE) {
    result = LookUpTreeNodeInfoForPage(*page);
    if (result.prev_child_id < 0 && result.next_child_id >= 0) {
      LogERROR("This shouldn't happen!! node %d", page->id());
      LogFATAL("next node has %d records",
               Page(result.next_child_id)->meta().num_records());
    }
  }

  // Special cases:
  if (page->meta().page_type() == TREE_LEAVE &&
      page->meta().overflow_page() > 0) {
    // 1. Delete header of a overflow chain - the first overflow page becomee
    // new header.
    auto new_header = Page(page->meta().overflow_page());
    new_header->Meta()->set_parent_page(parent->id());
    new_header->Meta()->set_is_overflow_page(0);
    *(ParseRecordField<PageID>(parent, slot_id_in_parent)) = new_header->id();
  }
  else if (page->meta().page_type() == TREE_LEAVE && need_lookup_parent &&
           result.prev_child_id < 0 && result.next_child_id >= 0) {
    // 2. Delete the left-most leave of a tree node while there are still
    // siblings on the right - Don't delete its tree node. Instead, we let min
    // tree node point to second leave. One case is that when second leave is
    // overflowed and first leave can't re-distribute nor merge with it.
    if (result.slot < 0) {
      result = LookUpTreeNodeInfoForPage(*page);
    }
    // Min tree node record re-point to second leave if exists.
    if (result.next_child_id > 0) {
      //printf("next child id is moved to first %d\n", result.next_child_id);
      CheckLogFATAL(parent->DeleteRecord(result.next_slot),
                    "Failed to delete tree node record of second leave %d",
                    result.next_child_id);
      *(ParseRecordField<PageID>(parent, slot_id_in_parent)) =
                                                        result.next_child_id;
    }
    else {
      // No leaves on the right.
      CheckLogFATAL(parent->DeleteRecord(slot_id_in_parent),
                    "Failed to delete tree node record of page %d", page->id());
    }
  }
  else {
    // 3. Common - Delete the tree node record of this page from its parent.
    CheckLogFATAL(parent->DeleteRecord(slot_id_in_parent),
                  "Failed to delete tree node record of page %d", page->id());
  }

  // Connect leaves after deleting.
  if (page->meta().page_type() == TREE_LEAVE) {
    ConnectLeaves(Page(page->meta().prev_page()),
                  Page(page->meta().next_page()));
  }

  // Recycle the page.
  CheckLogFATAL(RecyclePage(page->id()),
                "Failed to recycle page %d", page->id());

  // Parent node need to re-distribute records or merge nodes because of tree
  // node record deletion.
  ProcessNodeAfterRecordDeletion(parent, nullptr);

  return true;
}

bool BplusTree::ProcessNodeAfterRecordDeletion(
         RecordPage* page,
         DB::DeleteResult* delete_result) {
  if (!page) {
    LogERROR("nullptr page input to ProcessNodeAfterRecordDeletion");
    return false;
  }

  // Check space occupation less than 50%
  if (page->Occupation() >= 0.5) {
    return true;
  }

  // Process overflow page only when it's empty.
  if (page->meta().is_overflow_page()) {
    if (page->meta().num_records() <= 0) {
      DeleteOverflowLeave(page);
    }
    return true;
  }

  // If current page becomes empty, delete this page from B+ tree and call
  // DeleteNodeFromTree() recursively.
  if (page->meta().num_records() == 0) {
    // Pass -1 to recursive calls so that page's parent will be looked up.
    //debug(0);
    return DeleteNodeFromTree(page, -1);
  }

  // When page is root, and if it has only one record left after deleting this
  // child page, the root should be deleted. Instead, the remaining child
  // becomes new root.
  if (page->id() == header_->root_page()) {
    if (page->meta().num_records() > 1) {
      return true;
      //debug(1);
    }
    if (header_->depth() <= 1) {
      //debug(10);
      return true;
    }

    //debug(2);
    header_->decrement_depth(1);
    int page_id = -1;
    PageRecordsManager prmanager(page, schema(), key_indexes_,
                                 file_type_, page->meta().page_type());
    for (uint32 i = 0 ; i < prmanager.NumRecords(); i++) {
      // if (prmanager.RecordSlotID(i) < 0) {
      //   continue;
      // }
      page_id = (prmanager.GetRecord<Storage::TreeNodeRecord>(i))->page_id();
      printf("page_id = %d\n", page_id);
    }
    CheckLogFATAL(page_id > 0,  // new root id must > 0 (page 0 is meta page).
                  "Failed to find new root, current root is %d", page->id());
    header_->set_root_page(page_id);
    Page(page_id)->Meta()->set_parent_page(-1);
    CheckLogFATAL(RecyclePage(page->id()),
                  "Failed to recycle root %d", page->id());
    return true;
  }

  // Parse current page's parent and tries to find its siblings.
  // We prefer re-distribute records rather than merging, since merging might
  // propagate into upper nodes, and make nodes more occupied.
  //debug(3);
  SearchTreeNodeResult result = LookUpTreeNodeInfoForPage(*page);
  CheckLogFATAL(result.slot >= 0, "Invalid slot id of page %d in parent");

  std::vector<DataRecordRidMutation>* rid_mutations =
                   delete_result ? &delete_result->rid_mutations : nullptr;

  if (result.next_child_id >= 0) {
    if (ReDistributeRecordsWithinTwoPages(page, Page(result.next_child_id),
                                          result.next_slot, rid_mutations)) {
      //debug(4);
      if (page->meta().page_type() == TREE_LEAVE && delete_result) {
        delete_result->mutated_leaves.push_back(result.next_child_id);
      }
      return true;
    }
  }
  if (result.prev_child_id >= 0) {
    if (ReDistributeRecordsWithinTwoPages(Page(result.prev_child_id), page,
                                          result.slot, rid_mutations)) {
      //debug(5);
      if (page->meta().page_type() == TREE_LEAVE && delete_result) {
        delete_result->mutated_leaves.push_back(result.prev_child_id);
      }
      return true;
    }
  }
  if (result.next_child_id >= 0) {
    if (MergeTwoNodes(page, Page(result.next_child_id),
                      result.next_slot, rid_mutations)) {
      //debug(6);
      if (page->meta().page_type() == TREE_LEAVE && delete_result) {
        delete_result->mutated_leaves.push_back(result.next_child_id);
      }
      return true;
    }
  }
  if (result.prev_child_id >= 0) {
    if (MergeTwoNodes(Page(result.prev_child_id), page,
                      result.slot, rid_mutations)) {
      //debug(7);
      return true;
    }
  }

  return true;
}

bool BplusTree::Do_DeleteRecordByKey(
         const std::vector<std::shared_ptr<RecordBase>>& keys,
         DB::DeleteResult* result) {
  if (header_->root_page() < 0) {
    return true;
  }

  RecordPage* crt_leave = nullptr;
  for (const auto& key: keys) {
    auto leave = SearchByKey(*key);
    if (!leave) {
      LogERROR("Can't search to leave by this key:");
      keys[0]->Print();
      return false;
    }
    int leave_id = leave->id();

    // Post process for the node after record deletion.
    if (crt_leave && crt_leave->id() != leave->id()) {
      result->mutated_leaves.clear();
      ProcessNodeAfterRecordDeletion(crt_leave, result);
    }

    // Delete records.
    // The leave we previously searched might have been 'mutated' (deleted
    // or has records re-distributed) during the last run of
    // ProcessNodeAfterRecordDeletion(). We need to re-search the key.
    if (!result->mutated_leaves.empty() &&
        result->mutated_leaves[0] == leave_id) {
      leave = SearchByKey(*key);
    }
    int num = DeleteMatchedRecordsFromLeave(*key, leave, result);
    printf("deleted %d matching records\n", num);
    (void)num;
    crt_leave = leave;
  }
  ProcessNodeAfterRecordDeletion(crt_leave, result);

  return true;
}

bool BplusTree::Do_DeleteRecordByRecordID(
         DB::DeleteResult& index_del_result,
         DB::DeleteResult* result) {
  if (!result) {
    LogERROR("nullptr input 'result' to Do_DeleteRecordByRecordID");
    return false;
  }

  if (index_del_result.rid_deleted.empty()) {
    return true;
  }

  DataRecordRidMutation::SortByOldRid(&index_del_result.rid_deleted);

  auto& rid_deleted = index_del_result.rid_deleted;
  int group_start = 0, group_end = 0;
  for (int i = 0; i <= (int)rid_deleted.size(); i++) {
    if (i < (int)rid_deleted.size() &&
        (rid_deleted[group_start].old_rid.page_id() ==
         rid_deleted[i].old_rid.page_id())) {
      continue;
    }
    group_end = i;

    // delete this rids in [group_start, group_end - 1]
    auto page = Page(rid_deleted[group_start].old_rid.page_id());
    DB::DeleteResult crt_result;
    CheckLogFATAL(page, "Can't find data tree leave %d", page->id());
    //printf("-------------\n");
    for (int j = group_start; j < group_end; j++) {
      //printf("deleting: ");
      //rid_deleted[j].old_rid.Print();
      crt_result.rid_deleted.emplace_back(GetRecord(rid_deleted[j].old_rid),
                                          rid_deleted[j].old_rid,
                                          RecordID());
      page->DeleteRecord(rid_deleted[j].old_rid.slot_id());
    }

    ProcessNodeAfterRecordDeletion(page, &crt_result);

    index_del_result.UpdateDeleteRidsFromMutatedRids(crt_result);
    result->MergeFrom(crt_result);

    // printf("#### after merging rids\n");
    // for (const auto& m: index_del_result.rid_deleted) {
    //   m.Print();
    // }
    group_start = group_end;
    //printf("-------------\n");
  }

  return true;
}

bool BplusTree::UpdateIndexRecords(
    std::vector<DataRecordRidMutation>& rid_mutations) {
  if (rid_mutations.empty()) {
    return true;
  }

  bool is_delete_irecord = !(rid_mutations[0].new_rid.IsValid());

  // Sort DataRecordRidMutation list based on this tree's indexes.
  DataRecordRidMutation::Sort(&rid_mutations, key_indexes_);
  // Group DataRecordRidMutation list by key.
  std::vector<RecordGroup> rgroups;
  DataRecordRidMutation::GroupDataRecordRidMutations(
                              rid_mutations, key_indexes_, &rgroups);

  RecordPage* crt_leave = nullptr;
  std::shared_ptr<PageRecordsManager> prmanager;
  int i = 0;
  for (int rg_index = 0; rg_index <= (int)rgroups.size(); rg_index++) {
    if (rg_index == (int)rgroups.size()) {
      if (is_delete_irecord) {
        DB::DeleteResult result;
        ProcessNodeAfterRecordDeletion(crt_leave, &result);
      }
      break;
    }

    // Get key of this rgroup.
    auto group = rgroups[rg_index];
    auto crt_record = rid_mutations[group.start_index].record;
    RecordBase key;
    (reinterpret_cast<const DataRecord*>(crt_record.get()))
                                            ->ExtractKey(&key, key_indexes_);

    auto leave = SearchByKey(key);
    CheckLogFATAL(leave, "Failed to search key of rgroup.");
    int leave_id = leave->id();
    //printf("search to leave %d\n", leave_id);

    // If we search to a different leave for this rgroup, post-process current
    // leave after deletion.
    DB::DeleteResult result;
    if (crt_leave && crt_leave->id() != leave->id()) {
      result.mutated_leaves.clear();
      if (is_delete_irecord) {
        //printf("process leave %d\n", crt_leave->id());
        ProcessNodeAfterRecordDeletion(crt_leave, &result);
      }
      crt_leave = leave;
      prmanager.reset();
    }

    if (!crt_leave) {
      crt_leave = leave;
      prmanager.reset();
    }

    if (!result.mutated_leaves.empty() &&
        result.mutated_leaves[0] == leave_id) {
      leave = SearchByKey(key);
      crt_leave = leave;
    }

    // Load current leave if it's not loaded.
    if (!prmanager) {
      // Load new leave.
      prmanager.reset(new PageRecordsManager(
          leave, schema(), key_indexes_,
          INDEX, TREE_LEAVE)
      );
      i = 0;
    }

    // Begin updating or deleting index records to the leave.
    uint32 num_rids_updated = 0;
    while (leave) {
      for (; i < (int)prmanager->NumRecords(); i++) {
        int re = prmanager->CompareRecordWithKey(prmanager->record(i), key);
        if (re > 0) {
          break;
        } else if (re < 0) {
          continue;
        }
        RecordID rid = reinterpret_cast<IndexRecord*>(
                                   prmanager->mutable_record(i))->rid();
        // printf("scanning old rid\n");
        // rid.Print();
        for (uint32 rid_m_index = group.start_index;
             rid_m_index < group.start_index + group.num_records;
             rid_m_index++) {
          if (rid == rid_mutations[rid_m_index].old_rid) {
            // Update/Delete the rid for the IndexRecord.
            if (!is_delete_irecord) {
              prmanager->UpdateRecordID(prmanager->RecordSlotID(i),
                                        rid_mutations[rid_m_index].new_rid);
            }
            else {
              leave->DeleteRecord(prmanager->RecordSlotID(i));
            }
            num_rids_updated++;
          }
        }
      }

      // We might need to continue searching overflow pages.
      auto processed_leave = leave;
      leave = Page(leave->meta().overflow_page());
      if (leave) {
        prmanager.reset(new PageRecordsManager(
            leave, schema(), key_indexes_,
            INDEX, TREE_LEAVE)
        );
        i = 0;
      }
      if (processed_leave->meta().is_overflow_page() &&
          processed_leave->meta().num_records() <= 0) {
        DeleteOverflowLeave(processed_leave);  // Don't leak page.
      }
    }
    if (num_rids_updated != group.num_records) {
      LogERROR("Updated %d number of rids, expect %d, on leave %d, tree %d",
               num_rids_updated, group.num_records,
               crt_leave->id(), key_indexes_[0]);
      key.Print();
      return false;
    }
    // printf("---------- udpate for key ----------: \n");
    // key.Print();
    // printf("leave id = %d\n", leave->id());
    // printf("current leave = %d\n", crt_leave->id());
  }

  return true;
}

int BplusTree::DeleteMatchedRecordsFromLeave(
         const RecordBase& key,
         RecordPage* leave,
         DB::DeleteResult* result) {
  if (!leave || !result) {
    LogERROR("Nullptr input to DeleteMatchedRecordsFromLeave");
    return -1;
  }

  //printf("deleting records from leave %d \n", leave->id());
  int count_deleted = 0;
  while (leave) {
    PageRecordsManager prmanager(leave, schema(), key_indexes_,
                                 file_type_, TREE_LEAVE);
    // Fetch all matching records in this leave.
    bool last_is_match = false;
    for (uint32 index = 0; index < prmanager.NumRecords(); index++) {
      if (prmanager.CompareRecordWithKey(prmanager.record(index), key) == 0) {
        int slot_id = prmanager.RecordSlotID(index);
        if (result->del_mode == DB::DeleteResult::DEL_DATA) {
          // Save all records deleted from data tree.
          result->rid_deleted.emplace_back(
                                prmanager.plrecords().at(index).shared_record(),
                                RecordID(leave->id(), slot_id),
                                RecordID());
        }
        else if (result->del_mode == DB::DeleteResult::DEL_INDEX_PRE) {
          // Save the rids to delete from data tree.
          auto index_record = prmanager.GetRecord<IndexRecord>(index);
          result->rid_deleted.emplace_back(
                                  std::shared_ptr<RecordBase>(),
                                  RecordID(index_record->rid()),
                                  RecordID());
        }
        //prmanager.record(index).Print();
        CheckLogFATAL(leave->DeleteRecord(slot_id),
                      "Failed to delete record from leave %d", leave->id());
        count_deleted++;
        last_is_match = true;
      }
      else {
        if (leave->meta().is_overflow_page()) {
          // Overflow page must have all same records that match the key we're
          // searching for.
          LogFATAL("Overflow page stores inconsistent records!");
        }
        last_is_match = false;
      }
    }

    auto processed_leave = leave;
    // If index reaches the end of all records, check overflow page.
    if (last_is_match && leave->meta().overflow_page() >= 0) {
      leave = Page(leave->meta().overflow_page());
    }
    else {
      leave = nullptr;
    }
    // Delete overflow page if it becomes empty.
    if (processed_leave->meta().is_overflow_page() &&
        processed_leave->meta().num_records() <= 0) {
      DeleteOverflowLeave(processed_leave);
    }
  }

  return count_deleted;
}

bool BplusTree::DeleteOverflowLeave(RecordPage* leave) {
  if (!leave) {
    return false;
  }

  auto prev_leave = Page(leave->meta().prev_page());
  auto next_leave = Page(leave->meta().next_page());

  if (next_leave && next_leave->meta().is_overflow_page()) {
    prev_leave->Meta()->set_overflow_page(next_leave->id());
  }
  else {
    prev_leave->Meta()->set_overflow_page(-1);
  }
  ConnectLeaves(prev_leave, next_leave);
  RecyclePage(leave->id());
  return true;
}

RecordID BplusTree::InsertAfterOverflowLeave(
         const RecordBase& record,
         RecordPage* leave,
         SearchTreeNodeResult* search_result,
         std::vector<DataRecordRidMutation>* rid_mutations) {
  // Try inserting to this overflow page. If the new record happens to match
  // this overflow page we're done in this special case.
  RecordID rid = InsertNewRecordToOverFlowChain(record, leave);
  if (rid.IsValid()) {
    return rid;
  }

  // Special case - The overflowed leave has different tree node key with the
  // records in this leave - which means, the tree record is possibly 'less'
  // than records of the overflow page, and the new leave should reside left to
  // current leave.
  PageRecordsManager prmanager(leave, schema(), key_indexes_,
                               file_type_, TREE_LEAVE);
  if (prmanager.CompareRecords(record, prmanager.record(0)) < 0) {
    auto parent = Page(leave->meta().parent_page());
    // Replace the page_id field of left most TreeNodeRecord of parent page
    // with the new left-most leave.
    RecordPage* new_leave = CreateNewLeaveWithRecord(record);
    new_leave->Meta()->set_parent_page(parent->id());
    *(ParseRecordField<PageID>(parent, search_result->slot)) = new_leave->id();

    // Now the previous left-most leave (crt_leave) is the second leave to
    // the left, and it's tree node record in parent has been replaced by
    // new leave. We need to re-insert its tree node record.
    Storage::TreeNodeRecord tn_record;
    ProduceKeyRecordFromNodeRecord(prmanager.record(0), &tn_record);
    tn_record.set_page_id(leave->id());
    if (!InsertTreeNodeRecord(tn_record, parent)) {
      LogERROR("Inserting new leave to B+ tree failed");
      return RecordID();
    }
    auto prev_leave = Page(leave->meta().prev_page());
    ConnectLeaves(new_leave, leave);
    if (prev_leave) {
      ConnectLeaves(prev_leave, new_leave);
    }
    return RecordID(new_leave->id(), 0);
  }

  // Otherwise we check next leave. If next leave has the same parent, and is
  // not overflowed, we can do some redistribution.
  if (search_result->next_child_id >= 0 &&
      Page(search_result->next_child_id)->meta().overflow_page() < 0) {
    // First try inserting the new record to next leave.
    rid = InsertNewRecordToNextLeave(record, leave, search_result);
    if (rid.IsValid()) {
      return rid;
    }

    // Otherwise we need to create a new leave with the new record to insert,
    // and try re-distributing records with the next_leave if possible.
    Storage::TreeNodeRecord tn_record;
    RecordPage* new_leave = CreateNewLeaveWithRecord(record, &tn_record);
    tn_record.set_page_id(new_leave->id());
    rid = RecordID(new_leave->id(), 0);

    // Re-distribute records from next leave to balance.
    auto next_page = Page(search_result->next_child_id);
    CheckLogFATAL(next_page, "Failed to fetch next child leave");
    ReDistributeRecordsWithinTwoPages(new_leave, next_page,
                                      search_result->next_slot, rid_mutations,
                                      true);

    // Insert tree node record of the new leave to parent.
    auto parent = Page(leave->meta().parent_page());
    if (!InsertTreeNodeRecord(tn_record, parent)) {
      LogERROR("Inserting new leave to B+ tree failed");
      return RecordID();;
    }
    RecordPage* page2 = Page(search_result->next_leave_id);
    RecordPage* page1 = Page(page2->meta().prev_page());
    ConnectLeaves(new_leave, page2);
    ConnectLeaves(page1, new_leave);
  }
  else {
    // Next leave has different parent, or is overflowed too. We have no
    // choice but creating new leave and just inserting it to parent node.
    Storage::TreeNodeRecord tn_record;
    RecordPage* new_leave = CreateNewLeaveWithRecord(record, &tn_record);
    tn_record.set_page_id(new_leave->id());
    rid = RecordID(new_leave->id(), 0);
    
    // Insert tree node record of the new leave to parent.
    auto parent = Page(leave->meta().parent_page());
    if (!InsertTreeNodeRecord(tn_record, parent)) {
      LogERROR("Inserting new leave to B+ tree failed");
      return RecordID();
    }
    RecordPage* page1 = leave;
    if (search_result->next_leave_id >= 0) {
      RecordPage* page2 = Page(search_result->next_leave_id);
      page1 = Page(page2->meta().prev_page());
      ConnectLeaves(new_leave, page2);
    }
    else {
      page1 = GotoOverflowChainEnd(page1);
    }
    ConnectLeaves(page1, new_leave);
  }
  return rid;
}

RecordID BplusTree::ReDistributeToNextLeave(
         const RecordBase& record,
         RecordPage* leave,
         SearchTreeNodeResult* search_result,
         std::vector<DataRecordRidMutation>* rid_mutations) {
  // Check next child is valid.
  if (search_result->next_child_id < 0 ||
      Page(search_result->next_child_id)->meta().overflow_page() >= 0) {
    return RecordID();
  }

  auto next_leave = Page(search_result->next_child_id);
  CheckLogFATAL(next_leave, "Failed to fetch next child leave");
  // printf("(%d, %d)\n", leave->meta().space_used(),
  //        next_leave->meta().space_used());
  // If next leave is even more 'full' than current leave, can't redistribute.
  if (leave->meta().space_used() <= next_leave->meta().space_used()) {
    return RecordID();
  }

  PageRecordsManager prmanager(leave, schema(), key_indexes_,
                               file_type_, TREE_LEAVE);
  // Insert new record, sort and group records.
  if (!prmanager.InsertNewRecord(record)) {
    LogFATAL("Can't insert new record to PageRecordsManager");
  }
  std::vector<RecordGroup> rgroups;
  prmanager.GroupRecords(&rgroups);

  // Find new which record group new record belongs to.
  int new_record_gindex = 0;
  bool found_group = false;
  // printf("rgroup size = %d\n", (int)rgroups.size());
  for (; new_record_gindex < (int)rgroups.size(); new_record_gindex++) {
    for (uint32 index = rgroups[new_record_gindex].start_index;
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

  uint32 records_to_move = 0;
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
    if (size_to_move >= (int)record.size()) {
      can_insert_new_record = true;
      break;
    }
  }

  if (!can_insert_new_record) {
    return RecordID();
  }

  int left_size = leave->meta().space_used() - size_to_move + record.size();
  int right_size = next_leave->meta().space_used() + size_to_move;
  CheckLogFATAL((int)prmanager.total_size_ - size_to_move == left_size,
                "left leave size error");
  CheckLogFATAL(next_leave->meta().space_used() + size_to_move == right_size,
                "right leave size error");

  uint32 min_gap = left_size > right_size ?
                      left_size - right_size : right_size - left_size;
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

  RecordID rid;
  // Move from next_leave to leave.
  for (int index = rgroups[gindex].start_index;
       index < (int)prmanager.NumRecords();
       index++) {
    int new_id = prmanager.record(index).InsertToRecordPage(next_leave);
    CheckLogFATAL(new_id >= 0, "Failed to insert record to next page");

    int slot_id = prmanager.RecordSlotID(index);
    if (slot_id >= 0) {
      CheckLogFATAL(leave->DeleteRecord(slot_id),
                    "Failed to remove record from current leave");
      if (file_type_ == INDEX_DATA) {
        rid_mutations->emplace_back(prmanager.shared_record(index),
                                    RecordID(leave->id(), slot_id),
                                    RecordID(next_leave->id(), new_id));
      }
    }
    else {
      rid = RecordID(next_leave->id(), new_id);
    }
  }

  // If new record is not in next leave, it needs to be inserted to current
  // leave.
  if (gindex > new_record_gindex) {
    int slot_id = record.InsertToRecordPage(leave);
    if (slot_id < 0) {
      LogFATAL("Failed to insert new record to current leave");
    }
    rid = RecordID(leave->id(), slot_id);
  }

  // Delete original tree node record of next leave from parent.
  auto parent = Page(next_leave->meta().parent_page());
  CheckLogFATAL(parent, "Failed to fetch parent of next leave");
  CheckLogFATAL(parent->DeleteRecord(search_result->next_slot),
                "Failed to delete next leave's original tree node record");

  // Insert new tree node record of next leave to parent.
  Storage::TreeNodeRecord tn_record;
  const auto& new_min_record = prmanager.record(rgroups[gindex].start_index);
  ProduceKeyRecordFromNodeRecord(new_min_record, &tn_record);
  tn_record.set_page_id(next_leave->id());
  CheckLogFATAL(InsertTreeNodeRecord(tn_record, parent),
                "Failed to insert new tree node record for next leave");

  // printf("(%d, %d)\n", leave->meta().space_used(),
  //        next_leave->meta().space_used());

  return rid;
}

RecordID BplusTree::Do_InsertRecord(
         const RecordBase& record,
         std::vector<DataRecordRidMutation>* rid_mutations) {
  // Verify this data record fields matches schema.
  if (!CheckRecordFieldsType(record)) {
    LogERROR("Record fields type mismatch table schema");
    return RecordID();
  }

  // If tree is empty, add first leave to it.
  if (header_->root_page() < 0) {
    auto leave = CreateNewLeaveWithRecord(record);
    if (!AddFirstLeaveToTree(leave)) {
      return RecordID();
    }
    return RecordID(leave->id(), 0);
  }

  // B+ tree is not empty. Search key to find the leave to insert new record.
  // Produce the key of the record to search.
  RecordBase search_key;
  ProduceKeyRecordFromNodeRecord(record, &search_key);

  // Search the leave to insert.
  RecordPage* crt_page = Page(header_->root_page());
  SearchTreeNodeResult search_result;
  while (crt_page && crt_page->meta().page_type() == TREE_NODE) {
    search_result = SearchInTreeNode(search_key, crt_page);
    crt_page = Page(search_result.child_id);
  }
  CheckLogFATAL(crt_page, "Failed to search for key");
  CheckLogFATAL(crt_page->meta().page_type() == TREE_LEAVE,
                "Key search ending at non-leave node");

  // If current leave is overflowed, we check if the new record is same with
  // existing ones in it. If yes, we can insert this record to overflow pages.
  // If not, we have go to next leave.
  if (crt_page->meta().overflow_page() >= 0) {
    return InsertAfterOverflowLeave(record, crt_page, &search_result,
                                    rid_mutations);
  }

  // Try inserting the new record to leave.
  int slot_id = record.InsertToRecordPage(crt_page);
  if (slot_id >= 0) {
    return RecordID(crt_page->id(), slot_id);
  }

  // Try to re-organize records with next leave, it applicable (next leave
  // must exists, not an overflow leave, and has the same parent).
  RecordID rid = ReDistributeToNextLeave(record, crt_page, &search_result,
                                         rid_mutations);
  if (rid.IsValid()) {
    return rid;
  }

  // We have to insert and split current leave.
  int next_page_id = search_result.next_leave_id;
  rid = InsertNewRecordToLeaveWithSplit(record, next_page_id, crt_page,
                                        rid_mutations);
  if (rid.IsValid()) {
    return rid;
  }

  LogERROR("Failed to insert new record to B+ tree");
  return RecordID();
}

RecordPage* BplusTree::GotoOverflowChainEnd(RecordPage* leave) {
  if (!leave) {
    LogERROR("Nullptr input to GotoOverflowChainEnd");
    return nullptr;
  }
  if (leave->meta().overflow_page() < 0) {
    //LogERROR("Leave %d not overflowed, skipping", leave->id());
    return leave;
  }

  RecordPage* crt_leave = leave;
  while (crt_leave) {
    int next_of_id = crt_leave->meta().overflow_page();
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

RecordID BplusTree::InsertNewRecordToOverFlowChain(
         const RecordBase& record, RecordPage* leave) {
  if (!leave) {
    LogERROR("Nullptr input to CreateNewLeaveWithRecord");
    return RecordID();
  }

  // First verify that the new record is same with existing records.
  PageRecordsManager prmanager(leave, schema(), key_indexes_,
                               file_type_, TREE_LEAVE);
  if (prmanager.CompareRecords(record, prmanager.record(0)) != 0) {
    return RecordID();
  }

  // record->Print();
  // prmanager.record(0).Print();
  // prmanager.record(prmanager.NumRecords() - 1).Print();

  RecordPage* crt_leave = GotoOverflowChainEnd(leave);
  // Either insert record to last overflow leave, or append new overflow leave
  // if last overflow leave is full.
  int slot_id = record.InsertToRecordPage(crt_leave);
  if (slot_id < 0) {
    // Append new overflow page.
    auto next_leave = Page(crt_leave->meta().next_page());
    auto new_of_leave = AppendOverflowPageTo(crt_leave);
    crt_leave = new_of_leave;
    slot_id = record.InsertToRecordPage(crt_leave);
    CheckLogFATAL(slot_id >= 0,"Failed to insert record to new overflow leave");
    if (next_leave) {
      ConnectLeaves(new_of_leave, next_leave);
    }
  }
  return RecordID(crt_leave->id(), slot_id);
}

RecordPage* BplusTree::CreateNewLeaveWithRecord(
    const RecordBase& record, Storage::TreeNodeRecord* tn_record) {
  RecordPage* new_leave = AllocateNewPage(TREE_LEAVE);
  CheckLogFATAL(record.InsertToRecordPage(new_leave) >= 0,
                "Failed to insert record to empty leave");
  if (tn_record) {
    ProduceKeyRecordFromNodeRecord(record, tn_record);
  }
  return new_leave;
}

RecordID BplusTree::InsertNewRecordToLeaveWithSplit(
         const RecordBase& record, int next_leave_id,
         RecordPage* leave,
         std::vector<DataRecordRidMutation>* rid_mutations) {
  //printf("Splitting leave %d\n", leave->id());
  if (!leave) {
    LogERROR("Nullptr input to InsertNewRecordToLeaveWithSplit");
    return RecordID();
  }

  // Need to split the page.
  PageRecordsManager prmanager(leave, schema(), key_indexes_,
                               file_type_, TREE_LEAVE);
  prmanager.set_tree(this);
  auto leaves = prmanager.InsertRecordAndSplitPage(record, rid_mutations);

  // Insert back split leave pages to parent tree node(s).
  RecordPage* parent = Page(leave->meta().parent_page());
  CheckLogFATAL(parent, "Failed to fetch parent page of current leave");
  if (leaves.size() > 1) {
    //printf("%d split leaves returned\n", (int)leaves.size());
    for (int i = 1; i < (int)leaves.size(); i++) {
      Storage::TreeNodeRecord tn_record;
      ProduceKeyRecordFromNodeRecord(*leaves[i].record, &tn_record);
      tn_record.set_page_id(leaves[i].page->id());

      if (!InsertTreeNodeRecord(tn_record, parent)) {
        LogFATAL("Failed to add split leave %d into parent");
      }
      // Reset parent because upper node may have been split after last leave
      // was inserted. We always add next leave to the same parent as of its
      // prev leave.
      parent = Page(leaves[i].page->meta().parent_page());
      CheckLogFATAL(parent, "Failed to fetch parent page of current leave");
    }
    // Connect last split leave with following leaves.
    if (next_leave_id >= 0) {
      auto tail_leave = GotoOverflowChainEnd(leaves[leaves.size()-1].page);
      ConnectLeaves(tail_leave, Page(next_leave_id));
    }
  }
  else {
    //printf("overflow self\n");
    // Only one leave - original leave with a overflow page.
    auto of_leave = Page(leaves[0].page->meta().overflow_page());
    CheckLogFATAL(of_leave, "Failed to fetch overflow page of non-split leave");
    if (next_leave_id >= 0) {
      ConnectLeaves(of_leave, Page(next_leave_id));
    }
  }

  // The new record rid is in stored in leave[0].
  return leaves[0].rid;
}


}  // namespace Storage

