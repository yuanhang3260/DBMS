#ifndef STORAG_PAGE_RECORDS_MANAGER_
#define STORAG_PAGE_RECORDS_MANAGER_

#include "Storage/BplusTree.h"
#include "Storage/PageRecord_Common.h"
#include "Storage/Record.h"

namespace DataBaseFiles {
  class BplusTree;
}

namespace Storage {

// Page Records Manager provide service to load a page and parse records stored
// in this page. The data structure is critical to processe a page.
class PageRecordsManager {
 public:
  PageRecordsManager(RecordPage* page,
                     const Schema::TableSchema& schema,
                     std::vector<int> key_indexes,
                     FileType file_type,
                     PageType page_type);

  // Accessors
  DEFINE_ACCESSOR(schema, const Schema::TableSchema*);
  DEFINE_ACCESSOR(key_indexes, std::vector<int>);
  DEFINE_ACCESSOR(page, RecordPage*);
  DEFINE_ACCESSOR_ENUM(file_type, FileType);
  DEFINE_ACCESSOR_ENUM(page_type, PageType);
  DEFINE_ACCESSOR(total_size, int);
  DEFINE_ACCESSOR(tree, BplusTree*);

  uint32 NumRecords() const { return plrecords_.size(); }
  std::vector<PageLoadedRecord>& plrecords() { return plrecords_; }

  const RecordBase& record(uint32 index) const;
  RecordBase* Record(uint32 index);
  std::shared_ptr<RecordBase> Shared_Record(uint32 index);

  int RecordSlotID(uint32 index) const;

  template<class T>
  T* GetRecord(int index) {
    return dynamic_cast<T*>(Record(index));
  }

  template<class T>
  static T* ParseRecordField(RecordPage* page, int slot_id) {
    return reinterpret_cast<T*>(page->Record(slot_id) +
                                page->RecordLength(slot_id) -
                                sizeof(T));
  }

  // Print this page.
  void Print() const;

  // Sort a list of records based on indexes that specified key.
  static void SortRecords(
      std::vector<std::shared_ptr<RecordBase>>* records,
      const std::vector<int>& key_indexes);

  void SortByIndexes(const std::vector<int>& key_indexes);

  // Load all records from a page and sort it based on key.
  bool LoadRecordsFromPage();

  // Insert a Record to Page.
  bool InsertRecordToPage(const RecordBase* record);

  // Check sorted list of PageLoadedRecords, based on given key.
  bool CheckSort() const;

  // Produce list of indexes of fields to compare 2 records. If the page is a
  // tree leave of an index-data file, it should return key_indexes.
  // Otherwise the PageLoadedRecord already contains only key fields, and thus
  // each field needs to be compared one by one.
  std::vector<int> ProduceIndexesToCompare() const;

  // Append a new record to the plrecords list. This function is only called
  // in splitting this page. It won't take owner ship of the record passed.
  // It returns the middle point that splits all recors equally in respect of
  // space they take.
  int AppendRecordAndSplitPage(const RecordBase& record);

  // Search for a key, returns the left boundary index this key should reside
  // in a B+ tree node.
  int SearchForKey(const RecordBase& record) const;

  // Compare key with a record. It performs comparison based on the file type
  // and page type.
  int CompareRecordWithKey(const RecordBase& key,
                           const RecordBase& record) const;

  int CompareRecords(const RecordBase& r1, const RecordBase& r2) const;

  // Update rid of an IndexRecord.
  bool UpdateRecordID(int slot_id, const RecordID& rid);

  class SplitLeaveResults {
   public:
    SplitLeaveResults(RecordPage* page_) : page(page_) {}

    RecordPage* page;
    std::shared_ptr<RecordBase> record;
    RecordID rid;
  };

  // Insert a new data record to the plrecords list.
  std::vector<SplitLeaveResults> InsertRecordAndSplitPage(
      const RecordBase& record,
      std::vector<DataRecordRidMutation>& rid_mutations);

  friend class BplusTree;

 private:
  bool InsertNewRecord(const RecordBase& record);
  void GroupRecords(std::vector<RecordGroup>* rgroups);

  RecordPage* page_ = nullptr;

  std::vector<PageLoadedRecord> plrecords_;
  const Schema::TableSchema* schema_ = nullptr;
  std::vector<int> key_indexes_;

  FileType file_type_ = UNKNOWN_FILETYPE;
  PageType page_type_ = UNKNOW_PAGETYPE;

  uint32 total_size_ = 0;

  BplusTree* tree_ = nullptr;
};

}


#endif  /* STORAG_PAGE_RECORDS_MANAGER_ */