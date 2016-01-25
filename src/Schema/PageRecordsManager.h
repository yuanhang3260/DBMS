#ifndef SCHEMA_PAGE_RECORDS_MANAGER_
#define SCHEMA_PAGE_RECORDS_MANAGER_

#include "Record.h"
#include "PageRecord_Common.h"
#include "Storage/BplusTree.h"

namespace DataBaseFiles {
  class BplusTree;
}

namespace Schema {

// Page Records Manager provide service to load a page and parse records stored
// in this page. The data structure is critical to processe a page.
class PageRecordsManager {
 public:
  PageRecordsManager(DataBaseFiles::RecordPage* page,
                     TableSchema* schema,
                     std::vector<int> key_indexes,
                     DataBaseFiles::FileType file_type,
                     DataBaseFiles::PageType page_type);

  // Accessors
  DEFINE_ACCESSOR(schema, TableSchema*);
  DEFINE_ACCESSOR(key_indexes, std::vector<int>);
  DEFINE_ACCESSOR(page, DataBaseFiles::RecordPage*);
  DEFINE_ACCESSOR_ENUM(file_type, DataBaseFiles::FileType);
  DEFINE_ACCESSOR_ENUM(page_type, DataBaseFiles::PageType);
  DEFINE_ACCESSOR(total_size, int);
  DEFINE_ACCESSOR(tree, DataBaseFiles::BplusTree*);

  int NumRecords() const { return plrecords_.size(); }
  std::vector<PageLoadedRecord>& plrecords() { return plrecords_; }
  RecordBase* Record(int index) const;
  std::shared_ptr<RecordBase> Shared_Record(int index);
  int RecordSlotID(int index) const;
  template<class T>
  T* GetRecord(int index) {
    return reinterpret_cast<T*>(Record(index));
  }

  // Print this page.
  void Print() const;

  // Sort a list of records based on indexes that specified key.
  static void SortRecords(
      std::vector<std::shared_ptr<Schema::RecordBase>>& records,
      const std::vector<int>& key_indexes);

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
  int AppendRecordAndSplitPage(RecordBase* record);

  // Search for a key, returns the left boundary index this key should reside
  // in a B+ tree node.
  int SearchForKey(const RecordBase* record) const;

  // Compare key with a record. It performs comparison based on the file type
  // and page type.
  int CompareRecordWithKey(const RecordBase* key,
                           const RecordBase* record) const;

  int CompareRecords(const RecordBase* r1, const RecordBase* r2) const;

  // Update rid of an IndexRecord.
  bool UpdateRecordID(int slot_id, const RecordID& rid);

  class SplitLeaveResults {
   public:
    SplitLeaveResults(DataBaseFiles::RecordPage* page_) : page(page_) {}

    DataBaseFiles::RecordPage* page;
    std::shared_ptr<RecordBase> record;
    RecordID rid;
  };

  // Insert a new data record to the plrecords list.
  std::vector<SplitLeaveResults> InsertRecordAndSplitPage(
      const RecordBase* record,
      std::vector<Schema::DataRecordRidMutation>& rid_mutations);

  friend class DataBaseFiles::BplusTree;

 private:
  bool InsertNewRecord(const RecordBase* record);
  void GroupRecords(std::vector<RecordGroup>* rgroups);

  DataBaseFiles::RecordPage* page_ = nullptr;

  std::vector<PageLoadedRecord> plrecords_;
  TableSchema* schema_ = nullptr;
  std::vector<int> key_indexes_;

  DataBaseFiles::FileType file_type_ = DataBaseFiles::UNKNOWN_FILETYPE;
  DataBaseFiles::PageType page_type_ = DataBaseFiles::UNKNOW_PAGETYPE;

  int total_size_ = 0;

  DataBaseFiles::BplusTree* tree_ = nullptr;
};

}


#endif  /* SCHEMA_PAGE_RECORDS_MANAGER_ */