#ifndef SCHEMA_PAGE_RECORDS_MANAGER_
#define SCHEMA_PAGE_RECORDS_MANAGER_

#include "Record.h"
#include "Storage/BplusTree.h"

namespace DataBaseFiles {
  class BplusTree;
}

namespace Schema {

class PageRecordsManager;

// This class wraps a record loaded from page.
class PageLoadedRecord {
 public:
  PageLoadedRecord() = default;
  PageLoadedRecord(int slot_id) : slot_id_(slot_id) {}
  
  DEFINE_ACCESSOR(slot_id, int);
  DEFINE_ACCESSOR_SMART_PTR(record, Schema::RecordBase);

  std::shared_ptr<RecordBase> Record() { return record_; }

  int NumFields() const {
    if (!record_) {
      return 0;
    }
    return record_->fields().size();
  }

  // Generate internal record type for this PageLoadedRecord. The internal
  // reocrd can be DataRecord, IndexRecord or TreeNodeRecord, depending on
  // the specified file_type and page_type.
  bool GenerateRecordPrototype(const TableSchema* schema,
                               std::vector<int> key_indexes,
                               DataBaseFiles::FileType file_type,
                               DataBaseFiles::PageType page_type);

  // Comparator
  static bool Comparator(const PageLoadedRecord& r1, const PageLoadedRecord& r2,
                         const std::vector<int>& indexes);

  void Print() const {
    std::cout << "slot[" << slot_id_ << "] ";
    record_->Print();
  }

 private:
  std::shared_ptr<RecordBase> record_;
  int slot_id_ = -1;

  friend class PageRecordsManager;
};


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

  class SplitLeaveResults {
   public:
    SplitLeaveResults(DataBaseFiles::RecordPage* page_) : page(page_) {}

    DataBaseFiles::RecordPage* page;
    std::shared_ptr<RecordBase> record;
  };

  // Insert a new data record to the plrecords list.
  std::vector<SplitLeaveResults>
  InsertRecordAndSplitPage(const RecordBase* record);

  class RecordGroup {
   public:
    RecordGroup(int start_index_, int num_records_, int size_) :
        start_index(start_index_),
        num_records(num_records_),
        size(size_) {
    }
    int start_index;
    int num_records;
    int size;
  };

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