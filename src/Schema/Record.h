#ifndef SCHEMA_RECORD_KEY_
#define SCHEMA_RECORD_KEY_

#include <vector>
#include <memory>

#include "Storage/Common.h"
#include "Storage/RecordPage.h"
#include "DataTypes.h"
#include "DBTable_pb.h"

namespace Schema {

class RecordID {
 public:
  RecordID() = default;
  RecordID(int page_id, int slot_id) : page_id_(page_id), slot_id_(slot_id) {}

  // Accessors
  DEFINE_ACCESSOR(page_id, int);
  DEFINE_ACCESSOR(slot_id, int);

  int DumpToMem(byte* buf) const;
  int LoadFromMem(const byte* buf);

  int size() const { return sizeof(page_id_) + sizeof(slot_id_); }

  void Print() const;

 private:
  int page_id_ = -1;
  int slot_id_ = -1;
};

class RecordBase {
 public:
  RecordBase() = default;
  std::vector<std::shared_ptr<SchemaFieldType>>& fields() { return fields_; }
  const std::vector<std::shared_ptr<SchemaFieldType>>& fields() const {
    return fields_;
  }

  // Total size it takes as raw data.
  virtual int size() const;

  // Number of fields this key contains.
  int NumFields() const { return fields_.size(); }

  // Print this record.
  virtual void Print() const;

  // Add a new fiild. This method takes ownership of SchemaFieldType pointer.
  void AddField(SchemaFieldType* new_field);

  // Compare 2 Schema fields. We first compare field type and then the value
  // if the field types are same.
  static int CompareSchemaFields(const SchemaFieldType* field1,
                                 const SchemaFieldType* field2);

  virtual int DumpToMem(byte* buf) const;
  virtual int LoadFromMem(const byte* buf);

  // Overloading operators.
  bool operator<(const RecordBase& other) const;
  bool operator>(const RecordBase& other) const;
  bool operator<=(const RecordBase& other) const;
  bool operator>=(const RecordBase& other) const;
  bool operator==(const RecordBase& other) const;
  bool operator!=(const RecordBase& other) const;

  static bool RecordComparator(const std::shared_ptr<RecordBase> r1,
                               const std::shared_ptr<RecordBase> r2,
                               const std::vector<int>& indexes);

  bool InsertToRecordPage(DataBaseFiles::RecordPage* page);

 protected:
  void PrintImpl() const;

  std::vector<std::shared_ptr<SchemaFieldType>> fields_;
};


// Data record. They are the real records of database tables, residing in
// index-data B+ tree file leave nodes.
class DataRecord: public RecordBase {
 public:
  // Extract key data to a RecordKey object. The fields to extract as key
  // are given in arg field_indexes.
  // Extracted RecordKey will not allocate space for nor take ownership of the
  // data from this Record. It just maintains shared pointers to original data.
  bool ExtractKey(RecordBase* key, const std::vector<int>& field_indexes) const;
};


// Index record. They are indexes of database tables, maintained in index B+
// tree leave nodes, consisting of (Record_key, Record_id).
class IndexRecord: public RecordBase {
 public:
  DEFINE_ACCESSOR(rid, RecordID);

  int size() const override;

  int DumpToMem(byte* buf) const override;
  int LoadFromMem(const byte* buf) override;

  void Print() const override;

 private:
  RecordID rid_;
};


// B+ tree node record, consisting of (Record_key, page_id).
class TreeNodeRecord: public RecordBase {
 public:
  DEFINE_ACCESSOR(page_id, int);

  int size() const override;

  int DumpToMem(byte* buf) const override;
  int LoadFromMem(const byte* buf) override;

  void Print() const override;

 private:
  int page_id_ = -1;
};


// This class wraps a record loaded from page.
class PageLoadedRecord {
 public:
  PageLoadedRecord() = default;
  PageLoadedRecord(int slot_id) : slot_id_(slot_id) {}
  
  DEFINE_ACCESSOR(slot_id, int);
  DEFINE_ACCESSOR_SMART_PTR(record, RecordBase);

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
  int slot_id_;
};

// Page Records Manager provide service to load a page and parse records stored
// in this page. The data structure is critical to processe a page.
class PageRecordsManager {
 public:
  PageRecordsManager(DataBaseFiles::RecordPage* page,
                     TableSchema* schema,
                     std::vector<int> key_indexes,
                     DataBaseFiles::FileType file_type,
                     DataBaseFiles::PageType page_type) :
      page_(page),
      schema_(schema),
      key_indexes_(key_indexes),
      file_type_(file_type),
      page_type_(page_type) {}

  // Accessors
  DEFINE_ACCESSOR(schema, TableSchema*);
  DEFINE_ACCESSOR(key_indexes, std::vector<int>);
  DEFINE_ACCESSOR(page, DataBaseFiles::RecordPage*);
  DEFINE_ACCESSOR_ENUM(file_type, DataBaseFiles::FileType);
  DEFINE_ACCESSOR_ENUM(page_type, DataBaseFiles::PageType);
  DEFINE_ACCESSOR(total_size, int);

  std::vector<PageLoadedRecord>& plrecords() { return plrecords_; }

  // Sort a list of records based on indexes that specified key.
  static void SortRecords(
      std::vector<std::shared_ptr<Schema::RecordBase>>& records,
      const std::vector<int>& key_indexes);

  // Load all records from a page and sort it based on key.
  bool LoadRecordsFromPage();

  // Insert a Record to Page.
  bool InsertRecordToPage(const RecordBase* record);

  bool CheckSort() const;

 private:
  DataBaseFiles::RecordPage* page_ = nullptr;

  std::vector<PageLoadedRecord> plrecords_;
  TableSchema* schema_ = nullptr;
  std::vector<int> key_indexes_;

  DataBaseFiles::FileType file_type_ = DataBaseFiles::UNKNOWN_FILETYPE;
  DataBaseFiles::PageType page_type_ = DataBaseFiles::UNKNOW_PAGETYPE;

  int total_size_ = 0;
};

}  // namespace Schema

#endif  /* SCHEMA_RECORD_KEY_ */
