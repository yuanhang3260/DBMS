#ifndef STORAGE_RECORD_
#define STORAGE_RECORD_

#include <vector>
#include <memory>

#include "Database/Catalog_pb.h"
#include "Schema/DataTypes.h"
#include "Storage/Common.h"
#include "Storage/RecordPage.h"

namespace Storage {

enum RecordType {
  UNKNOWN_RECORDTYPE,
  INDEX_RECORD,
  DATA_RECORD,
  TREENODE_RECORD,
};

std::string RecordTypeStr(RecordType record_type);

class RecordID {
 public:
  RecordID() = default;
  RecordID(int page_id, int slot_id) : page_id_(page_id), slot_id_(slot_id) {}

  // Accessors
  DEFINE_ACCESSOR(page_id, int);
  DEFINE_ACCESSOR(slot_id, int);

  int DumpToMem(byte* buf) const;
  int LoadFromMem(const byte* buf);

  uint32 size() const { return sizeof(page_id_) + sizeof(slot_id_); }

  void Print() const;

  bool IsValid() const { return page_id_ >= 0 && slot_id_ >= 0; }

  void reset() { page_id_ = -1; slot_id_ = -1; }

  bool operator==(const RecordID& other) const {
    return page_id_ == other.page_id_ && slot_id_ == other.slot_id_;
  }

  bool operator<(const RecordID& other) const {
    if (page_id_ != other.page_id_) {
      return page_id_ < other.page_id_;
    }
    return slot_id_ < other.slot_id_;
  }

  bool operator>(const RecordID& other) const {
    if (page_id_ != other.page_id_) {
      return page_id_ > other.page_id_;
    }
    return slot_id_ > other.slot_id_;
  }

  bool operator<=(const RecordID& other) const {
    return !((*this) > other);
  }

  bool operator>=(const RecordID& other) const {
    return !((*this) < other);
  }

  bool operator!=(const RecordID& other) const {
    return !((*this) == other);
  }

 private:
  int page_id_ = -1;
  int slot_id_ = -1;
};

class RecordBase {
 public:
  RecordBase() = default;
  std::vector<std::shared_ptr<Schema::Field>>& fields() { return fields_; }
  const std::vector<std::shared_ptr<Schema::Field>>& fields() const {
    return fields_;
  }

  virtual ~RecordBase() { /*printf("deleting RecordBase\n");*/ }

  // Total size it takes as raw data.
  virtual uint32 size() const;

  // Number of fields this key contains.
  uint32 NumFields() const { return fields_.size(); }

  virtual RecordType type() const { return UNKNOWN_RECORDTYPE; }

  // Print this record.
  virtual void Print() const;

  // Add a new field. This method takes ownership of SchemaFieldType pointer.
  void AddField(Schema::Field* new_field);
  void AddField(std::shared_ptr<Schema::Field> new_field);

  // Reset all fields to minimum.
  virtual void reset();

  virtual void clear();

  // Init records fields with schema and key_indexes.
  bool InitRecordFields(const DB::TableInfo& schema,
                        const std::vector<int>& indexes);

  // Check all fields type match a schema.
  bool CheckFieldsType(const DB::TableInfo& schema,
                       std::vector<int> key_indexes) const;
  bool CheckFieldsType(const DB::TableInfo& schema) const;

  // Parse from text of format as Print() method prints.
  bool ParseFromText(std::string str, int chararray_len_limit);

  // Compare 2 Schema fields. We first compare field type and then the value
  // if the field types are same.
  static int CompareSchemaFields(const Schema::Field* field1,
                                 const Schema::Field* field2);

  virtual int DumpToMem(byte* buf) const;
  virtual int LoadFromMem(const byte* buf);

  virtual RecordBase* Duplicate() const;
  bool CopyFieldsFrom(const RecordBase& source);

  // Overloading operators.
  bool operator<(const RecordBase& other) const;
  bool operator>(const RecordBase& other) const;
  bool operator<=(const RecordBase& other) const;
  bool operator>=(const RecordBase& other) const;
  bool operator==(const RecordBase& other) const;
  bool operator!=(const RecordBase& other) const;

  static int CompareRecordsBasedOnIndex(const RecordBase& r1,
                                        const RecordBase& r2,
                                        const std::vector<int>& indexes);

  static bool RecordComparator(const RecordBase& r1,
                               const RecordBase& r2,
                               const std::vector<int>& indexes);

  // Greater comparator. This can be used in containers like std::priority_queue
  // which is a max heap, to provide stable sort.
  static bool RecordComparatorGt(const RecordBase& r1,
                                 const RecordBase& r2,
                                 const std::vector<int>& indexes);

  static int CompareRecords(const RecordBase& r1, const RecordBase& r2);

  static int CompareRecordWithKey(const RecordBase& key,
                                  const RecordBase& record,
                                  const std::vector<int>& indexes);

  // Insert the record to a page and returns the record's slot id.
  int InsertToRecordPage(RecordPage* page) const;

 protected:
  void PrintImpl() const;

  std::vector<std::shared_ptr<Schema::Field>> fields_;
};


// Data record. They are the real records of database tables, residing in
// index-data B+ tree file leave nodes.
class DataRecord: public RecordBase {
 public:
  virtual ~DataRecord() {}

  virtual RecordType type() const { return DATA_RECORD; }

  RecordBase* Duplicate() const override;

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
  virtual ~IndexRecord() {}

  DEFINE_ACCESSOR(rid, RecordID);

  uint32 size() const override;
  virtual RecordType type() const { return INDEX_RECORD; }

  int DumpToMem(byte* buf) const override;
  int LoadFromMem(const byte* buf) override;

  void Print() const override;

  RecordBase* Duplicate() const override;

  void reset() override;

 private:
  RecordID rid_;
};


// B+ tree node record, consisting of (Record_key, page_id).
class TreeNodeRecord: public RecordBase {
 public:
  virtual ~TreeNodeRecord() {}

  DEFINE_ACCESSOR(page_id, int);

  uint32 size() const override;
  virtual RecordType type() const { return TREENODE_RECORD; }

  int DumpToMem(byte* buf) const override;
  int LoadFromMem(const byte* buf) override;

  void Print() const override;

  RecordBase* Duplicate() const override;

  void reset() override;

 private:
  int page_id_ = -1;
};


}  // namespace Schema

#endif  /* STORAGE_RECORD_ */
