#ifndef SCHEMA_RECORD_KEY_
#define SCHEMA_RECORD_KEY_

#include <vector>
#include <memory>

#include "Storage/Common.h"
#include "Storage/RecordPage.h"
#include "DataTypes.h"
#include "DBTable_pb.h"

namespace Schema {

enum RecordType {
  UNKNOWN_RECORDTYPE,
  TREENODE_RECORD,
  DATA_RECORD,
  INDEX_RECORD,
};

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

  virtual ~RecordBase() { /*printf("deleting RecordBase\n");*/ }

  // Total size it takes as raw data.
  virtual int size() const;

  // Number of fields this key contains.
  int NumFields() const { return fields_.size(); }

  virtual RecordType type() const { return UNKNOWN_RECORDTYPE; }

  // Print this record.
  virtual void Print() const;

  // Add a new fiild. This method takes ownership of SchemaFieldType pointer.
  void AddField(SchemaFieldType* new_field);

  // Reset all fields to minimum.
  virtual void reset();

  virtual void clear();

  // Init records fields with schema and key_indexes.
  bool InitRecordFields(const TableSchema* schema,
                        std::vector<int> key_indexes,
                        DataBaseFiles::FileType file_type,
                        DataBaseFiles::PageType page_type);

  // Check all fields type match a schema.
  bool CheckFieldsType(const TableSchema* schema,
                       std::vector<int> key_indexes) const;
  bool CheckFieldsType(const TableSchema* schema) const;

  // Compare 2 Schema fields. We first compare field type and then the value
  // if the field types are same.
  static int CompareSchemaFields(const SchemaFieldType* field1,
                                 const SchemaFieldType* field2);

  virtual int DumpToMem(byte* buf) const;
  virtual int LoadFromMem(const byte* buf);

  virtual RecordBase* Duplicate() const;
  bool CopyFieldsFrom(const RecordBase* source);

  // Overloading operators.
  bool operator<(const RecordBase& other) const;
  bool operator>(const RecordBase& other) const;
  bool operator<=(const RecordBase& other) const;
  bool operator>=(const RecordBase& other) const;
  bool operator==(const RecordBase& other) const;
  bool operator!=(const RecordBase& other) const;

  static int CompareRecordsBasedOnKey(const RecordBase* r1,
                                      const RecordBase* r2,
                                      const std::vector<int>& indexes);

  static bool RecordComparator(const std::shared_ptr<RecordBase> r1,
                               const std::shared_ptr<RecordBase> r2,
                               const std::vector<int>& indexes);

  static int CompareRecordWithKey(const RecordBase* key,
                                  const RecordBase* record,
                                  const std::vector<int>& indexes);

  bool InsertToRecordPage(DataBaseFiles::RecordPage* page) const;

 protected:
  void PrintImpl() const;

  std::vector<std::shared_ptr<SchemaFieldType>> fields_;
};


// Data record. They are the real records of database tables, residing in
// index-data B+ tree file leave nodes.
class DataRecord: public RecordBase {
 public:
  virtual ~DataRecord() {}

  RecordType type() const { return DATA_RECORD; }

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

  int size() const override;
  RecordType type() const { return INDEX_RECORD; }

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

  int size() const override;
  RecordType type() const { return TREENODE_RECORD; }

  int DumpToMem(byte* buf) const override;
  int LoadFromMem(const byte* buf) override;

  void Print() const override;

  RecordBase* Duplicate() const override;

  void reset() override;

 private:
  int page_id_ = -1;
};


}  // namespace Schema

#endif  /* SCHEMA_RECORD_KEY_ */
