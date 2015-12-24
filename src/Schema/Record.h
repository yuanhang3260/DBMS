#ifndef SCHEMA_RECORD_KEY_
#define SCHEMA_RECORD_KEY_

#include <vector>
#include <memory>

#include "DataTypes.h"
#include "Storage/RecordData.h"


namespace Schema {

class RecordBase {
 public:
  RecordBase() = default;
  std::vector<std::shared_ptr<SchemaFieldType>>& fields() { return fields_; }
  const std::vector<std::shared_ptr<SchemaFieldType>>& fields() const {
    return fields_;
  }

  // Total size it takes as raw data.
  int size() const;

  // Number of fields this key contains.
  int NumFields() const { return fields_.size(); }
  // Size of this key.

  // Add a new fiild. This method takes ownership of SchemaFieldType pointer.
  void AddField(SchemaFieldType* new_field);

  // Compare 2 Schema fields. We first compare field type and then the value
  // if the field types are same.
  static int CompareSchemaFields(const SchemaFieldType* field1,
                                 const SchemaFieldType* field2);

  int DumpToMem(byte* buf) const;
  int LoadFromMem(const byte* buf);

  // Overloading operators.
  bool operator<(const RecordBase& other) const;
  bool operator>(const RecordBase& other) const;
  bool operator<=(const RecordBase& other) const;
  bool operator>=(const RecordBase& other) const;
  bool operator==(const RecordBase& other) const;
  bool operator!=(const RecordBase& other) const;

 protected:
  std::vector<std::shared_ptr<SchemaFieldType>> fields_;
};


class RecordKey: public RecordBase {
 private:
  // TODO: Rid?
  //DataBaseFiles::RecordID* rid = nullptr;
};


class Record: public RecordBase {
 public:
  // Extract key data to a RecordKey object. The fields to extract as key
  // are given in arg field_indexes.
  // In extraction, "fields_" in RecordKey will not allocate for nor take owner
  // ship of the data of this Record.
  bool ExtractKey(RecordKey* key, const std::vector<int>& field_indexes) {
    return false;
  }
};

}  // namespace RecordBase

#endif  /* SCHEMA_RECORD_KEY_ */
