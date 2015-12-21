#ifndef SCHEMA_RECORD_KEY_
#define SCHEMA_RECORD_KEY_

#include <vector>
#include <memory>

#include "DataTypes.h"

namespace Schema {

class RecordKey {
 public:
  RecordKey() = default;
  std::vector<std::shared_ptr<SchemaFieldType>>& fields() { return fields_; }
  const std::vector<std::shared_ptr<SchemaFieldType>>& fields() const {
    return fields_;
  }

  // Number of fields this key contains.
  int NumFields() const { return fields_.size(); }
  // Size of this key.

  // Add a new fiild. This method takes ownership of SchemaFieldType pointer.
  void AddField(SchemaFieldType* new_field);

  // Compare 2 Schema fields. We first compare field type and then the value
  // if the field types are same.
  static int CompareSchemaFields(const SchemaFieldType* field1,
                                 const SchemaFieldType* field2);

  // Overloading operators.
  bool operator<(const RecordKey& other) const;
  bool operator>(const RecordKey& other) const;
  bool operator<=(const RecordKey& other) const;
  bool operator>=(const RecordKey& other) const;
  bool operator==(const RecordKey& other) const;
  bool operator!=(const RecordKey& other) const;

 private:
  std::vector<std::shared_ptr<SchemaFieldType>> fields_;
};

}  // namespace RecordKey

#endif  /* SCHEMA_RECORD_KEY_ */
