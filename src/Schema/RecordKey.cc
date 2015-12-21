#include <stdexcept>

#include "Base/Utils.h"
#include "RecordKey.h"

namespace Schema {

void RecordKey::AddField(SchemaFieldType* new_field) {
  fields_.push_back(std::shared_ptr<SchemaFieldType>(new_field));
}

bool RecordKey::operator<(const RecordKey& other) const {
  const auto& other_fields = other.fields();
  int len = Utils::Min(fields_.size(), other_fields.size());
  for (int i = 0; i < len; i++) {
    int re = RecordKey::CompareSchemaFields(
                 fields_.at(i).get(), other_fields.at(i).get());
    if (re < 0) {
      return true;
    }
    else if (re > 0){
      return false;
    }
  }
  return fields_.size() < other_fields.size();
}

bool RecordKey::operator>(const RecordKey& other) const {
  const auto& other_fields = other.fields();
  int len = Utils::Min(fields_.size(), other_fields.size());
  for (int i = 0; i < len; i++) {
    int re = RecordKey::CompareSchemaFields(
                 fields_.at(i).get(), other_fields.at(i).get());
    if (re > 0) {
      return true;
    }
    else if (re < 0){
      return false;
    }
  }
  return fields_.size() > other_fields.size();
}

bool RecordKey::operator<=(const RecordKey& other) const {
  return !(*this > other);
}

bool RecordKey::operator>=(const RecordKey& other) const {
  return !(*this < other);
}

bool RecordKey::operator==(const RecordKey& other) const {
  uint32 len = fields_.size();
  if (len != other.fields_.size()) {
    return false;
  }

  const auto& other_fields = other.fields();
  for (uint32 i = 0; i < len; i++) {
    int re = RecordKey::CompareSchemaFields(
                 fields_.at(i).get(), other_fields.at(i).get());
    if (re != 0) {
      return false;
    }
  }
  return true;
}

bool RecordKey::operator!=(const RecordKey& other) const {
  return !(*this == other);
}

#define COMPARE_FIELDS_WITH_TYPE(TYPE, FIELD1, FIELD2)  \
  const TYPE& f1 = *reinterpret_cast<const TYPE*>(FIELD1);  \
  const TYPE& f2 = *reinterpret_cast<const TYPE*>(FIELD2);  \
  if (f1 < f2) {                                      \
    return -1;                                        \
  }                                                   \
  if (f1 > f2) {                                      \
    return 1;                                         \
  }                                                   \
  return 0;                                           \

int RecordKey::CompareSchemaFields(const SchemaFieldType* field1,
                                   const SchemaFieldType* field2) {
  if (!field1 && !field2) {
    return 0;
  }
  if (!field1) {
    return -1;
  }
  if (!field2) {
    return 1;
  }

  auto type = field1->type();
  if (type != field2->type()) {
    return type - field2->type();
  }

  if (type == INT) {
    COMPARE_FIELDS_WITH_TYPE(IntType, field1, field2);
  }
  if (type == LONGINT) {
    COMPARE_FIELDS_WITH_TYPE(LongIntType, field1, field2);
  }
  if (type == DOUBLE) {
    COMPARE_FIELDS_WITH_TYPE(DoubleType, field1, field2);
  }
  if (type == BOOL) {
    COMPARE_FIELDS_WITH_TYPE(BoolType, field1, field2);
  }
  if (type == STRING) {
    COMPARE_FIELDS_WITH_TYPE(StringType, field1, field2);
  }
  if (type == CHARARRAY) {
    COMPARE_FIELDS_WITH_TYPE(CharArrayType, field1, field2);
  }

  throw std::runtime_error("Compare Schema Fields - Should NOT Reach Here.");
  return 0;
}

}  // namespace Schema
