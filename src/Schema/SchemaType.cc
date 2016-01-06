#include "SchemaType.h"

namespace Schema {

std::string SchemaFieldType::FieldTypeAsString(FieldType type) {
  if (type == INT) {
    return "Int";
  }
  if (type == LONGINT) {
    return "LongInt";
  }
  if (type == DOUBLE) {
    return "Double";
  }
  if (type == BOOL) {
    return "Bool";
  }
  if (type == STRING) {
    return "String";
  }
  if (type == CHARARRAY) {
    return "CharArray";
  }
  return "Unknown";
}

bool SchemaFieldType::MatchesSchemaType(TableField::Type schema_type) const {
  if (type() == INT && schema_type != TableField::INTEGER) {
    return false;
  }
  if (type() == LONGINT && schema_type != TableField::LLONG) {
    return false;
  }
  if (type() == DOUBLE && schema_type != TableField::DOUBLE) {
    return false;
  }
  if (type() == BOOL && schema_type != TableField::BOOL) {
    return false;
  }
  if (type() == STRING && schema_type != TableField::STRING) {
    return false;
  }
  if (type() == CHARARRAY && schema_type != TableField::CHARARR) {
    return false;
  }

  return true;
}

}  // Schema
