#include "SchemaType.h"

namespace Schema {

std::string Field::FieldTypeAsString(FieldType type) {
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

bool Field::MatchesSchemaType(DB::TableField::Type schema_type) const {
  if (type() == INT && schema_type != DB::TableField::INTEGER) {
    return false;
  }
  if (type() == LONGINT && schema_type != DB::TableField::LLONG) {
    return false;
  }
  if (type() == DOUBLE && schema_type != DB::TableField::DOUBLE) {
    return false;
  }
  if (type() == BOOL && schema_type != DB::TableField::BOOL) {
    return false;
  }
  if (type() == STRING && schema_type != DB::TableField::STRING) {
    return false;
  }
  if (type() == CHARARRAY && schema_type != DB::TableField::CHARARR) {
    return false;
  }

  return true;
}

}  // Schema
