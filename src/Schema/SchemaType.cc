#include "SchemaType.h"

namespace Schema {

std::string FieldTypeStr(FieldType type) {
  if (type == FieldType::INT) {
    return "Int";
  }
  if (type == FieldType::LONGINT) {
    return "Int64";
  }
  if (type == FieldType::DOUBLE) {
    return "Double";
  }
  if (type == FieldType::BOOL) {
    return "Bool";
  }
  if (type == FieldType::CHAR) {
    return "Char";
  }
  if (type == FieldType::STRING) {
    return "String";
  }
  if (type == FieldType::CHARARRAY) {
    return "CharArray";
  }
  return "Unknown Value Type";
}

bool Field::MatchesSchemaType(DB::TableField::Type schema_type) const {
  return type() == schema_type;
}

}  // Schema
