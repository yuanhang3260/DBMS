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

}  // Schema
