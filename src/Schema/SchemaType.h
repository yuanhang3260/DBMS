#ifndef SCHEMA_TYPES_
#define SCHEMA_TYPES_

#include "Base/MacroUtils.h"

namespace Schema {

enum FieldType {
  INT,
  CHAR,
  STRING,
  DOUBLE,
  BOOL,
};

class SchemaField {
 public:
  SchemaField(FieldType type, int length) : type_(type), length_(length) {}

  // Accessors
  DEFINE_ACCESSOR_ENUM(type, FieldType);
  DEFINE_ACCESSOR(length, int);

 private:
  FieldType type_;
  int length_;
};

}  // Schema

#endif  /* SCHEMA_TYPES_ */
