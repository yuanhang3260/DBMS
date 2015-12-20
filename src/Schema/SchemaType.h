#ifndef SCHEMA_TYPES_
#define SCHEMA_TYPES_

#include "Base/BaseTypes.h"
#include "Base/MacroUtils.h"

namespace Schema {

enum FieldType {
  INT,
  LONGINT,
  DOUBLE,
  BOOL,
  STRING,
  CHARARRAY,
};

class SchemaFieldType {
 public:
  SchemaFieldType() = default;

  virtual FieldType type() = 0;
  virtual int length() = 0;
  virtual int DumpToMem(byte* buf) const = 0;

 protected:
};

}  // Schema

#endif  /* SCHEMA_TYPES_ */
