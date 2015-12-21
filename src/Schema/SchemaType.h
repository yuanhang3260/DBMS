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

  virtual FieldType type() const = 0;
  virtual int length() const = 0;
  virtual int DumpToMem(byte* buf) const = 0;
  virtual int LoadFromMem(const byte* buf) = 0;

 protected:
};

}  // Schema

#endif  /* SCHEMA_TYPES_ */
