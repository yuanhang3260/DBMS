#ifndef SCHEMA_TYPES_
#define SCHEMA_TYPES_

#include <string>

#include "Database/Catalog_pb.h"
#include "Base/BaseTypes.h"
#include "Base/MacroUtils.h"

namespace Schema {

typedef DB::TableField::Type FieldType;
std::string FieldTypeStr(FieldType type);

class Field {
 public:
  Field() = default;
  virtual ~Field() {}

  virtual FieldType type() const = 0;
  virtual int length() const = 0;
  virtual int DumpToMem(byte* buf) const = 0;
  virtual int LoadFromMem(const byte* buf) = 0;

  virtual std::string AsString() const = 0;

  virtual void reset() = 0;

  virtual Field* Copy() const = 0;

  bool MatchesSchemaType(DB::TableField::Type type) const;

 protected:
};

}  // Schema

#endif  /* SCHEMA_TYPES_ */
