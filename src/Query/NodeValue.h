#ifndef QUERY_NODE_VALUE_H_
#define QUERY_NODE_VALUE_H_

#include "Query/Common.h"

namespace Query {

// We use a different representation of value type enum other than
// DB::TableField::Type. This enum is specific for Sql query parser/evaluator.
//
// All integer types are treated as INT64, both string and char_array types are
// treated as STRING type.
enum ValueType {
  UNKNOWN_VALUE_TYPE,
  INT64,
  DOUBLE,
  STRING,
  CHAR,
  BOOL,
};

std::string ValueTypeStr(ValueType value_type);
// Convert a schema field type to value type.
ValueType FromSchemaType(Schema::FieldType field_type);

struct NodeValue {
  int64 v_int64 = 0;
  double v_double = 0;
  std::string v_str;
  char v_char = 0;
  bool v_bool = false;

  ValueType type = UNKNOWN_VALUE_TYPE;
  bool negative = false;

  bool has_value() const { return has_value_flags_ != 0; }
  byte has_value_flags_ = 0;

  NodeValue() : type(UNKNOWN_VALUE_TYPE) {}
  explicit NodeValue(ValueType type_arg) : type(type_arg) {}

  NodeValue static IntValue(int64 v);
  NodeValue static DoubleValue(double v);
  NodeValue static StringValue(const std::string& v);
  NodeValue static CharValue(char v);
  NodeValue static BoolValue(bool v);

  std::shared_ptr<Schema::Field>
  ToSchemaField(Schema::FieldType field_type) const;

  std::string AsString() const;

  bool operator==(const NodeValue& other) const;
  bool operator!=(const NodeValue& other) const;
  bool operator<(const NodeValue& other) const;
  bool operator>(const NodeValue& other) const;
  bool operator<=(const NodeValue& other) const;
  bool operator>=(const NodeValue& other) const;
};

NodeValue operator+(const NodeValue& lhs, const NodeValue& rhs);
NodeValue operator-(const NodeValue& lhs, const NodeValue& rhs);
NodeValue operator*(const NodeValue& lhs, const NodeValue& rhs);
NodeValue operator/(const NodeValue& lhs, const NodeValue& rhs);
NodeValue operator%(const NodeValue& lhs, const NodeValue& rhs);
bool operator&&(const NodeValue& lhs, const NodeValue& rhs);
bool operator||(const NodeValue& lhs, const NodeValue& rhs);
bool operator!(const NodeValue& lhs);

}  // namespace Query

#endif  // QUERY_NODE_VALUE_H_
