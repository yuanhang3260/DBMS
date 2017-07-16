#ifndef QUERY_COMMON_
#define QUERY_COMMON_

#include <string>

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

enum OperatorType {
  UNKNOWN_OPERATOR,
  ADD,  // +
  SUB,  // -
  MUL,  // *
  DIV,  // /
  MOD,  // %
  EQUAL,     // =
  NONEQUAL,  // !=
  LT,  // <
  GT,  // >
  LE,  // <=
  GE,  // >=
};

std::string OpTypeStr(OperatorType value_type);
OperatorType StrToOp(const std::string& str);

}  // namespace Query

#endif  // QUERY_COMMON_
