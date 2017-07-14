#ifndef QUERY_COMMON_
#define QUERY_COMMON_

#include <string>

namespace Query {

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

}  // namespace Query

#endif  // QUERY_COMMON_
