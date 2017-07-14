#include "Query/Common.h"

namespace Query {

std::string ValueTypeStr(ValueType value_type) {
  switch (value_type) {
    case INT64:
      return "INT64";
    case DOUBLE:
      return "DOUBLE";
    case STRING:
      return "STRING";
    case CHAR:
      return "CHAR";
    case BOOL:
      return "BOOL";
    case UNKNOWN_VALUE_TYPE:
      return "UNKNOWN_VALUE_TYPE";
  }
  return "UNKNOWN_VALUE_TYPE";
}

std::string OpTypeStr(OperatorType value_type) {
  switch (value_type) {
    case ADD:
      return "+";
    case SUB:
      return "-";
    case MUL:
      return "*";
    case DIV:
      return "/";
    case MOD:
      return "%%";
    case EQUAL:
      return "=";
    case NONEQUAL:
      return "!=";
    case LT:
      return "<";
    case GT:
      return ">";
    case LE:
      return "<=";
    case GE:
      return ">=";
    case UNKNOWN_OPERATOR:
      return "UNKNOWN_OPERATOR";
  }
  return "UNKNOWN_OPERATOR";
}

}  // namespace Query
