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

std::string OpTypeStr(OperatorType op_type) {
  switch (op_type) {
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
    case AND:
      return "AND";
    case OR:
      return "OR";
    case NOT:
      return "NOT";
    case UNKNOWN_OPERATOR:
      return "UNKNOWN_OPERATOR";
  }
  return "UNKNOWN_OPERATOR";
}

ValueType FromSchemaType(Schema::FieldType field_type) {
  switch (field_type) {
    case Schema::FieldType::INT:
    case Schema::FieldType::LONGINT:
      return INT64;
    case Schema::FieldType::DOUBLE:
      return DOUBLE;
    case Schema::FieldType::BOOL:
      return BOOL;
    case Schema::FieldType::CHAR:
      return CHAR;
    case Schema::FieldType::STRING:
    case Schema::FieldType::CHARARRAY:
      return STRING;
    default:
      return UNKNOWN_VALUE_TYPE;
  }
  return UNKNOWN_VALUE_TYPE;
}

OperatorType StrToOp(const std::string& str) {
  if (str == "+") {
    return ADD;
  }
  if (str == "-") {
    return SUB;
  }
  if (str == "*") {
    return MUL;
  }
  if (str == "/") {
    return DIV;
  }
  if (str == "%%") {
    return MOD;
  }
  if (str == "=") {
    return EQUAL;
  }
  if (str == "!=") {
    return NONEQUAL;
  }
  if (str == "<") {
    return LT;
  }
  if (str == ">") {
    return GT;
  }
  if (str == "<=") {
    return LE;
  }
  if (str == ">=") {
    return GE;
  }

  return UNKNOWN_OPERATOR;
}

bool IsNumerateOp(OperatorType op_type) {
  if (op_type == ADD || op_type == SUB ||
      op_type == MUL || op_type == DIV || op_type == MOD) {
    return true;
  }
  return false;
}

bool IsCompareOp(OperatorType op_type) {
  if (op_type == EQUAL || op_type == NONEQUAL ||
      op_type == LT || op_type == GT ||
      op_type == LE || op_type == GE) {
    return true;
  }
  return false;
}

bool IsLogicalOp(OperatorType op_type) {
  if (op_type == AND || op_type == OR || op_type == NOT) {
    return true;
  }
  return false;
}

}  // namespace Query
