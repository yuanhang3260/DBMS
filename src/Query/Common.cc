#include "Strings/Utils.h"

#include "Query/Common.h"

namespace Query {

// **************************** OperatorType ******************************** //
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

OperatorType FlipOp(OperatorType op_type) {
  if (op_type == LT) {
    return GT;
  } else if (op_type == GT) {
    return LT;
  } else if (op_type == LE) {
    return GE;
  } else if (op_type == GE) {
    return LE;
  } else {
    return op_type;
  }
}


// ******************************** Column ********************************** //
std::string Column::DebugString() const {
  std::string result = Strings::StrCat("(", table_name, ", ",
                                       column_name, ", ",
                                       std::to_string(index), ")");
  return result;
}

std::string Column::AsString(bool use_table_prefix) const {
  std::string result = column_name;
  if (use_table_prefix) {
    result = Strings::StrCat(table_name, ".", column_name);
  }
  return result;
}

}  // namespace Query
