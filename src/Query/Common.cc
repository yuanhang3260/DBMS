#include <math.h>

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

bool NodeValue::operator==(const NodeValue& other) const {
  if (type != other.type) {
    LogFATAL("Comparing with value type %s with %s",
             ValueTypeStr(type).c_str(), ValueTypeStr(other.type).c_str());
    return false;
  }

  switch (type) {
    case INT64 : return v_int64 == other.v_int64;
    case DOUBLE : return v_double == other.v_double;
    case BOOL : return v_bool == other.v_bool;
    case CHAR : return v_char == other.v_char;
    case STRING : return v_str == other.v_str;
    default: return false;
  }
  return false;
}

bool NodeValue::operator!=(const NodeValue& other) const {
  return !(*this == other);
}

bool NodeValue::operator<(const NodeValue& other) const {
  if (type != other.type) {
    LogFATAL("Comparing with value type %s with %s",
             ValueTypeStr(type).c_str(), ValueTypeStr(other.type).c_str());
  }

  switch (type) {
    case INT64 : return v_int64 < other.v_int64;
    case DOUBLE : return v_double < other.v_double;
    case BOOL : return v_bool < other.v_bool;
    case CHAR : return v_char < other.v_char;
    case STRING : return v_str < other.v_str;
    default: return false;
  }
  return false;
}

bool NodeValue::operator>(const NodeValue& other) const {
  if (type != other.type) {
    LogFATAL("Comparing with value type %s with %s",
             ValueTypeStr(type).c_str(), ValueTypeStr(other.type).c_str());
  }

  switch (type) {
    case INT64 : return v_int64 > other.v_int64;
    case DOUBLE : return v_double > other.v_double;
    case BOOL : return v_bool > other.v_bool;
    case CHAR : return v_char > other.v_char;
    case STRING : return v_str > other.v_str;
    default: return false;
  }
  return false;
}

bool NodeValue::operator<=(const NodeValue& other) const {
  return !(*this > other);
}

bool NodeValue::operator>=(const NodeValue& other) const {
  return !(*this < other);
}

void CastValueType(QueryCondition* condition) {
  auto type = FromSchemaType(condition->column.type);
  auto& value = condition->value;
  if (type == INT64) {
    if (value.type == DOUBLE) {
      // Careful, don't cast directly. Needs to check the op type.
      if (condition->op == EQUAL) {
        if (floor(value.v_double) == value.v_double) {
          condition->is_const = true;
          condition->const_result = false;
        } else {
          value.v_int64 = static_cast<int64>(value.v_double);
        }
      } else if (condition->op == NONEQUAL) {
        if (floor(value.v_double) == value.v_double) {
          condition->is_const = true;
          condition->const_result = true;
        } else {
          value.v_int64 = static_cast<int64>(value.v_double);
        }
      } else if (condition->op == LT) {
        if (floor(value.v_double) == value.v_double) {
          value.v_int64 = static_cast<int64>(value.v_double);
        } else {
          value.v_int64 = static_cast<int64>(value.v_double) + 1;
        }
      } else if (condition->op == LE) {
        value.v_int64 = static_cast<int64>(value.v_double);
      } else if (condition->op == GT) {
        value.v_int64 = static_cast<int64>(value.v_double);
      } else if (condition->op == GE) {
        if (floor(value.v_double) == value.v_double) {
          value.v_int64 = static_cast<int64>(value.v_double);
        } else {
          value.v_int64 = static_cast<int64>(value.v_double) + 1;
        }
      } 
    } else if (value.type == CHAR) {
      value.v_int64 = static_cast<int64>(value.v_char);
    } else if (value.type != INT64) {
      LogFATAL("Unexpected value conversion from %s to INT64",
               ValueTypeStr(value.type).c_str());
    }
    value.type = INT64;
  } else if (type == DOUBLE) {
    if (value.type == INT64) {
      value.v_double = static_cast<double>(value.v_int64);
    } else if (value.type != DOUBLE) {
      LogFATAL("Unexpected value conversion from %s to DOUBLE",
               ValueTypeStr(value.type).c_str());
    }
    value.type = DOUBLE;
  } else if (type == CHAR) {
    if (value.type == INT64) {
      value.v_char = static_cast<char>(value.v_int64);
    } else if (value.type != CHAR) {
      LogFATAL("Unexpected value conversion from %s to DOUBLE",
               ValueTypeStr(value.type).c_str());
    }
    value.type = CHAR;
  }
}

}  // namespace Query
