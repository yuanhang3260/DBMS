#include "Base/MacroUtils.h"
#include "Strings/Split.h"
#include "Strings/Utils.h"

#include "Query/Expression.h"

namespace Query {

// ***************************** NodeValue ********************************** //
NodeValue NodeValue::IntValue(int64 v) {
  NodeValue value(INT64);
  value.v_int64 = v;
  value.has_value_flags_ = 1;
  return value;
}

NodeValue NodeValue::DoubleValue(double v) {
  NodeValue value(DOUBLE);
  value.v_double = v;
  value.has_value_flags_ = 1;
  return value;
}

NodeValue NodeValue::StringValue(const std::string& v) {
  NodeValue value(STRING);
  value.v_str = v;
  value.has_value_flags_ = 1;
  return value;
}

NodeValue NodeValue::CharValue(char v) {
  NodeValue value(CHAR);
  value.v_char = v;
  value.has_value_flags_ = 1;
  return value;
}

NodeValue NodeValue::BoolValue(bool v) {
  NodeValue value(BOOL);
  value.v_bool = v;
  value.has_value_flags_ = 1;
  return value;
}

std::string NodeValue::AsString() const {
  switch (type) {
    case INT64:
      return Strings::StrCat("Int64 ", std::to_string(v_int64));
    case DOUBLE:
      return Strings::StrCat("Double ", std::to_string(v_double));
    case STRING:
      return Strings::StrCat("String ", v_str);
    case CHAR:
      return Strings::StrCat("Char ", std::to_string(v_char));
    case BOOL:
      return Strings::StrCat("Bool ", v_bool ? "true" : "false");
    case UNKNOWN_VALUE_TYPE:
      return "Unknown type";
  }
  return "Unknown type";
}


// **************************** ExprTreeNode ******************************** //
std::string ExprTreeNode::NodeTypeStr(ExprTreeNode::Type node_type) {
  switch (node_type) {
    case CONST_VALUE:
      return "CONST_VALUE";
    case TABLE_COLUMN:
      return "TABLE_COLUMN";
    case OPERATOR:
      return "OPERATOR";
    case LOGICAL:
      return "LOGICAL";
    case UNKNOWN_NODE_TYPE:
      return "UNKNOWN_NODE_TYPE";
  }
  return "UNKNOWN_NODE_TYPE";
}

void ExprTreeNode::set_negative(bool neg) {
  if (value_.type == STRING || value_.type == UNKNOWN_VALUE_TYPE) {
    LogERROR("Can use negative sign on type %s", ValueTypeStr(value_.type));
    return;
  }
  value_.negative = neg;
}


// ************************** ConstValueNode ******************************** //
void ConstValueNode::Print() const {
  printf("const value node %s\n", value_.AsString().c_str());
}


// **************************** ColumnNode ********************************** //
ColumnNode::ColumnNode(const std::string& name) {
  auto re = Strings::Split(name, ".");
  if (re.size() == 1) {
    column_ = re.at(0);
  } else if (re.size() == 2) {
    table_ = re.at(0);
    column_ = re.at(1);
  } else {
    LogERROR("Invalid column name %s", name.c_str());
    valid_ = false;
  }
}

void ColumnNode::Print() const {
  printf("column node (%s, %s)\n", table_.c_str(), column_.c_str());
}


// *************************** OperatorNode ********************************* //
OperatorNode::OperatorNode(OperatorType op,
                           std::shared_ptr<ExprTreeNode> left,
                           std::shared_ptr<ExprTreeNode> right) :
    ExprTreeNode(left, right),
    op_(op) {
  Init();
}

void OperatorNode::Print() const {
  printf("operator %s node\n", OpTypeStr(op_).c_str());
}

bool OperatorNode::Init() {
  // Check left and right children exist.
  if (!left_ || !right_) {
    error_msg_ = Strings::StrCat((!left_) ? "left " : "",
                                 (!right_) ? "right " : "",
                                 "operand missing for opeartor ",
                                 OpTypeStr(op_));
    valid_ = false;
    return false;
  }

  // Check left and right chilren are valid.
  if (left_ && !left_->valid()) {
    valid_ = false;
    error_msg_ = Strings::StrCat("Invalid left node - ", left_->error_msg());
    return false;
  }

  if (right_ && !right_->valid()) {
    valid_ = false;
    error_msg_ = Strings::StrCat("Invalid right node - ", right_->error_msg());
    return false;
  }

  // Check left and right children node type are valid for OpeartorNode.
  if (left_ &&
      left_->type() != CONST_VALUE &&
      left_->type() != TABLE_COLUMN &&
      left_->type() != OPERATOR) {
    valid_ = false;
    error_msg_ = Strings::StrCat("Invalid left operand type ",
                                 NodeTypeStr(left_->type()),
                                 "for opeartor ", OpTypeStr(op_));
    return false;
  }

  if (right_ &&
      right_->type() != CONST_VALUE &&
      right_->type() != TABLE_COLUMN &&
      right_->type() != OPERATOR) {
    valid_ = false;
    error_msg_ = Strings::StrCat("Invalid right operand type ",
                                 NodeTypeStr(right_->type()),
                                 "for opeartor ", OpTypeStr(op_));
    return false;
  }

  // Check left and right children value type and try to derive result value
  // type. If failed, set error msg.
  ValueType result_type = DeriveResultValueType(left_->value().type,
                                                      right_->value().type);
  if (result_type == UNKNOWN_VALUE_TYPE) {
    valid_ = false;
    error_msg_ = Strings::StrCat("Invalid operation: ",
                                 ValueTypeStr(left_->value().type), " ",
                                 OpTypeStr(op_), " ",
                                 ValueTypeStr(right_->value().type));
    return false;
  }
  set_value_type(result_type);

  // Done.
  valid_ = true;
  return true;
}

ValueType OperatorNode::DeriveResultValueType(ValueType t1, ValueType t2) {
  auto types_match = [&] (ValueType type_1, ValueType type_2) {
    if ((t1 == type_1 && t2 == type_2) || (t1 == type_2 && t2 == type_1)) {
      return true;
    }
    return false;
  };

  switch (op_) {
    case ADD:
    case SUB:
      if (types_match(INT64, INT64)) {
        return INT64;
      } else if (types_match(INT64, DOUBLE)) {
        return DOUBLE;
      } else if (types_match(INT64, CHAR)) {
        return CHAR;
      } else if (types_match(INT64, BOOL)) {
        return INT64;
      } else if (types_match(DOUBLE, DOUBLE)) {
        return DOUBLE;
      } else if (types_match(CHAR, CHAR)) {
        return CHAR;
      } else {
        return UNKNOWN_VALUE_TYPE;
      }
      break;
    case MUL:
    case DIV:
      if (types_match(INT64, INT64)) {
        return INT64;
      } else if (types_match(INT64, DOUBLE)) {
        return DOUBLE;
      } else if (types_match(DOUBLE, DOUBLE)) {
        return DOUBLE;
      } else {
        return UNKNOWN_VALUE_TYPE;
      }
    case MOD:
      if (types_match(INT64, INT64)) {
        return INT64;
      } else {
        return UNKNOWN_VALUE_TYPE;
      }
    case EQUAL:
    case NONEQUAL:
    case LT:
    case GT:
    case LE:
    case GE:
      if (types_match(INT64, INT64) ||
          types_match(INT64, DOUBLE) ||
          types_match(INT64, CHAR) ||
          types_match(INT64, BOOL) ||
          types_match(DOUBLE, DOUBLE) ||
          types_match(STRING, STRING) ||
          types_match(CHAR, CHAR)) {
        return BOOL;
      } else {
        return UNKNOWN_VALUE_TYPE;
      }
    default:
      return UNKNOWN_VALUE_TYPE;
  }
  return UNKNOWN_VALUE_TYPE;
}

// This is silly, but I can't find a better way to eliminate duplicated code.
#define PRODUCE_RESULT_VALUE_1(t1, t2, t_result, t1_name, t2_name, t_result_name, op)    \
    if (types_match(t1, t2)) {                                                           \
      value_.type = t_result;                                                            \
      value_.has_value_flags_ = 1;                                                       \
      value_.v_##t_result_name =                                                         \
        (left_value.negative? -left_value.v_##t1_name : left_value.v_##t1_name) op       \
        (right_value.negative? -right_value.v_##t2_name : right_value.v_##t2_name);      \
    }                                                                                    \

#define PRODUCE_RESULT_VALUE_2(t1, t2, t_result, t1_name, t2_name, t_result_name, op)    \
    PRODUCE_RESULT_VALUE_1(t1, t2, t_result, t1_name, t2_name, t_result_name, op)        \
    if (types_match(t2, t1)) {                                                           \
      value_.type = t_result;                                                            \
      value_.has_value_flags_ = 1;                                                       \
      value_.v_##t_result_name =                                                         \
        (left_value.negative? -left_value.v_##t2_name : left_value.v_##t2_name) op       \
        (right_value.negative? -right_value.v_##t1_name : right_value.v_##t1_name);      \
    }                                                                                    \

#define PRODUCE_STRING_OP_RESULT(op)                            \
    value_.type = BOOL;                                         \
    value_.has_value_flags_ = 1;                                \
    value_.v_bool = left_value.v_str op right_value.v_str;      \

NodeValue OperatorNode::Evaluate() {
  SANITY_CHECK(valid_, "Invalid node");
  SANITY_CHECK(left_ && right_, "left/right child missing for OperatorNode");

  NodeValue left_value = left_->Evaluate();
  NodeValue right_value = right_->Evaluate();
  SANITY_CHECK(left_value.type != UNKNOWN_VALUE_TYPE &&
               right_value.type != UNKNOWN_VALUE_TYPE,
               "Invalid child node value");

  auto types_match = [&] (ValueType type_1, ValueType type_2) {
    if (left_value.type == type_1 && right_value.type == type_2) {
      return true;
    }
    return false;
  };

  // Enjoy.
  switch (op_) {
    case ADD:
      // Example of this macro expansion:
      //
      // if (types_match(INT64, INT64)) {
      //   value_.type = INT64;
      //   value_.v_int64 = left_value.v_int64 + right_value.v_int64;
      // }
      PRODUCE_RESULT_VALUE_1(INT64, INT64, INT64, int64, int64, int64, +)
      PRODUCE_RESULT_VALUE_2(INT64, DOUBLE, DOUBLE, int64, double, double, +)
      PRODUCE_RESULT_VALUE_2(INT64, CHAR, CHAR, int64, char, char, +)
      PRODUCE_RESULT_VALUE_2(INT64, BOOL, BOOL, int64, bool, bool, +)
      PRODUCE_RESULT_VALUE_1(DOUBLE, DOUBLE, DOUBLE, double, double, double, +)
      PRODUCE_RESULT_VALUE_1(CHAR, CHAR, CHAR, char, char, char, +)
      break;
    case SUB:
      PRODUCE_RESULT_VALUE_1(INT64, INT64, INT64, int64, int64, int64, -)
      PRODUCE_RESULT_VALUE_2(INT64, DOUBLE, DOUBLE, int64, double, double, -)
      PRODUCE_RESULT_VALUE_2(INT64, CHAR, CHAR, int64, char, char, -)
      PRODUCE_RESULT_VALUE_2(INT64, BOOL, BOOL, int64, bool, bool, -)
      PRODUCE_RESULT_VALUE_1(DOUBLE, DOUBLE, DOUBLE, double, double, double, -)
      PRODUCE_RESULT_VALUE_1(CHAR, CHAR, CHAR, char, char, char, -)
      break;
    case MUL:
      PRODUCE_RESULT_VALUE_1(INT64, INT64, INT64, int64, int64, int64, *)
      PRODUCE_RESULT_VALUE_2(INT64, DOUBLE, DOUBLE, int64, double, double, *)
      PRODUCE_RESULT_VALUE_1(DOUBLE, DOUBLE, DOUBLE, double, double, double, *)
      break;
    case DIV:
      PRODUCE_RESULT_VALUE_1(INT64, INT64, INT64, int64, int64, int64, /)
      PRODUCE_RESULT_VALUE_2(INT64, DOUBLE, DOUBLE, int64, double, double, /)
      PRODUCE_RESULT_VALUE_1(DOUBLE, DOUBLE, DOUBLE, double, double, double, /)
      break;
    case MOD:
      PRODUCE_RESULT_VALUE_1(INT64, INT64, INT64, int64, int64, int64, %)
      break;
    case EQUAL:
      PRODUCE_RESULT_VALUE_1(INT64, INT64, BOOL, int64, int64, bool, ==)
      PRODUCE_RESULT_VALUE_2(INT64, DOUBLE, BOOL, int64, double, bool, ==)
      PRODUCE_RESULT_VALUE_2(INT64, CHAR, BOOL, int64, char, bool, ==)
      PRODUCE_RESULT_VALUE_2(INT64, BOOL, BOOL, int64, bool, bool, ==)
      PRODUCE_RESULT_VALUE_1(DOUBLE, DOUBLE, BOOL, double, double, bool, ==)
      PRODUCE_STRING_OP_RESULT(==)
      PRODUCE_RESULT_VALUE_1(CHAR, CHAR, BOOL, char, char, bool, ==)
      break;
    case NONEQUAL:
      PRODUCE_RESULT_VALUE_1(INT64, INT64, BOOL, int64, int64, bool, !=)
      PRODUCE_RESULT_VALUE_2(INT64, DOUBLE, BOOL, int64, double, bool, !=)
      PRODUCE_RESULT_VALUE_2(INT64, CHAR, BOOL, int64, char, bool, !=)
      PRODUCE_RESULT_VALUE_2(INT64, BOOL, BOOL, int64, bool, bool, !=)
      PRODUCE_RESULT_VALUE_1(DOUBLE, DOUBLE, BOOL, double, double, bool, !=)
      PRODUCE_STRING_OP_RESULT(!=)
      PRODUCE_RESULT_VALUE_1(CHAR, CHAR, BOOL, char, char, bool, !=)
      break;
    case LT:
      PRODUCE_RESULT_VALUE_1(INT64, INT64, BOOL, int64, int64, bool, <)
      PRODUCE_RESULT_VALUE_2(INT64, DOUBLE, BOOL, int64, double, bool, <)
      PRODUCE_RESULT_VALUE_2(INT64, CHAR, BOOL, int64, char, bool, <)
      PRODUCE_RESULT_VALUE_2(INT64, BOOL, BOOL, int64, bool, bool, <)
      PRODUCE_RESULT_VALUE_1(DOUBLE, DOUBLE, BOOL, double, double, bool, <)
      PRODUCE_STRING_OP_RESULT(<)
      PRODUCE_RESULT_VALUE_1(CHAR, CHAR, BOOL, char, char, bool, <)
      break;
    case GT:
      PRODUCE_RESULT_VALUE_1(INT64, INT64, BOOL, int64, int64, bool, >)
      PRODUCE_RESULT_VALUE_2(INT64, DOUBLE, BOOL, int64, double, bool, >)
      PRODUCE_RESULT_VALUE_2(INT64, CHAR, BOOL, int64, char, bool, >)
      PRODUCE_RESULT_VALUE_2(INT64, BOOL, BOOL, int64, bool, bool, >)
      PRODUCE_RESULT_VALUE_1(DOUBLE, DOUBLE, BOOL, double, double, bool, >)
      PRODUCE_STRING_OP_RESULT(>)
      PRODUCE_RESULT_VALUE_1(CHAR, CHAR, BOOL, char, char, bool, >)
      break;
    case LE:
      PRODUCE_RESULT_VALUE_1(INT64, INT64, BOOL, int64, int64, bool, <=)
      PRODUCE_RESULT_VALUE_2(INT64, DOUBLE, BOOL, int64, double, bool, <=)
      PRODUCE_RESULT_VALUE_2(INT64, CHAR, BOOL, int64, char, bool, <=)
      PRODUCE_RESULT_VALUE_2(INT64, BOOL, BOOL, int64, bool, bool, <=)
      PRODUCE_RESULT_VALUE_1(DOUBLE, DOUBLE, BOOL, double, double, bool, <=)
      PRODUCE_STRING_OP_RESULT(<=)
      PRODUCE_RESULT_VALUE_1(CHAR, CHAR, BOOL, char, char, bool, <=)
      break;
    case GE:
      PRODUCE_RESULT_VALUE_1(INT64, INT64, BOOL, int64, int64, bool, >=)
      PRODUCE_RESULT_VALUE_2(INT64, DOUBLE, BOOL, int64, double, bool, >=)
      PRODUCE_RESULT_VALUE_2(INT64, CHAR, BOOL, int64, char, bool, >=)
      PRODUCE_RESULT_VALUE_2(INT64, BOOL, BOOL, int64, bool, bool, >=)
      PRODUCE_RESULT_VALUE_1(DOUBLE, DOUBLE, BOOL, double, double, bool, >=)
      PRODUCE_STRING_OP_RESULT(>=)
      PRODUCE_RESULT_VALUE_1(CHAR, CHAR, BOOL, char, char, bool, >=)
      break;
    default:
      LogFATAL("Invalid operator");
      break;
  }

  SANITY_CHECK(value_.has_value(),
               "Node value is not computed. Missing some type operation?");
  return value_;
}

}  // namespace Query
