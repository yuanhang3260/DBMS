#include "Strings/Split.h"
#include "Strings/Utils.h"

#include "Query/Expression.h"

namespace Query {

// ***************************** NodeValue ********************************** //
NodeValue NodeValue::IntValue(int64 v) {
  NodeValue value(INT64);
  value.v_int64 = v;
  return value;
}

NodeValue NodeValue::DoubleValue(double v) {
  NodeValue value(DOUBLE);
  value.v_double = v;
  return value;
}

NodeValue NodeValue::StringValue(const std::string& v) {
  NodeValue value(STRING);
  value.v_str = v;
  return value;
}

NodeValue NodeValue::CharValue(char v) {
  NodeValue value(CHAR);
  value.v_char = v;
  return value;
}

NodeValue NodeValue::BoolValue(bool v) {
  NodeValue value(BOOL);
  value.v_bool = v;
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
    default:
      return UNKNOWN_VALUE_TYPE;
  }
  return UNKNOWN_VALUE_TYPE;
}

}  // namespace Query
