#include "Strings/Split.h"
#include "Strings/Utils.h"

#include "Query/Expression.h"

namespace Query {

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
    case UNKNOWN_TYPE:
      return "Unknown type";
  }
  return "Unknown type";
}

void ConstValueNode::Print() const {
  printf("const value node %s\n", value_.AsString().c_str());
}

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

OperatorNode::OperatorNode(char op,
                           std::shared_ptr<ExprTreeNode> left,
                           std::shared_ptr<ExprTreeNode> right) :
    ExprTreeNode(left, right),
    op_(op) {
  // TODO: Validity check:
  //   1. Child nodes must be ColumnNode/ConstValueNode/OperatorNodetype.
  //   2. Value type must be 
}

}  // namespace Query
