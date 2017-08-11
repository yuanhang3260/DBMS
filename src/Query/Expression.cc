#include "Base/MacroUtils.h"
#include "Strings/Split.h"
#include "Strings/Utils.h"

#include "Query/Expression.h"

namespace Query {

// **************************** ExprTreeNode ******************************** //
std::string ExprTreeNode::NodeTypeStr(ExprTreeNode::Type node_type) {
  switch (node_type) {
    case CONST_VALUE:
      return "CONST_VALUE";
    case TABLE_COLUMN:
      return "TABLE_COLUMN";
    case OPERATOR:
      return "OPERATOR";
    case UNKNOWN_NODE_TYPE:
      return "UNKNOWN_NODE_TYPE";
  }
  return "UNKNOWN_NODE_TYPE";
}

void ExprTreeNode::set_negative(bool neg) {
  if (value_.type == STRING || value_.type == UNKNOWN_VALUE_TYPE) {
    return;
  }

  if (type() == ExprTreeNode::CONST_VALUE) {
    switch (value_.type) {
      case INT64:
        value_.v_int64 = -value_.v_int64;
        break;
      case DOUBLE:
        value_.v_double = -value_.v_double;
        break;
      case CHAR:
        value_.v_char = -value_.v_char;
        break;
      default:
        break;
    }
  } else {
    value_.negative = neg;
  }
}

// ************************** ConstValueNode ******************************** //
void ConstValueNode::Print() const {
  printf("const value node %s\n", value_.AsString().c_str());
}

NodeValue ConstValueNode::Evaluate(const FetchedResult::Tuple& arg) const {
  return value_;
}


// **************************** ColumnNode ********************************** //
ColumnNode::ColumnNode(const Column& column) :
    column_(column) {
  if (column_.table_name.empty()) {
    valid_ = false;
    error_msg_ = Strings::StrCat("Ambiguous column \"", column_.column_name,
                                 "\", table name needed.");
    return;
  }
  Init();
}

void ColumnNode::Print() const {
  printf("column node (%s, %s)\n",
         column_.table_name.c_str(), column_.column_name.c_str());
}

bool ColumnNode::Init() {
  if (!valid_) {
    return false;
  }

  // Get value type of this ColumnNode, from the schema field type.
  value_.type = FromSchemaType(column_.type);
  valid_ = true;
  return true;
}

NodeValue ColumnNode::Evaluate(const FetchedResult::Tuple& tuple) const {
  // Make sure Init() has run successfully.
  CHECK(valid_, "Invalid ColumnNode");

  CHECK(column_.index >= 0, "field index is not initialized");
  CHECK(column_.type != Schema::FieldType::UNKNOWN_TYPE,
        "field type is not initialized");

  NodeValue value = value_;

  // Find the field from record.
  const auto& it = tuple.find(column_.table_name);
  CHECK(it != tuple.end(),
        Strings::StrCat("Couldn't find record of table ", column_.table_name,
                        " from the given tuple"));

  const auto& table_record = it->second;
  const Schema::Field* field = table_record.GetField(column_.index);

  // Get value from field.
  switch (column_.type) {
    case Schema::FieldType::INT:
      CHECK(field->type() == Schema::FieldType::INT,
            Strings::StrCat("Field type INT mismatch with actual: ",
                            Schema::FieldTypeStr(field->type())));
      // This check is optional - just want to make sure FromSchemaType()
      // works correctly.
      CHECK(value.type == INT64,
            Strings::StrCat("Field type INT mismatch with "
                            "ColumnNode value type ",
                            ValueTypeStr(value_.type)));
      value.v_int64 = dynamic_cast<const Schema::IntField*>(field)->value();
      if (value.negative) {
        value.v_int64 = -value.v_int64;
      }
      break;
    case Schema::FieldType::LONGINT:
      CHECK(field->type() == Schema::FieldType::LONGINT,
            Strings::StrCat("Field type LONGINT mismatch with actual: ",
                            Schema::FieldTypeStr(field->type())));
      CHECK(value.type == INT64,
            Strings::StrCat("Field type LONGINT mismatch with "
                            "ColumnNode value type ",
                            ValueTypeStr(value.type)));
      value.v_int64 =dynamic_cast<const Schema::LongIntField*>(field)->value();
      if (value.negative) {
        value.v_int64 = -value.v_int64;
      }
      break;
    case Schema::FieldType::DOUBLE:
      CHECK(field->type() == Schema::FieldType::DOUBLE,
            Strings::StrCat("Field type DOUBLE mismatch with actual: ",
                            Schema::FieldTypeStr(field->type())));
      CHECK(value.type == DOUBLE,
            Strings::StrCat("Field type DOUBLE mismatch with "
                            "ColumnNode value type ",
                            ValueTypeStr(value.type)));
      value.v_double =dynamic_cast<const Schema::DoubleField*>(field)->value();
      if (value.negative) {
        value.v_double = -value.v_double;
      }
      break;
    case Schema::FieldType::CHAR:
      CHECK(field->type() == Schema::FieldType::CHAR,
            Strings::StrCat("Field type CHAR mismatch with actual: ",
                            Schema::FieldTypeStr(field->type())));
      CHECK(value.type == CHAR,
            Strings::StrCat("Field type CHAR mismatch with "
                            "ColumnNode value type ",
                            ValueTypeStr(value.type)));
      value.v_char = dynamic_cast<const Schema::CharField*>(field)->value();
      if (value.negative) {
        value.v_char = -value.v_char;
      }
      break;
    case Schema::FieldType::BOOL:
      CHECK(field->type() == Schema::FieldType::BOOL,
            Strings::StrCat("Field type BOOL mismatch with actual: ",
                            Schema::FieldTypeStr(field->type())));
      CHECK(value.type == BOOL,
            Strings::StrCat("Field type BOOL mismatch with "
                            "ColumnNode value type ",
                            ValueTypeStr(value.type)));
      value.v_bool = dynamic_cast<const Schema::BoolField*>(field)->value();
      break;
    case Schema::FieldType::STRING:
      CHECK(field->type() == Schema::FieldType::STRING,
            Strings::StrCat("Field type STRING mismatch with actual: ",
                            Schema::FieldTypeStr(field->type())));
      CHECK(value.type == STRING,
            Strings::StrCat("Field type STRING mismatch with "
                            "ColumnNode value type ",
                            ValueTypeStr(value.type)));
      value.v_str = dynamic_cast<const Schema::StringField*>(field)->value();
      break;
    case Schema::FieldType::CHARARRAY:
      CHECK(field->type() == Schema::FieldType::CHARARRAY,
            Strings::StrCat("Field type CHARARRAY mismatch with actual: ",
                            Schema::FieldTypeStr(field->type())));
      CHECK(value.type == STRING,
            Strings::StrCat("Field type CHARARRAY mismatch with "
                            "ColumnNode value type ",
                            ValueTypeStr(value.type)));
      value.v_str =
          dynamic_cast<const Schema::CharArrayField*>(field)->AsString();
      break;
    default:
      break;
  }

  return value;
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
  if (op_ != NOT && (!left_ || !right_)) {
    error_msg_ = Strings::StrCat((!left_) ? "left " : "",
                                 (!right_) ? "right " : "",
                                 "operand missing for opeartor ",
                                 OpTypeStr(op_));
    valid_ = false;
    return false;
  }

  if (op_ == NOT && !left_) {
    error_msg_ = Strings::StrCat("operand missing for opeartor NOT");
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
  ValueType left_value_type = left_? left_->value().type : UNKNOWN_VALUE_TYPE;
  ValueType right_value_type =right_? right_->value().type : UNKNOWN_VALUE_TYPE;
  ValueType result_type = DeriveResultValueType(left_value_type,
                                                right_value_type);
  if (result_type == UNKNOWN_VALUE_TYPE) {
    valid_ = false;
    if (op_ != NOT) {
      error_msg_ = Strings::StrCat("Invalid operation: ",
                                   ValueTypeStr(left_->value().type), " ",
                                   OpTypeStr(op_), " ",
                                   ValueTypeStr(right_->value().type));
    } else {
      error_msg_ = Strings::StrCat("Invalid operation: NOT ",
                                   ValueTypeStr(left_->value().type));
    }
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
      if (types_match(INT64, INT64) ||
          types_match(INT64, DOUBLE) ||
          types_match(INT64, CHAR) ||
          types_match(DOUBLE, DOUBLE) ||
          types_match(BOOL, BOOL) ||
          types_match(STRING, STRING) ||
          types_match(CHAR, CHAR)) {
        return BOOL;
      } else {
        return UNKNOWN_VALUE_TYPE;
      }
    case LT:
    case GT:
    case LE:
    case GE:
      if (types_match(INT64, INT64) ||
          types_match(INT64, DOUBLE) ||
          types_match(INT64, CHAR) ||
          types_match(DOUBLE, DOUBLE) ||
          types_match(STRING, STRING) ||
          types_match(CHAR, CHAR)) {
        return BOOL;
      } else {
        return UNKNOWN_VALUE_TYPE;
      }
    case AND:
    case OR:
      if (types_match(BOOL, BOOL)) {
        return BOOL;
      }
    case NOT:
      if (t1 == BOOL) {
        return BOOL;
      }
    default:
      return UNKNOWN_VALUE_TYPE;
  }
  return UNKNOWN_VALUE_TYPE;
}

NodeValue OperatorNode::Evaluate(const FetchedResult::Tuple& tuple) const {
  CHECK(valid_, "Invalid OperatorNode");

  NodeValue left_value = left_? left_->Evaluate(tuple) : NodeValue();
  NodeValue right_value = right_? right_->Evaluate(tuple) : NodeValue();

  NodeValue value;
  switch (op_) {
    case ADD:
      value = left_value + right_value;
      break;
    case SUB:
      value = left_value - right_value;
      break;
    case MUL:
      value = left_value * right_value;
      break;
    case DIV:
      value = left_value / right_value;
      break;
    case MOD:
      value = left_value % right_value;
      break;
    case EQUAL:
      value = NodeValue::BoolValue(left_value == right_value);
      break;
    case NONEQUAL:
      value = NodeValue::BoolValue(left_value != right_value);
      break;
    case LT:
      value = NodeValue::BoolValue(left_value < right_value);
      break;
    case GT:
      value = NodeValue::BoolValue(left_value > right_value);
      break;
    case LE:
      value = NodeValue::BoolValue(left_value <= right_value);
      break;
    case GE:
      value = NodeValue::BoolValue(left_value >= right_value);
      break;
    case AND:
      value = NodeValue::BoolValue(left_value && right_value);
      break;
    case OR:
      value = NodeValue::BoolValue(left_value || right_value);
      break;
    case NOT:
      value = NodeValue::BoolValue(!left_value);
      break;
    default:
      LogFATAL("Invalid operator");
      break;
  }

  CHECK(value.has_value(),
        "Node value is not computed. Missing some type operation?");
  return value;
}

}  // namespace Query
