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
    case UNKNOWN_NODE_TYPE:
      return "UNKNOWN_NODE_TYPE";
  }
  return "UNKNOWN_NODE_TYPE";
}

void ExprTreeNode::set_negative(bool neg) {
  if (value_.type == STRING || value_.type == UNKNOWN_VALUE_TYPE) {
    return;
  }
  value_.negative = neg;
}


// ************************** ConstValueNode ******************************** //
void ConstValueNode::Print() const {
  printf("const value node %s\n", value_.AsString().c_str());
}


// **************************** ColumnNode ********************************** //
ColumnNode::ColumnNode(const Column& column, const DB::TableField& field) :
    column_(column) {
  if (column_.table_name.empty()) {
    valid_ = false;
    error_msg_ = Strings::StrCat("Ambiguous column \"", column_.column_name,
                                 "\", table name needed.");
    return;
  }
  Init(field);
}

void ColumnNode::Print() const {
  printf("column node (%s, %s)\n",
         column_.table_name.c_str(), column_.column_name.c_str());
}

bool ColumnNode::Init(const DB::TableField& field) {
  if (!valid_) {
    return false;
  }

  // Get value type of this ColumnNode, from the schema field type.
  value_.type = FromSchemaType(field.type());
  field_index_ = field.index();
  field_type_ = field.type();
  valid_ = true;
  return true;
}

NodeValue ColumnNode::Evaluate(const EvaluateArgs& arg) const {
  // Make sure Init() has run successfully.
  SANITY_CHECK(valid_, "Invalid ColumnNode");

  SANITY_CHECK(field_index_ >= 0, "field index is not initialized");
  SANITY_CHECK(field_type_ != Schema::FieldType::UNKNOWN_TYPE,
               "field type is not initialized");

  NodeValue value = value_;

  // Find the field from record.
  const auto& fields = arg.record.fields();
  const Schema::Field* field = nullptr;
  if (arg.record_type == Storage::DATA_RECORD) {
    field = fields.at(field_index_).get();
  } else if (arg.record_type == Storage::INDEX_RECORD) {
    int32 pos = -1;
    for (uint32 i = 0 ; i < arg.field_indexes.size(); i++) {
      if (arg.field_indexes.at(i) == field_index_) {
        pos = i;
        break;
      }
    }
    // Check if the field is in the index record. This should be assured by
    // the expression evaluator system. It should never pass in index records
    // that don't contain all table column fields that the expression requires.
    SANITY_CHECK(pos > 0, Strings::StrCat("Can't find required field index ",
                                          std::to_string(field_index_),
                                          " for this index record"));
    field = fields.at(pos).get();
  } else {
    // Never should pass in record types other than DATA_RECORD/INDEX_RECORD.
    LogFATAL("Invalid record type to evalute: %s",
             Storage::RecordTypeStr(arg.record_type).c_str());
  }

  // Get value from field.
  switch (field_type_) {
    case Schema::FieldType::INT:
      SANITY_CHECK(field->type() == Schema::FieldType::INT,
                   Strings::StrCat("Field type INT mismatch with actual: ",
                                   Schema::FieldTypeStr(field->type())));
      // This check is optional - just want to make sure FromSchemaType()
      // works correctly.
      SANITY_CHECK(value.type == INT64,
                   Strings::StrCat("Field type INT mismatch with "
                                   "ColumnNode value type ",
                                   ValueTypeStr(value_.type)));
      value.v_int64 = dynamic_cast<const Schema::IntField*>(field)->value();
      if (value.negative) {
        value.v_int64 = -value.v_int64;
      }
      break;
    case Schema::FieldType::LONGINT:
      SANITY_CHECK(field->type() == Schema::FieldType::LONGINT,
                   Strings::StrCat("Field type LONGINT mismatch with actual: ",
                                   Schema::FieldTypeStr(field->type())));
      SANITY_CHECK(value.type == INT64,
                   Strings::StrCat("Field type LONGINT mismatch with "
                                   "ColumnNode value type ",
                                   ValueTypeStr(value.type)));
      value.v_int64 =dynamic_cast<const Schema::LongIntField*>(field)->value();
      if (value.negative) {
        value.v_int64 = -value.v_int64;
      }
      break;
    case Schema::FieldType::DOUBLE:
      SANITY_CHECK(field->type() == Schema::FieldType::DOUBLE,
                   Strings::StrCat("Field type DOUBLE mismatch with actual: ",
                                   Schema::FieldTypeStr(field->type())));
      SANITY_CHECK(value.type == DOUBLE,
                   Strings::StrCat("Field type DOUBLE mismatch with "
                                   "ColumnNode value type ",
                                   ValueTypeStr(value.type)));
      value.v_double =dynamic_cast<const Schema::DoubleField*>(field)->value();
      if (value.negative) {
        value.v_double = -value.v_double;
      }
      break;
    case Schema::FieldType::CHAR:
      SANITY_CHECK(field->type() == Schema::FieldType::CHAR,
                   Strings::StrCat("Field type CHAR mismatch with actual: ",
                                   Schema::FieldTypeStr(field->type())));
      SANITY_CHECK(value.type == CHAR,
                   Strings::StrCat("Field type CHAR mismatch with "
                                   "ColumnNode value type ",
                                   ValueTypeStr(value.type)));
      value.v_char = dynamic_cast<const Schema::CharField*>(field)->value();
      if (value.negative) {
        value.v_char = -value.v_char;
      }
      break;
    case Schema::FieldType::BOOL:
      SANITY_CHECK(field->type() == Schema::FieldType::BOOL,
                   Strings::StrCat("Field type BOOL mismatch with actual: ",
                                   Schema::FieldTypeStr(field->type())));
      SANITY_CHECK(value.type == BOOL,
                   Strings::StrCat("Field type BOOL mismatch with "
                                   "ColumnNode value type ",
                                   ValueTypeStr(value.type)));
      value.v_bool = dynamic_cast<const Schema::BoolField*>(field)->value();
      if (value.negative) {
        value.v_bool = -value.v_bool;
      }
      break;
    case Schema::FieldType::STRING:
      SANITY_CHECK(field->type() == Schema::FieldType::STRING,
                   Strings::StrCat("Field type STRING mismatch with actual: ",
                                   Schema::FieldTypeStr(field->type())));
      SANITY_CHECK(value.type == STRING,
                   Strings::StrCat("Field type STRING mismatch with "
                                   "ColumnNode value type ",
                                   ValueTypeStr(value.type)));
      value.v_str = dynamic_cast<const Schema::StringField*>(field)->value();
      break;
    case Schema::FieldType::CHARARRAY:
      SANITY_CHECK(field->type() == Schema::FieldType::CHARARRAY,
                   Strings::StrCat("Field type CHARARRAY mismatch with actual:",
                                   " ", Schema::FieldTypeStr(field->type())));
      SANITY_CHECK(value.type == STRING,
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

// This is silly, but I can't find a better way to eliminate duplicated code.
#define PRODUCE_RESULT_VALUE_1(t1, t2, t_result, t1_name, t2_name, t_result_name, op)    \
    if (types_match(t1, t2)) {                                                           \
      value.type = t_result;                                                             \
      value.has_value_flags_ = 1;                                                        \
      value.v_##t_result_name =                                                          \
        ((left_value.negative? -left_value.v_##t1_name : left_value.v_##t1_name) op      \
         (right_value.negative? -right_value.v_##t2_name : right_value.v_##t2_name));    \
    }                                                                                    \

#define PRODUCE_RESULT_VALUE_2(t1, t2, t_result, t1_name, t2_name, t_result_name, op)    \
    PRODUCE_RESULT_VALUE_1(t1, t2, t_result, t1_name, t2_name, t_result_name, op)        \
    if (types_match(t2, t1)) {                                                           \
      value.type = t_result;                                                             \
      value.has_value_flags_ = 1;                                                        \
      value.v_##t_result_name =                                                          \
        ((left_value.negative? -left_value.v_##t2_name : left_value.v_##t2_name) op      \
         (right_value.negative? -right_value.v_##t1_name : right_value.v_##t1_name));    \
    }                                                                                    \

#define PRODUCE_STRING_OP_RESULT(op)                            \
    if (types_match(STRING, STRING)) {                          \
      value.type = BOOL;                                        \
      value.has_value_flags_ = 1;                               \
      value.v_bool = left_value.v_str op right_value.v_str;     \
    }                                                           \

NodeValue OperatorNode::Evaluate(const EvaluateArgs& arg) const {
  SANITY_CHECK(valid_, "Invalid OperatorNode");

  NodeValue left_value = left_? left_->Evaluate(arg) : NodeValue();
  NodeValue right_value = right_? right_->Evaluate(arg) : NodeValue();
  NodeValue value = value_;

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
      //   value.type = INT64;
      //   value.has_value_flags_ = 1;
      //   value.v_int64 = left_value.v_int64 + right_value.v_int64;
      // }
      PRODUCE_RESULT_VALUE_1(INT64, INT64, INT64, int64, int64, int64, +)
      PRODUCE_RESULT_VALUE_2(INT64, DOUBLE, DOUBLE, int64, double, double, +)
      PRODUCE_RESULT_VALUE_2(INT64, CHAR, CHAR, int64, char, char, +)
      PRODUCE_RESULT_VALUE_2(INT64, BOOL, INT64, int64, bool, int64, +)
      PRODUCE_RESULT_VALUE_1(DOUBLE, DOUBLE, DOUBLE, double, double, double, +)
      PRODUCE_RESULT_VALUE_1(CHAR, CHAR, CHAR, char, char, char, +)
      break;
    case SUB:
      PRODUCE_RESULT_VALUE_1(INT64, INT64, INT64, int64, int64, int64, -)
      PRODUCE_RESULT_VALUE_2(INT64, DOUBLE, DOUBLE, int64, double, double, -)
      PRODUCE_RESULT_VALUE_2(INT64, CHAR, CHAR, int64, char, char, -)
      PRODUCE_RESULT_VALUE_2(INT64, BOOL, INT64, int64, bool, int64, -)
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
    case AND:
      PRODUCE_RESULT_VALUE_1(BOOL, BOOL, BOOL, bool, bool, bool, &&)
      break;
    case OR:
      PRODUCE_RESULT_VALUE_1(BOOL, BOOL, BOOL, bool, bool, bool, ||)
      break;
    case NOT:
      if (left_value.type == BOOL) {
        value.type = BOOL;
        value.has_value_flags_ = 1;
        value.v_bool = !left_value.v_bool;
      }
      break;
    default:
      LogFATAL("Invalid operator");
      break;
  }

  SANITY_CHECK(value.has_value(),
               "Node value is not computed. Missing some type operation?");
  return value;
}

}  // namespace Query
