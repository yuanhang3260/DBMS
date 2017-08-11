#include "Query/NodeValue.h"

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


// ****************************** NodeValue ********************************* //
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

std::shared_ptr<Schema::Field>
NodeValue::ToSchemaField(Schema::FieldType field_type) const {
  if (field_type == Schema::FieldType::INT) {
    CHECK(type == INT64,
          Strings::StrCat("Couldn't convert ", ValueTypeStr(type),
          " to IntField"));
    return std::shared_ptr<Schema::Field>(new Schema::IntField(v_int64));
  } else if (field_type == Schema::FieldType::LONGINT) {
    CHECK(type == INT64,
          Strings::StrCat("Couldn't convert ", ValueTypeStr(type),
          " to LongIntField"));
    return std::shared_ptr<Schema::Field>(new Schema::LongIntField(v_int64));
  } else if (field_type == Schema::FieldType::DOUBLE) {
    CHECK(type == DOUBLE,
          Strings::StrCat("Couldn't convert ", ValueTypeStr(type),
          " to DoubleField"));
    return std::shared_ptr<Schema::Field>(new Schema::DoubleField(v_double));
  } else if (field_type == Schema::FieldType::BOOL) {
    CHECK(type == BOOL,
          Strings::StrCat("Couldn't convert ", ValueTypeStr(type),
          " to BoolField"));
    return std::shared_ptr<Schema::Field>(new Schema::BoolField(v_bool));
  } else if (field_type == Schema::FieldType::CHAR) {
    CHECK(type == CHAR,
          Strings::StrCat("Couldn't convert ", ValueTypeStr(type),
          " to CharField"));
    return std::shared_ptr<Schema::Field>(new Schema::CharField(v_char));
  } else if (field_type == Schema::FieldType::STRING) {
    CHECK(type == STRING,
          Strings::StrCat("Couldn't convert ", ValueTypeStr(type),
          " to StringField"));
    return std::shared_ptr<Schema::Field>(new Schema::StringField(v_str));
  } else if (field_type == Schema::FieldType::CHARARRAY) {
    CHECK(type == STRING,
          Strings::StrCat("Couldn't convert ", ValueTypeStr(type),
          " to CharArrayField"));
    return std::shared_ptr<Schema::Field>(new Schema::CharArrayField(v_str));
  } else {
    LogFATAL("Unexpected field type %s",
             Schema::FieldTypeStr(field_type).c_str());
    return nullptr;
  }
}

// This is silly, but I can't find a better way to eliminate duplicated code.
#define PRODUCE_RESULT_VALUE_1(value_1, t1, value_2, t2, t_result, t1_name, t2_name, t_result_name, op)  \
    if (ValueMatchType(value_1, t1, value_2, t2)) {  \
      value.type = t_result;  \
      value.has_value_flags_ = 1;  \
      value.v_##t_result_name =  \
        ((value_1.negative? -value_1.v_##t1_name : value_1.v_##t1_name) op  \
         (value_2.negative? -value_2.v_##t2_name : value_2.v_##t2_name));  \
    }  \

#define PRODUCE_RESULT_VALUE_2(value_1, t1, value_2, t2, t_result, t1_name, t2_name, t_result_name, op)  \
    PRODUCE_RESULT_VALUE_1(value_1, t1, value_2, t2, t_result, t1_name, t2_name, t_result_name, op)  \
    if (ValueMatchType(value_2, t1, value_1, t2)) {  \
      value.type = t_result;  \
      value.has_value_flags_ = 1;  \
      value.v_##t_result_name =  \
        ((value_1.negative? -value_1.v_##t2_name : value_1.v_##t2_name) op  \
         (value_2.negative? -value_2.v_##t1_name : value_2.v_##t1_name));  \
    }  \

#define PRODUCE_STRING_OP_RESULT(value_1, op, value_2)  \
    if (ValueMatchType(value_1, STRING, value_2, STRING)) {  \
      value.type = BOOL;  \
      value.has_value_flags_ = 1;  \
      value.v_bool = value_1.v_str op value_2.v_str;  \
    }  \


namespace {

bool ValueMatchType(const NodeValue& value_1, ValueType type_1,
                    const NodeValue& value_2, ValueType type_2) {
  if (value_1.type == type_1 && value_2.type == type_2) {
    return true;
  }
  return false;
};

}  // namespace

bool NodeValue::operator==(const NodeValue& other) const {
  NodeValue value;
  PRODUCE_RESULT_VALUE_1((*this), INT64, other, INT64, BOOL, int64, int64, bool, ==)
  PRODUCE_RESULT_VALUE_2((*this), INT64, other, DOUBLE, BOOL, int64, double, bool, ==)
  PRODUCE_RESULT_VALUE_2((*this), INT64, other, CHAR, BOOL, int64, char, bool, ==)
  PRODUCE_RESULT_VALUE_1((*this), DOUBLE, other, DOUBLE, BOOL, double, double, bool, ==)
  PRODUCE_RESULT_VALUE_1((*this), BOOL, other, BOOL, BOOL, bool, bool, bool, ==)
  PRODUCE_STRING_OP_RESULT((*this), ==, other)
  PRODUCE_RESULT_VALUE_1((*this), CHAR, other, CHAR, BOOL, char, char, bool, ==)

  CHECK(value.has_value(),
        "Can't do comparison with %s and %s",
        ValueTypeStr(type).c_str(), ValueTypeStr(other.type).c_str());
  return value.v_bool;
}

bool NodeValue::operator!=(const NodeValue& other) const {
  return !((*this) == other);
}

bool NodeValue::operator<(const NodeValue& other) const {
  NodeValue value;
  PRODUCE_RESULT_VALUE_1((*this), INT64, other, INT64, BOOL, int64, int64, bool, <)
  PRODUCE_RESULT_VALUE_2((*this), INT64, other, DOUBLE, BOOL, int64, double, bool, <)
  PRODUCE_RESULT_VALUE_2((*this), INT64, other, CHAR, BOOL, int64, char, bool, <)
  PRODUCE_RESULT_VALUE_1((*this), DOUBLE, other, DOUBLE, BOOL, double, double, bool, <)
  PRODUCE_STRING_OP_RESULT((*this), <, other)
  PRODUCE_RESULT_VALUE_1((*this), CHAR, other, CHAR, BOOL, char, char, bool, <)

  CHECK(value.has_value(),
        "Can't do comparison with %s and %s",
        ValueTypeStr(type).c_str(), ValueTypeStr(other.type).c_str());
  return value.v_bool;
}

bool NodeValue::operator>(const NodeValue& other) const {
  NodeValue value;
  PRODUCE_RESULT_VALUE_1((*this), INT64, other, INT64, BOOL, int64, int64, bool, >)
  PRODUCE_RESULT_VALUE_2((*this), INT64, other, DOUBLE, BOOL, int64, double, bool, >)
  PRODUCE_RESULT_VALUE_2((*this), INT64, other, CHAR, BOOL, int64, char, bool, >)
  PRODUCE_RESULT_VALUE_1((*this), DOUBLE, other, DOUBLE, BOOL, double, double, bool, >)
  PRODUCE_STRING_OP_RESULT((*this), >, other)
  PRODUCE_RESULT_VALUE_1((*this), CHAR, other, CHAR, BOOL, char, char, bool, >)

  CHECK(value.has_value(),
        "Can't do comparison with %s and %s",
        ValueTypeStr(type).c_str(), ValueTypeStr(other.type).c_str());
  return value.v_bool;
}

bool NodeValue::operator<=(const NodeValue& other) const {
  return !((*this) > other);
}

bool NodeValue::operator>=(const NodeValue& other) const {
  return !((*this) < other);
}

NodeValue operator+(const NodeValue& lhs, const NodeValue& rhs) {
  NodeValue value;
  PRODUCE_RESULT_VALUE_1(lhs, INT64, rhs, INT64, INT64, int64, int64, int64, +)
  PRODUCE_RESULT_VALUE_2(lhs, INT64, rhs, DOUBLE, DOUBLE, int64, double, double, +)
  PRODUCE_RESULT_VALUE_2(lhs, INT64, rhs, CHAR, CHAR, int64, char, char, +)
  PRODUCE_RESULT_VALUE_1(lhs, DOUBLE, rhs, DOUBLE, DOUBLE, double, double, double, +)
  PRODUCE_RESULT_VALUE_1(lhs, CHAR, rhs, CHAR, CHAR, char, char, char, +)

  CHECK(value.has_value(),
        "Can't do comparison with %s and %s",
        ValueTypeStr(lhs.type).c_str(), ValueTypeStr(rhs.type).c_str());
  return value;
}

NodeValue operator-(const NodeValue& lhs, const NodeValue& rhs) {
  NodeValue value;
  PRODUCE_RESULT_VALUE_1(lhs, INT64, rhs, INT64, INT64, int64, int64, int64, -)
  PRODUCE_RESULT_VALUE_2(lhs, INT64, rhs, DOUBLE, DOUBLE, int64, double, double, -)
  PRODUCE_RESULT_VALUE_2(lhs, INT64, rhs, CHAR, CHAR, int64, char, char, -)
  PRODUCE_RESULT_VALUE_1(lhs, DOUBLE, rhs, DOUBLE, DOUBLE, double, double, double, -)
  PRODUCE_RESULT_VALUE_1(lhs, CHAR, rhs, CHAR, CHAR, char, char, char, -)

  CHECK(value.has_value(),
        "Can't do comparison with %s and %s",
        ValueTypeStr(lhs.type).c_str(), ValueTypeStr(rhs.type).c_str());
  return value;
}

NodeValue operator*(const NodeValue& lhs, const NodeValue& rhs) {
  NodeValue value;
  PRODUCE_RESULT_VALUE_1(lhs, INT64, rhs, INT64, INT64, int64, int64, int64, *)
  PRODUCE_RESULT_VALUE_2(lhs, INT64, rhs, DOUBLE, DOUBLE, int64, double, double, *)
  PRODUCE_RESULT_VALUE_1(lhs, DOUBLE, rhs, DOUBLE, DOUBLE, double, double, double, *)

  CHECK(value.has_value(),
        "Can't do comparison with %s and %s",
        ValueTypeStr(lhs.type).c_str(), ValueTypeStr(rhs.type).c_str());
  return value;
}

NodeValue operator/(const NodeValue& lhs, const NodeValue& rhs) {
  NodeValue value;
  PRODUCE_RESULT_VALUE_1(lhs, INT64, rhs, INT64, INT64, int64, int64, int64, /)
  PRODUCE_RESULT_VALUE_2(lhs, INT64, rhs, DOUBLE, DOUBLE, int64, double, double, /)
  PRODUCE_RESULT_VALUE_1(lhs, DOUBLE, rhs, DOUBLE, DOUBLE, double, double, double, /)

  CHECK(value.has_value(),
        "Can't do comparison with %s and %s",
        ValueTypeStr(lhs.type).c_str(), ValueTypeStr(rhs.type).c_str());
  return value;
}

NodeValue operator%(const NodeValue& lhs, const NodeValue& rhs) {
  NodeValue value;
  PRODUCE_RESULT_VALUE_1(lhs, INT64, rhs, INT64, INT64, int64, int64, int64, %)

  CHECK(value.has_value(),
        "Can't do comparison with %s and %s",
        ValueTypeStr(lhs.type).c_str(), ValueTypeStr(rhs.type).c_str());
  return value;
}

bool operator&&(const NodeValue& lhs, const NodeValue& rhs) {
  NodeValue value;
  PRODUCE_RESULT_VALUE_1(lhs, BOOL, rhs, BOOL, BOOL, bool, bool, bool, &&)

  CHECK(value.has_value(),
        "Can't do comparison with %s and %s",
        ValueTypeStr(lhs.type).c_str(), ValueTypeStr(rhs.type).c_str());
  return value.v_bool;
}

bool operator||(const NodeValue& lhs, const NodeValue& rhs) {
  NodeValue value;
  PRODUCE_RESULT_VALUE_1(lhs, BOOL, rhs, BOOL, BOOL, bool, bool, bool, ||)

  CHECK(value.has_value(),
        "Can't do comparison with %s and %s",
        ValueTypeStr(lhs.type).c_str(), ValueTypeStr(rhs.type).c_str());
  return value.v_bool;
}

bool operator!(const NodeValue& lhs) {
  if (lhs.type == BOOL) {
    return !lhs.v_bool;
  } else {
    LogFATAL("Can't use ! on value type %s", ValueTypeStr(lhs.type).c_str());
    return false;
  }
}

}  // namespace Query
