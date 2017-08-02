#ifndef QUERY_COMMON_
#define QUERY_COMMON_

#include <string>

#include "Schema/SchemaType.h"
#include "Strings/Utils.h"

namespace Query {

class OperatorNode;

// We use a different representation of value type enum other than
// DB::TableField::Type. This enum is specific for Sql query parser/evaluator.
//
// All integer types are treated as INT64, both string and char_array types are
// treated as STRING type.
enum ValueType {
  UNKNOWN_VALUE_TYPE,
  INT64,
  DOUBLE,
  STRING,
  CHAR,
  BOOL,
};

std::string ValueTypeStr(ValueType value_type);

// Convert a schema field type to value type.
ValueType FromSchemaType(Schema::FieldType field_type);


enum OperatorType {
  UNKNOWN_OPERATOR,
  ADD,  // +
  SUB,  // -
  MUL,  // *
  DIV,  // /
  MOD,  // %
  EQUAL,     // =
  NONEQUAL,  // !=
  GE,  // >=
  GT,  // >
  LT,  // <
  LE,  // <=
  AND,
  OR,
  NOT,
};

std::string OpTypeStr(OperatorType op_type);
OperatorType StrToOp(const std::string& str);
bool IsNumerateOp(OperatorType op_type);
bool IsCompareOp(OperatorType op_type);
bool IsLogicalOp(OperatorType op_type);
OperatorType FlipOp(OperatorType op_type);

struct Column {
  Column() = default;
  Column(const std::string& table_name_, const std::string& column_name_):
      table_name(table_name_), column_name(column_name_) {}

  std::string table_name;
  std::string column_name;
  int index = -1;
  Schema::FieldType type = Schema::FieldType::UNKNOWN_TYPE;

  std::string AsString() const {
    return Strings::StrCat("(", table_name, ", ", column_name, ", ",
                           std::to_string(index), ")");
  }
};

struct NodeValue {
  int64 v_int64 = 0;
  double v_double = 0;
  std::string v_str;
  char v_char = 0;
  bool v_bool = false;

  ValueType type = UNKNOWN_VALUE_TYPE;
  bool negative = false;

  bool has_value() const { return has_value_flags_ != 0; }
  byte has_value_flags_ = 0;

  NodeValue() : type(UNKNOWN_VALUE_TYPE) {}
  explicit NodeValue(ValueType type_arg) : type(type_arg) {}

  NodeValue static IntValue(int64 v);
  NodeValue static DoubleValue(double v);
  NodeValue static StringValue(const std::string& v);
  NodeValue static CharValue(char v);
  NodeValue static BoolValue(bool v);

  std::string AsString() const;

  bool operator==(const NodeValue& other) const;
  bool operator!=(const NodeValue& other) const;
  bool operator<(const NodeValue& other) const;
  bool operator>(const NodeValue& other) const;
  bool operator<=(const NodeValue& other) const;
  bool operator>=(const NodeValue& other) const;
};

struct QueryCondition {
  Column column;
  OperatorType op;
  NodeValue value;
  bool is_const = false;
  bool const_result = false;  // only used when is_const = true

  std::string AsString() const {
    return Strings::StrCat(column.AsString(), " ", OpTypeStr(op), " ",
                           value.AsString());
  }
};

// INT64 can be compared with DOUBLE and CHAR. We need to unify the value type
// with the column type.
void CastValueType(QueryCondition* condition);

struct PhysicalPlan {
  enum Plan {
    NO_PLAN,
    CONST_FALSE_SKIP,
    CONST_TRUE_SCAN,  // This should scan
    SCAN,
    SEARCH,
    POP,  // pop result from children nodes.
  };

  enum ExecuteNode {
    NON,
    BOTH,
    LEFT,
    RIGHT,
  };

  Plan plan = NO_PLAN;
  double query_ratio = 0;
  ExecuteNode pop_node = NON;  // Only used when plan is POP.

  std::vector<QueryCondition> conditions;

  bool ShouldScan() const { return plan == SCAN || plan == CONST_TRUE_SCAN; }

  void reset();
};

}  // namespace Query

#endif  // QUERY_COMMON_
