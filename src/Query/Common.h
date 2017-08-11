#ifndef QUERY_COMMON_H_
#define QUERY_COMMON_H_

#include <string>

#include "Strings/Utils.h"

#include "Schema/SchemaType.h"
#include "Storage/Record.h"

namespace Query {

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

enum AggregationType {
  NO_AGGREGATION,
  SUM,
  AVG,
  COUNT,
  MAX,
  MIN,
};

std::string AggregationStr(AggregationType aggregation_type);
bool IsFieldAggregationValid(AggregationType aggregation_type,
                             Schema::FieldType field_type);

void AggregateField(AggregationType aggregation_type,
                    Schema::Field* aggregated_field,
                    const Schema::Field* original_field);

void CalculateAvg(Schema::Field* aggregated_field, uint32 group_size);

struct Column {

  Column() = default;
  Column(const std::string& table_name_, const std::string& column_name_):
      table_name(table_name_), column_name(column_name_) {}

  std::string table_name;
  std::string column_name;
  int index = -1;
  Schema::FieldType type = Schema::FieldType::UNKNOWN_TYPE;

  std::string AsString(bool use_table_prefix) const;
  std::string DebugString() const;

  bool operator==(const Column& other) const {
    return table_name == other.table_name && column_name == other.column_name;
  }

  bool operator!=(const Column& other) const {
    return !(*this == other);
  }

  bool operator<(const Column& other) const {
    if (table_name != other.table_name) {
      return table_name < other.table_name;
    } else if (column_name != other.column_name) {
      return column_name < other.column_name;
    }
    return false;
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

  std::shared_ptr<Schema::Field>
  ToSchemaField(Schema::FieldType field_type) const;

  std::string AsString() const;

  bool operator==(const NodeValue& other) const;
  bool operator!=(const NodeValue& other) const;
  bool operator<(const NodeValue& other) const;
  bool operator>(const NodeValue& other) const;
  bool operator<=(const NodeValue& other) const;
  bool operator>=(const NodeValue& other) const;
};

NodeValue operator+(const NodeValue& lhs, const NodeValue& rhs);
NodeValue operator-(const NodeValue& lhs, const NodeValue& rhs);
NodeValue operator*(const NodeValue& lhs, const NodeValue& rhs);
NodeValue operator/(const NodeValue& lhs, const NodeValue& rhs);
NodeValue operator%(const NodeValue& lhs, const NodeValue& rhs);
bool operator&&(const NodeValue& lhs, const NodeValue& rhs);
bool operator||(const NodeValue& lhs, const NodeValue& rhs);
bool operator!(const NodeValue& lhs);

struct QueryCondition {
  Column column;
  OperatorType op;
  NodeValue value;
  bool is_const = false;
  bool const_result = false;  // only used when is_const = true

  std::string AsString() const {
    return Strings::StrCat(column.DebugString(), " ", OpTypeStr(op), " ",
                           value.AsString());
  }

  // INT64 can be compared with DOUBLE and CHAR. We need to unify the value type
  // with the column type.
  void CastValueType();
};

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

  bool ShouldScan() const { return plan == SCAN || plan == CONST_TRUE_SCAN; }
  static std::string PlanStr(Plan plan);

  void reset();

  Plan plan = NO_PLAN;
  double query_ratio = 1.0;
  ExecuteNode pop_node = NON;  // Only used when plan is POP.
  std::string table_name;

  std::vector<QueryCondition> conditions;
};

struct TableRecordMeta {
  std::vector<uint32> field_indexes;
};

struct ResultRecord {
  ResultRecord(std::shared_ptr<Storage::RecordBase> record_) :
      record(record_) {}

  std::shared_ptr<Storage::RecordBase> record;
  TableRecordMeta* meta = nullptr;

  Storage::RecordType record_type() const;

  const Schema::Field* GetField(uint32 index) const;
  Schema::Field* MutableField(uint32 index);
  // Takes ownership of the argument.
  void AddField(Schema::Field* field);
};

struct FetchedResult {
  using Tuple = std::map<std::string, ResultRecord>;
  using TupleMeta = std::map<std::string, TableRecordMeta>;

  std::vector<Tuple> tuples;
  TupleMeta* tuple_meta;

  bool AddTuple(const Tuple& tuple);
  bool AddTuple(Tuple&& tuple);

  static bool AddTupleMeta(Tuple* tuple, TupleMeta* meta);

  int NumTuples() const { return tuples.size(); }

  void reset();

  static int CompareBasedOnColumns(
      const Tuple& t1, const Tuple& t2, const std::vector<Column>& columns);

  void SortByColumns(const std::vector<Column>& columns);
  void SortByColumns(const std::string& table_name,
                     const std::vector<uint32>& field_indexes);

  // Take two set of results, sort and merge them by columns.
  void MergeSortResults(FetchedResult& result_1,
                        FetchedResult& result_2,
                        const std::vector<Column>& columns);

  void MergeSortResults(FetchedResult& result_1,
                        FetchedResult& result_2,
                        const std::string& table_name,
                        const std::vector<uint32>& field_indexes);

  void MergeSortResultsRemoveDup(FetchedResult& result_1,
                                 FetchedResult& result_2,
                                 const std::vector<Column>& columns);

  void MergeSortResultsRemoveDup(FetchedResult& result_1,
                                 FetchedResult& result_2,
                                 const std::string& table_name,
                                 const std::vector<uint32>& field_indexes);
};

}  // namespace Query

#endif  // QUERY_COMMON_H_
