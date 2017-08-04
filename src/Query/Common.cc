#include <algorithm>
#include <math.h>

#include "Strings/Utils.h"

#include "Query/Common.h"

namespace {
using Storage::RecordBase;
}

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
        if (floor(value.v_double) != value.v_double) {
          condition->is_const = true;
          condition->const_result = false;
        } else {
          value.v_int64 = static_cast<int64>(value.v_double);
        }
      } else if (condition->op == NONEQUAL) {
        if (floor(value.v_double) != value.v_double) {
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

std::string PhysicalPlan::PlanStr(Plan plan) {
  switch (plan) {
    case NO_PLAN: return "NO_PLAN";
    case CONST_FALSE_SKIP: return "CONST_FALSE_SKIP";
    case CONST_TRUE_SCAN: return "CONST_TRUE_SCAN";
    case SCAN: return "SCAN";
    case SEARCH: return "SEARCH";
    case POP: return "POP";
    default: return "UNKNOWN_PLAN_TYPE";
  }
  return "";
}

void PhysicalPlan::reset() {
  plan = NO_PLAN;
  query_ratio = 0;
  pop_node = NON;  // Only used when plan is POP.
  conditions.clear();
}

Storage::RecordType ResultRecord::record_type() const {
  if (record) {
    return record->type();
  }
  return Storage::UNKNOWN_RECORDTYPE;
}

int FetchedResult::CompareBasedOnColumns(const Tuple& t1, const Tuple& t2,
                                         const std::vector<Column>& columns) {
  for (const Column& column : columns) {
    auto it = t1.find(column.table_name);
    CHECK(it != t1.end(),
          Strings::StrCat("Coulnd't find record of table ", column.table_name,
                          " from tuple 1"));
    const ResultRecord& record_1 = it->second;

    it = t2.find(column.table_name);
    CHECK(it != t2.end(),
          Strings::StrCat("Coulnd't find record of table ", column.table_name,
                          " from tuple 2"));
    const ResultRecord& record_2 = it->second;

    CHECK(record_1.record_type() == record_2.record_type(),
          "Comparing records with different record types");
    int pos = -1;
    if (record_1.record_type() == Storage::DATA_RECORD) {
      pos = column.index;
    } else if (record_1.record_type() == Storage::INDEX_RECORD) {
      CHECK(record_1.field_indexes == record_2.field_indexes,
            "Comparing index records with different index fields");
      for (uint32 i = 0 ; i < record_1.field_indexes.size(); i++) {
        if (record_1.field_indexes.at(i) == column.index) {
          pos = i;
          break;
        }
      }
      CHECK(pos > 0, Strings::StrCat("Can't find required field index ",
                                     std::to_string(column.index),
                                     " for this index record"));
    } else {
      LogFATAL("Invalid record type to evalute: %s",
               Storage::RecordTypeStr(record_1.record_type()).c_str());
    }
    int re = RecordBase::CompareSchemaFields(
                record_1.record->fields().at(pos).get(),
                record_2.record->fields().at(pos).get());
    if (re != 0) {
      return re;
    }
  }
  return 0;
}

void FetchedResult::SortByColumns(const std::vector<Column>& columns) {
  auto comparator = [&] (const Tuple& t1, const Tuple& t2) {
    return CompareBasedOnColumns(t1, t2, columns);
  };

  std::sort(tuples.begin(), tuples.end(), comparator);
}

void FetchedResult::SortByColumns(const std::string& table_name,
                                  std::vector<int>& field_indexes) {
  std::vector<Column> columns;
  for (int index : field_indexes) {
    columns.emplace_back(table_name, "" /* column name doesn't matter */);
    columns.back().index = index;
  }

  SortByColumns(columns);
}

void FetchedResult::MergeSortResults(FetchedResult& result_1,
                                     FetchedResult& result_2,
                                     const std::vector<Column>& columns) {
  result_1.SortByColumns(columns);
  result_2.SortByColumns(columns);

  auto comparator = [&] (const Tuple& t1, const Tuple& t2) {
    return CompareBasedOnColumns(t1, t2, columns);
  };

  auto iter_1 = result_1.tuples.begin();
  auto iter_2 = result_2.tuples.begin();
  while (iter_1 != result_1.tuples.end() && iter_1 != result_1.tuples.end()) {
    if (comparator(*iter_1, *iter_2) <= 0) {
      tuples.push_back(*iter_1);
      ++iter_1;
    } else {
      tuples.push_back(*iter_2);
      ++iter_2;
    }
  }

  while (iter_1 != result_1.tuples.end()) {
    tuples.push_back(*iter_1);
    ++iter_1;
  }
  while (iter_2 != result_2.tuples.end()) {
    tuples.push_back(*iter_2);
    ++iter_2;
  }
}

void FetchedResult::MergeSortResults(FetchedResult& result_1,
                                     FetchedResult& result_2,
                                     const std::string& table_name,
                                     std::vector<int>& field_indexes) {
  std::vector<Column> columns;
  for (int index : field_indexes) {
    columns.emplace_back(table_name, "" /* column name doesn't matter */);
    columns.back().index = index;
  }

  MergeSortResults(result_1, result_2, columns);
}

void FetchedResult::MergeSortResultsRemoveDup(
    FetchedResult& result_1, FetchedResult& result_2,
    const std::vector<Column>& columns) {
  result_1.SortByColumns(columns);
  result_2.SortByColumns(columns);

  auto comparator = [&] (const Tuple& t1, const Tuple& t2) {
    return CompareBasedOnColumns(t1, t2, columns);
  };

  auto iter_1 = result_1.tuples.begin();
  auto iter_2 = result_2.tuples.begin();
  const Tuple* last_tuple = nullptr;
  while (iter_1 != result_1.tuples.end() && iter_1 != result_1.tuples.end()) {
    if (comparator(*iter_1, *iter_2) <= 0) {
      if (last_tuple && comparator(*last_tuple, *iter_1) != 0) {
        tuples.push_back(*iter_1);
        last_tuple = &tuples.back();
      }
      ++iter_1;
    } else {
      if (last_tuple && comparator(*last_tuple, *iter_2) != 0) {
        tuples.push_back(*iter_2);
        last_tuple = &tuples.back();
      }
      ++iter_2;
    }
  }

  while (iter_1 != result_1.tuples.end()) {
    if (last_tuple && comparator(*last_tuple, *iter_1) != 0) {
      tuples.push_back(*iter_1);
      last_tuple = &tuples.back();
    }
    ++iter_1;
  }
  while (iter_2 != result_2.tuples.end()) {
    if (last_tuple && comparator(*last_tuple, *iter_2) != 0) {
      tuples.push_back(*iter_2);
      last_tuple = &tuples.back();
    }
    ++iter_2;
  }
}

}  // namespace Query
