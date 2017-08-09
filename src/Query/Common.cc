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

std::string AggregationStr(AggregationType aggregation_type) {
  switch (aggregation_type) {
    case NO_AGGREGATION: return "NO_AGGREGATION";
    case SUM: return "SUM";
    case AVG: return "AVG";
    case COUNT: return "COUNT";
    case MAX: return "MAX";
    case MIN: return "MIN";
  }
  return "NO_AGGREGATION";
}

bool IsFieldAggregationValid(AggregationType aggregation_type,
                             Schema::FieldType field_type) {
  switch (aggregation_type) {
    case SUM:
    case AVG:
      if (field_type == Schema::FieldType::INT ||
          field_type == Schema::FieldType::LONGINT ||
          field_type == Schema::FieldType::DOUBLE) {
        return true;
      } else {
        return false;
      }
      break;
    case MAX:
    case MIN:
      if (field_type != Schema::FieldType::BOOL) {
        return true;
      } else {
        return false;
      }
      break;
    case COUNT:
      return true;
    default:
      return false;
  }
  return false;
}

#define CAST_FIELDS(schema_field)  \
    auto* cast_aggregated_field =  \
        dynamic_cast<Schema::schema_field*>(aggregated_field);  \
    const auto* cast_original_field =  \
        dynamic_cast<const Schema::schema_field*>(original_field);  \

void AggregateField(AggregationType aggregation_type,
                    Schema::Field* aggregated_field,
                    const Schema::Field* original_field) {
  auto field_type = aggregated_field->type();
  if (original_field) {
    CHECK(field_type == original_field->type(),
          "Aggregating field type %s with %s",
          Schema::FieldTypeStr(field_type).c_str(),
          Schema::FieldTypeStr(original_field->type()).c_str());
  } else {
    CHECK(aggregation_type == COUNT && field_type == Schema::FieldType::INT,
          "Expect INT aggregation type for COUNT, but got %s",
          Schema::FieldTypeStr(field_type).c_str());
  }
  
  switch (aggregation_type) {
    case SUM:
    case AVG:
      if (field_type == Schema::FieldType::INT) {
        CAST_FIELDS(IntField);
        *cast_aggregated_field += *cast_original_field;
      } else if (field_type == Schema::FieldType::LONGINT) {
        CAST_FIELDS(LongIntField);
        *cast_aggregated_field += *cast_original_field;
      } else if (field_type == Schema::FieldType::DOUBLE) {
        CAST_FIELDS(DoubleField);
        *cast_aggregated_field += *cast_original_field;
      }
      break;
    case MAX: {
      if (field_type == Schema::FieldType::INT) {
        CAST_FIELDS(IntField);
        if (cast_original_field->value() > cast_aggregated_field->value()) {
          cast_aggregated_field->set_value(cast_original_field->value());
        }
      } else if (field_type == Schema::FieldType::LONGINT) {
        CAST_FIELDS(LongIntField);
        if (cast_original_field->value() > cast_aggregated_field->value()) {
          cast_aggregated_field->set_value(cast_original_field->value());
        }
      } else if (field_type == Schema::FieldType::DOUBLE) {
        CAST_FIELDS(DoubleField);
        if (cast_original_field->value() > cast_aggregated_field->value()) {
          cast_aggregated_field->set_value(cast_original_field->value());
        }
      } else if (field_type == Schema::FieldType::BOOL) {
        CAST_FIELDS(BoolField);
        if (cast_original_field->value() > cast_aggregated_field->value()) {
          cast_aggregated_field->set_value(cast_original_field->value());
        }
      } else if (field_type == Schema::FieldType::CHAR) {
        CAST_FIELDS(CharField);
        if (cast_original_field->value() > cast_aggregated_field->value()) {
          cast_aggregated_field->set_value(cast_original_field->value());
        }
      } else if (field_type == Schema::FieldType::STRING) {
        CAST_FIELDS(StringField);
        if (cast_original_field->value() > cast_aggregated_field->value()) {
          cast_aggregated_field->set_value(cast_original_field->value());
        }
      }  else if (field_type == Schema::FieldType::CHARARRAY) {
        CAST_FIELDS(CharArrayField);
        if (cast_original_field->AsString() >
            cast_aggregated_field->AsString()) {
          cast_aggregated_field->SetData(cast_original_field->value(),
                                         cast_original_field->valid_length());
        }
      }
      break;
    }
    case MIN: {
      if (field_type == Schema::FieldType::INT) {
        CAST_FIELDS(IntField);
        if (cast_original_field->value() < cast_aggregated_field->value()) {
          cast_aggregated_field->set_value(cast_original_field->value());
        }
      } else if (field_type == Schema::FieldType::LONGINT) {
        CAST_FIELDS(LongIntField);
        if (cast_original_field->value() < cast_aggregated_field->value()) {
          cast_aggregated_field->set_value(cast_original_field->value());
        }
      } else if (field_type == Schema::FieldType::DOUBLE) {
        CAST_FIELDS(DoubleField);
        if (cast_original_field->value() < cast_aggregated_field->value()) {
          cast_aggregated_field->set_value(cast_original_field->value());
        }
      } else if (field_type == Schema::FieldType::BOOL) {
        CAST_FIELDS(BoolField);
        if (cast_original_field->value() < cast_aggregated_field->value()) {
          cast_aggregated_field->set_value(cast_original_field->value());
        }
      } else if (field_type == Schema::FieldType::CHAR) {
        CAST_FIELDS(CharField);
        if (cast_original_field->value() < cast_aggregated_field->value()) {
          cast_aggregated_field->set_value(cast_original_field->value());
        }
      } else if (field_type == Schema::FieldType::STRING) {
        CAST_FIELDS(StringField);
        if (cast_original_field->value() < cast_aggregated_field->value()) {
          cast_aggregated_field->set_value(cast_original_field->value());
        }
      }  else if (field_type == Schema::FieldType::CHARARRAY) {
        CAST_FIELDS(CharArrayField);
        if (cast_original_field->AsString() <
            cast_aggregated_field->AsString()) {
          cast_aggregated_field->SetData(cast_original_field->value(),
                                         cast_original_field->valid_length());
        }
      }
      break;
    }
    case COUNT: {
      auto* cast_aggregated_field =
        dynamic_cast<Schema::IntField*>(aggregated_field);
      cast_aggregated_field->Inc(1);
      break;
    }
    default:
      break;
  }
}

void CalculateAvg(Schema::Field* aggregated_field, uint32 group_size) {
  auto field_type = aggregated_field->type();
  if (field_type == Schema::FieldType::INT) {
    auto* cast_aggregated_field =
        dynamic_cast<Schema::IntField*>(aggregated_field);
    cast_aggregated_field->set_value(
        cast_aggregated_field->value() / group_size);
  } else if (field_type == Schema::FieldType::LONGINT) {
    auto* cast_aggregated_field =
        dynamic_cast<Schema::LongIntField*>(aggregated_field);
    cast_aggregated_field->set_value(
        cast_aggregated_field->value() / group_size);
  } else if (field_type == Schema::FieldType::DOUBLE) {
    auto* cast_aggregated_field =
        dynamic_cast<Schema::DoubleField*>(aggregated_field);
    cast_aggregated_field->set_value(
        cast_aggregated_field->value() / group_size);
  }
}

std::string Column::DebugString() const {
  std::string result = Strings::StrCat("(", table_name, ", ",
                                       column_name, ", ",
                                       std::to_string(index), ")");
  return result;
}

std::string Column::AsString(bool use_table_prefix) const {
  std::string result = column_name;
  if (use_table_prefix) {
    result = Strings::StrCat(table_name, ". ", column_name);
  }
  return result;
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

void QueryCondition::CastValueType() {
  auto type = FromSchemaType(column.type);
  if (type == INT64) {
    if (value.type == DOUBLE) {
      // Careful, don't cast directly. Needs to check the op type.
      if (op == EQUAL) {
        if (floor(value.v_double) != value.v_double) {
          is_const = true;
          const_result = false;
        } else {
          value.v_int64 = static_cast<int64>(value.v_double);
        }
      } else if (op == NONEQUAL) {
        if (floor(value.v_double) != value.v_double) {
          is_const = true;
          const_result = true;
        } else {
          value.v_int64 = static_cast<int64>(value.v_double);
        }
      } else if (op == LT) {
        if (floor(value.v_double) == value.v_double) {
          value.v_int64 = static_cast<int64>(value.v_double);
        } else {
          value.v_int64 = static_cast<int64>(value.v_double) + 1;
        }
      } else if (op == LE) {
        value.v_int64 = static_cast<int64>(value.v_double);
      } else if (op == GT) {
        value.v_int64 = static_cast<int64>(value.v_double);
      } else if (op == GE) {
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

const Schema::Field* ResultRecord::GetField(uint32 index) const {
  if (record_type() == Storage::DATA_RECORD) {
    CHECK(index < record->NumFields(), "index %d out of range", index);
    return record->fields().at(index).get();
  } else if (record_type() == Storage::INDEX_RECORD) {
    int pos = -1;
    for (uint32 i = 0 ; i < meta->field_indexes.size(); i++) {
      if (meta->field_indexes.at(i) == index) {
        pos = i;
        break;
      }
    }
    CHECK(pos > 0, Strings::StrCat("Can't find required field index ",
                                   std::to_string(index),
                                   " for this index record"));
    return record->fields().at(pos).get();
  } else {
    LogFATAL("Invalid record type to evalute: %s",
             Storage::RecordTypeStr(record_type()).c_str());
  }
  return nullptr;
}

Schema::Field* ResultRecord::MutableField(uint32 index) {
  return const_cast<Schema::Field*>(GetField(index));
}

void ResultRecord::AddField(Schema::Field* field) {
  record->AddField(field);
}

bool FetchedResult::AddTuple(const Tuple& tuple) {
  tuples.push_back(tuple);
  for (auto& table_record_iter : tuples.back()) {
    auto meta_it = tuple_meta.find(table_record_iter.first);
    if (meta_it == tuple_meta.end()) {
      tuples.erase(tuples.end() - 1);
      return false;
    }
    table_record_iter.second.meta = &meta_it->second;
  }
  return true;
}

bool FetchedResult::AddTuple(Tuple&& tuple) {
  for (auto& table_record_iter : tuple) {
    auto meta_it = tuple_meta.find(table_record_iter.first);
    if (meta_it == tuple_meta.end()) {
      return false;
    }
    table_record_iter.second.meta = &meta_it->second;
  }
  tuples.push_back(std::move(tuple));
  return true;
}

int FetchedResult::CompareBasedOnColumns(
    const Tuple& t1, const Tuple& t2,
    const std::vector<Column>& columns) {
  for (const Column& column : columns) {
    auto it = t1.find(column.table_name);
    CHECK(it != t1.end(),
          Strings::StrCat("Couldn't find record of table ", column.table_name,
                          " from tuple 1"));
    const auto& record_1 = it->second;
    CHECK(record_1.meta != nullptr,
          Strings::StrCat("Couldn't find record meta of table ",
                          column.table_name,
                          " from tuple 1"));

    it = t2.find(column.table_name);
    CHECK(it != t2.end(),
          Strings::StrCat("Couldn't find record of table ", column.table_name,
                          " from tuple 2"));
    const auto& record_2 = it->second;
    CHECK(record_2.meta != nullptr,
          Strings::StrCat("Couldn't find record meta of table ",
                          column.table_name,
                          " from tuple 2"));

    CHECK(record_1.record_type() == record_2.record_type(),
          "Comparing records with different record types");

    CHECK(record_1.meta->field_indexes == record_2.meta->field_indexes,
            "Comparing index records with different index fields");

    int re = RecordBase::CompareSchemaFields(record_1.GetField(column.index),
                                             record_2.GetField(column.index));
    if (re != 0) {
      return re;
    }
  }
  return 0;
}

void FetchedResult::SortByColumns(const std::vector<Column>& columns) {
  auto comparator = [&] (const Tuple& t1, const Tuple& t2) {
    return CompareBasedOnColumns(t1, t2, columns) < 0;
  };

  std::sort(tuples.begin(), tuples.end(), comparator);
}

void FetchedResult::SortByColumns(const std::string& table_name,
                                  const std::vector<uint32>& field_indexes) {
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

  auto iter_1 = result_1.tuples.begin();
  auto iter_2 = result_2.tuples.begin();
  while (iter_1 != result_1.tuples.end() && iter_2 != result_2.tuples.end()) {
    if (CompareBasedOnColumns(*iter_1, *iter_2, columns) <= 0) {
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
                                     const std::vector<uint32>& field_indexes) {
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

  auto iter_1 = result_1.tuples.begin();
  auto iter_2 = result_2.tuples.begin();
  const Tuple* last_tuple = nullptr;
  while (iter_1 != result_1.tuples.end() && iter_2 != result_2.tuples.end()) {
    int re = CompareBasedOnColumns(*iter_1, *iter_2, columns);
    if (re <= 0) {
      if (!last_tuple ||
          (last_tuple &&
           CompareBasedOnColumns(*last_tuple, *iter_1, columns) != 0)) {
        AddTuple(std::move(*iter_1));
        last_tuple = &tuples.back();
      }
      ++iter_1;
    } else {
      if (!last_tuple ||
          (last_tuple &&
           CompareBasedOnColumns(*last_tuple, *iter_2, columns) != 0)) {
        AddTuple(std::move(*iter_2));
        last_tuple = &tuples.back();
      }
      ++iter_2;
    }
  }

  while (iter_1 != result_1.tuples.end()) {
    if (!last_tuple ||
        (last_tuple &&
         CompareBasedOnColumns(*last_tuple, *iter_1, columns)) != 0) {
      AddTuple(std::move(*iter_1));
      last_tuple = &tuples.back();
    }
    ++iter_1;
  }
  while (iter_2 != result_2.tuples.end()) {
    if (!last_tuple ||
        (last_tuple &&
         CompareBasedOnColumns(*last_tuple, *iter_2, columns) != 0)) {
      AddTuple(std::move(*iter_2));
      last_tuple = &tuples.back();
    }
    ++iter_2;
  }
}

void FetchedResult::MergeSortResultsRemoveDup(
    FetchedResult& result_1, FetchedResult& result_2,
    const std::string& table_name, const std::vector<uint32>& field_indexes) {
  std::vector<Column> columns;
  for (int index : field_indexes) {
    columns.emplace_back(table_name, "" /* column name doesn't matter */);
    columns.back().index = index;
  }

  MergeSortResultsRemoveDup(result_1, result_2, columns);
}

}  // namespace Query
