#include <algorithm>

#include "Query/Result.h"

namespace Query {

namespace {
using Storage::RecordBase;
}

// *************************** TableRecordMeta *******************************//
void TableRecordMeta::CreateDataRecordMeta(const DB::TableInfo& schema) {
  fetched_fields.clear();
  for (const auto& field : schema.fields()) {
    fetched_fields.push_back(field);
  }
  record_type = Storage::DATA_RECORD;
}

void TableRecordMeta::CreateIndexRecordMeta(
    const DB::TableInfo& schema, const std::vector<uint32>& field_indexes) {
  fetched_fields.clear();
  for (uint32 index : field_indexes) {
    fetched_fields.push_back(schema.fields(index));
  }
  record_type = Storage::INDEX_RECORD;
}

// ****************************** ResultRecord *******************************//
Storage::RecordType ResultRecord::record_type() const {
  if (meta) {
    return meta->record_type;
  } else {
    return record->type();
  }
}

const Schema::Field* ResultRecord::GetField(uint32 index) const {
  if (record_type() == Storage::DATA_RECORD) {
    CHECK(index < record->NumFields(), "index %d out of range", index);
    return record->fields().at(index).get();
  } else if (record_type() == Storage::INDEX_RECORD) {
    int pos = -1;
    for (uint32 i = 0 ; i < meta->fetched_fields.size(); i++) {
      if (meta->fetched_fields.at(i).index() == index) {
        pos = i;
        break;
      }
    }
    CHECK(pos >= 0, Strings::StrCat("Can't find required field index ",
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


// ******************************* Tuple ************************************ //
uint32 Tuple::size() const {
  uint32 size = 0;
  for (const auto& iter : records) {
    size += iter.second.record->size();
  }
  return size;
}
void Tuple::Print() const {
  for (const auto& iter : records) {
    printf("%s ", iter.first.c_str());
    if (iter.second.record) {
      iter.second.record->Print();
    }
  }
  printf("\n");
}

const ResultRecord* Tuple::GetTableRecord(const std::string& table_name) const {
  auto it = records.find(table_name);
  if (it == records.end()) {
    return nullptr;
  }
  return &it->second;
}

ResultRecord* Tuple::MutableTableRecord(const std::string& table_name) {
  return const_cast<ResultRecord*>(MutableTableRecord(table_name));
}

bool Tuple::AddTableRecord(const std::string& table_name,
                           std::shared_ptr<Storage::RecordBase> record) {
  records.emplace(table_name, ResultRecord(record));
  return true;
}

bool Tuple::AddMeta(const TupleMeta& meta) {
  for (auto& table_record_iter : records) {
    auto meta_it = meta.find(table_record_iter.first);
    if (meta_it == meta.end()) {
      return false;
    }
    table_record_iter.second.meta = &meta_it->second;
  }
  return true;
}

std::shared_ptr<Tuple> Tuple::MergeTuples(
    const Tuple& t1, const Tuple& t2) {
  std::shared_ptr<Tuple> tuple(new Tuple(t1));
  for (const auto& iter : t2.records) {
    tuple->records.emplace(iter.first, iter.second);
  }
  return tuple;
}

int Tuple::CompareBasedOnColumns(
    const Tuple& t1, const std::vector<Column>& columns_1,
    const Tuple& t2, const std::vector<Column>& columns_2) {
  CHECK(columns_1.size() == columns_2.size(),
        "Comparing tuples with different number of columns");
  uint32 num_columns = columns_1.size();

  for (uint32 i = 0; i < num_columns; i++) {
    const Column& column_1 = columns_1.at(i);
    auto record_1 = t1.GetTableRecord(column_1.table_name);
    CHECK(record_1 != nullptr,
          Strings::StrCat("Couldn't find record of table ", column_1.table_name,
                          " from tuple 1"));
    CHECK(record_1->meta != nullptr,
          Strings::StrCat("Couldn't find record meta of table ",
                          column_1.table_name,
                          " from tuple 1"));
    auto field_1 = record_1->GetField(column_1.index);
    CHECK(field_1 != nullptr,
          Strings::StrCat("Couldn't find record field ",
                          std::to_string(column_1.index),
                          "of table ", column_1.table_name,
                          " from the given tuple"));

    const Column& column_2 = columns_2.at(i);
    auto record_2 = t2.GetTableRecord(column_2.table_name);
    CHECK(record_2 != nullptr,
          Strings::StrCat("Couldn't find record of table ", column_2.table_name,
                          " from tuple 2"));
    CHECK(record_2->meta != nullptr,
          Strings::StrCat("Couldn't find record meta of table ",
                          column_2.table_name,
                          " from tuple 2"));
    auto field_2 = record_2->GetField(column_2.index);
    CHECK(field_2 != nullptr,
          Strings::StrCat("Couldn't find record field ",
                          std::to_string(column_2.index),
                          "of table ", column_2.table_name,
                          " from the given tuple"));

    // CHECK(record_1.meta == record_2.meta,
    //       "Comparing table records with different meta");

    int re = RecordBase::CompareSchemaFields(field_1, field_2);
    if (re != 0) {
      return re;
    }
  }
  return 0;
}

int Tuple::CompareBasedOnColumns(
    const Tuple& t1, const Tuple& t2,
    const std::vector<Column>& columns) {
  return CompareBasedOnColumns(t1, columns, t2, columns);
}


// ************************* ResultContainer ******************************** //
std::shared_ptr<Tuple> ResultContainer::GetTuple(uint32 index) {
  if (index >= tuples_.size()) {
    return nullptr;
  }

  return tuples_.at(index);
}

std::shared_ptr<Tuple> ResultContainer::GetNextTuple() {
  if (!materialized_) {
    if (crt_tindex_ < tuples_.size()) {
      return tuples_.at(crt_tindex_++);
    } else {
      return nullptr;
    }
  } else {
    // TODO: Read from FlatTupleFile.
    return nullptr;
  }
}

bool ResultContainer::AddTuple(std::shared_ptr<Tuple> tuple) {
  if (!tuple->AddMeta(*tuple_meta_)) {
    return false;
  }
  tuples_.push_back(tuple);
  return true;
}

bool ResultContainer::AddTuple(const Tuple& tuple) {
  std::shared_ptr<Tuple> new_tuple(new Tuple(tuple));
  if (!new_tuple->AddMeta(*tuple_meta_)) {
    return false;
  }
  tuples_.push_back(new_tuple);
  return true;
}

bool ResultContainer::AddTuple(Tuple&& tuple) {
  std::shared_ptr<Tuple> new_tuple(new Tuple(std::move(tuple)));
  if (!new_tuple->AddMeta(*tuple_meta_)) {
    return false;
  }
  tuples_.push_back(new_tuple);
  return true;
}

void ResultContainer::SortByColumns(const std::vector<Column>& columns) {
  auto comparator = [&] (std::shared_ptr<Tuple> t1, std::shared_ptr<Tuple> t2) {
    return Tuple::CompareBasedOnColumns(*t1, *t2, columns) < 0;
  };

  std::sort(tuples_.begin(), tuples_.end(), comparator);
}

void ResultContainer::SortByColumns(const std::string& table_name,
                                    const std::vector<uint32>& field_indexes) {
  std::vector<Column> columns;
  for (int index : field_indexes) {
    columns.emplace_back(table_name, "" /* column name doesn't matter */);
    columns.back().index = index;
  }

  SortByColumns(columns);
}

void ResultContainer::MergeSortResults(ResultContainer& result_1,
                                       ResultContainer& result_2,
                                       const std::vector<Column>& columns) {
  result_1.SortByColumns(columns);
  result_2.SortByColumns(columns);

  auto iter_1 = result_1.tuples_.begin();
  auto iter_2 = result_2.tuples_.begin();
  while (iter_1 != result_1.tuples_.end() && iter_2 != result_2.tuples_.end()) {
    if (Tuple::CompareBasedOnColumns(**iter_1, **iter_2, columns) <= 0) {
      AddTuple(*iter_1);
      ++iter_1;
    } else {
      AddTuple(*iter_2);
      ++iter_2;
    }
  }

  while (iter_1 != result_1.tuples_.end()) {
    AddTuple(*iter_1);
    ++iter_1;
  }
  while (iter_2 != result_2.tuples_.end()) {
    AddTuple(*iter_2);
    ++iter_2;
  }
}

void ResultContainer::MergeSortResults(
    ResultContainer& result_1,
    ResultContainer& result_2,
    const std::string& table_name,
    const std::vector<uint32>& field_indexes) {
  std::vector<Column> columns;
  for (int index : field_indexes) {
    columns.emplace_back(table_name, "" /* column name doesn't matter */);
    columns.back().index = index;
  }

  MergeSortResults(result_1, result_2, columns);
}

void ResultContainer::MergeSortResultsRemoveDup(
    ResultContainer& result_1, ResultContainer& result_2,
    const std::vector<Column>& columns) {
  result_1.SortByColumns(columns);
  result_2.SortByColumns(columns);

  auto iter_1 = result_1.tuples_.begin();
  auto iter_2 = result_2.tuples_.begin();
  std::shared_ptr<Tuple> last_tuple;
  while (iter_1 != result_1.tuples_.end() && iter_2 != result_2.tuples_.end()) {
    int re = Tuple::CompareBasedOnColumns(**iter_1, **iter_2, columns);
    if (re <= 0) {
      if (!last_tuple ||
          (last_tuple &&
           Tuple::CompareBasedOnColumns(*last_tuple, **iter_1, columns) != 0)) {
        AddTuple(*iter_1);
        last_tuple = tuples_.back();
      }
      ++iter_1;
    } else {
      if (!last_tuple ||
          (last_tuple &&
           Tuple::CompareBasedOnColumns(*last_tuple, **iter_2, columns) != 0)) {
        AddTuple(*iter_2);
        last_tuple = tuples_.back();
      }
      ++iter_2;
    }
  }

  while (iter_1 != result_1.tuples_.end()) {
    if (!last_tuple ||
        (last_tuple &&
         Tuple::CompareBasedOnColumns(*last_tuple, **iter_1, columns)) != 0) {
      AddTuple(*iter_1);
      last_tuple = tuples_.back();
    }
    ++iter_1;
  }
  while (iter_2 != result_2.tuples_.end()) {
    if (!last_tuple ||
        (last_tuple &&
         Tuple::CompareBasedOnColumns(*last_tuple, **iter_2, columns) != 0)) {
      AddTuple(*iter_2);
      last_tuple = tuples_.back();
    }
    ++iter_2;
  }
}

void ResultContainer::MergeSortResultsRemoveDup(
    ResultContainer& result_1, ResultContainer& result_2,
    const std::string& table_name, const std::vector<uint32>& field_indexes) {
  std::vector<Column> columns;
  for (int index : field_indexes) {
    columns.emplace_back(table_name, "" /* column name doesn't matter */);
    columns.back().index = index;
  }

  MergeSortResultsRemoveDup(result_1, result_2, columns);
}

void ResultContainer::reset() {
  tuple_meta_ = nullptr;
  tuples_.clear();
  materialized_ = false;
  num_tuples_ = 0;
  crt_tindex_ = 0;
}


// ************************* Result Aggregation ***************************** //
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

}  // namespace Query
