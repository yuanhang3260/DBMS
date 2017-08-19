#include <algorithm>

#include "Query/Result.h"

namespace Query {

namespace {
using Storage::RecordBase;
}

// *************************** TableRecordMeta *******************************//
void TableRecordMeta::CreateDataRecordMeta(const DB::TableInfo& schema) {
  if (!fetched_fields.empty()) {
    return;
  }

  for (const auto& field : schema.fields()) {
    fetched_fields.push_back(field);
  }
  record_type = Storage::DATA_RECORD;
}

void TableRecordMeta::CreateIndexRecordMeta(
    const DB::TableInfo& schema, const std::vector<uint32>& field_indexes) {
  if (!fetched_fields.empty()) {
    return;
  }

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


// **************************** FetchedResult ******************************* //
uint32 FetchedResult::TupleSize(const FetchedResult::Tuple& tuple) {
  uint32 size = 0;
  for (const auto& iter : tuple) {
    size += iter.second.record->size();
  }
  return size;
}

bool FetchedResult::AddTuple(const Tuple& tuple) {
  tuples.push_back(tuple);
  for (auto& table_record_iter : tuples.back()) {
    auto meta_it = tuple_meta->find(table_record_iter.first);
    if (meta_it == tuple_meta->end()) {
      tuples.erase(tuples.end() - 1);
      return false;
    }
    table_record_iter.second.meta = &meta_it->second;
  }
  return true;
}

bool FetchedResult::AddTuple(Tuple&& tuple) {
  if (!AddTupleMeta(&tuple, tuple_meta)) {
    return false;
  }
  tuples.push_back(std::move(tuple));
  return true;
}

bool FetchedResult::AddTupleMeta(Tuple* tuple, TupleMeta* meta) {
  for (auto& table_record_iter : *tuple) {
    auto meta_it = meta->find(table_record_iter.first);
    if (meta_it == meta->end()) {
      return false;
    }
    table_record_iter.second.meta = &meta_it->second;
  }
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

    CHECK(record_1.meta == record_2.meta,
          "Comparing table records with different meta");

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

void FetchedResult::reset() {
  tuple_meta = nullptr;
  tuples.clear();
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
