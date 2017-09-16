#include <algorithm>

#include "Query/Result.h"
#include "Query/SqlQuery.h"
#include "Storage/Common.h"

namespace {
const uint32 kContainerBufferLimit = 1 * Storage::kPageSize;
}

namespace Query {

// ************************* ResultContainer ******************************** //
ResultContainer::ResultContainer(SqlQuery* query) : query_(query) {
  tuple_meta_ = query_->mutable_tuple_meta();
}

ResultContainer::~ResultContainer() {
  if (materialized_) {
    //std::cout << "delete tmpfile " << ft_file_->filename() << std::endl;
    ft_file_->DeleteFile();
  }
}

std::shared_ptr<Storage::FlatTupleFile>
ResultContainer::CreateFlatTupleFile(
    const std::vector<std::string>& tables) const {
  if (query_) {
    return query_->CreateFlatTupleFile(tables);
  } else if (tuple_meta_) {
    Storage::FlatTupleFileOptions opts(*tuple_meta_, tables);
    opts.db_name = "RESERVED_DB__";
    opts.txn_id = 0;
    return std::make_shared<Storage::FlatTupleFile>(opts);
  } else {
    LogERROR("Could not create FlatTupleFile");
    return nullptr;
  }
}

ResultContainer::Iterator::Iterator(ResultContainer* results) :
    results_(results) {
  if (materialized()) {
    ftf_iterator_ = results_->ft_file_->GetIterator();
  }
}

std::shared_ptr<Tuple> ResultContainer::Iterator::GetNextTuple() {
  if (!materialized()) {
    if (crt_tindex_ < results_->tuples_.size()) {
      return results_->tuples_.at(crt_tindex_++);
    } else {
      return nullptr;
    }
  } else {
    return ftf_iterator_.NextTuple();
  }
}

ResultContainer::Iterator ResultContainer::GetIterator() {
  return Iterator(this);
}

bool ResultContainer::AddTuple(std::shared_ptr<Tuple> new_tuple) {
  if (!new_tuple->AddMeta(*tuple_meta_)) {
    return false;
  }

  if (tables_.empty()) {
    for (const auto& iter: new_tuple->records) {
      tables_.push_back(iter.first);
    }
  }

  if (!materialized_) {
    if (tuples_size_ + new_tuple->size() <= kContainerBufferLimit) {
      tuples_.push_back(new_tuple);
    } else {
      // Cache vector is full. Create FlatTupleFile and dump all tuples.
      ft_file_ = CreateFlatTupleFile(tables_);
      if (!ft_file_->InitForWriting()) {
        return false;
      }
      for (const auto& tuple : tuples_) {
        ft_file_->WriteTuple(*tuple);
      }
      materialized_ = true;
      if (!ft_file_->WriteTuple(*new_tuple)) {
        return false;
      }
    }
  } else {
    if (!ft_file_->WriteTuple(*new_tuple)) {
      return false;
    }
  }

  num_tuples_++;
  tuples_size_ += new_tuple->size();
  return true;
}

bool ResultContainer::AddTuple(const Tuple& tuple) {
  std::shared_ptr<Tuple> new_tuple(new Tuple(tuple));
  return AddTuple(new_tuple);
}

bool ResultContainer::AddTuple(Tuple&& tuple) {
  std::shared_ptr<Tuple> new_tuple(new Tuple(std::move(tuple)));
  return AddTuple(new_tuple);
}

bool ResultContainer::FinalizeAdding() {
  if (!materialized_) {
    return true;
  } else {
    return ft_file_->FinishWriting();
  }
}

bool ResultContainer::SortByColumns(const std::vector<Column>& columns) {
  if (!materialized_) {
    auto comparator = [&] (std::shared_ptr<Tuple> t1,
                           std::shared_ptr<Tuple> t2) {
      return Tuple::CompareBasedOnColumns(*t1, *t2, columns) < 0;
    };
    std::sort(tuples_.begin(), tuples_.end(), comparator);
    return true;
  } else {
    return ft_file_->Sort(columns);
  }
}

bool ResultContainer::SortByColumns(const std::string& table_name,
                                    const std::vector<uint32>& field_indexes) {
  std::vector<Column> columns;
  for (int index : field_indexes) {
    columns.emplace_back(table_name, "" /* column name doesn't matter */);
    columns.back().index = index;
  }

  return SortByColumns(columns);
}

bool ResultContainer::MergeSortResults(ResultContainer& result_1,
                                       ResultContainer& result_2,
                                       const std::vector<Column>& columns) {
  if (!result_1.SortByColumns(columns)) {
    return false;
  }
  if (!result_2.SortByColumns(columns)) {
    return false;
  }

  auto iter_1 = result_1.GetIterator();
  auto iter_2 = result_2.GetIterator();
  auto tuple_1 = iter_1.GetNextTuple();
  auto tuple_2 = iter_2.GetNextTuple();
  while (tuple_1 && tuple_2) {
    if (Tuple::CompareBasedOnColumns(*tuple_1, *tuple_1, columns) <= 0) {
      AddTuple(tuple_1);
      tuple_1 = iter_1.GetNextTuple();
    } else {
      AddTuple(tuple_2);
      tuple_2 = iter_2.GetNextTuple();
    }
  }

  while (tuple_1) {
    AddTuple(tuple_1);
    tuple_1 = iter_1.GetNextTuple();
  }
  while (tuple_2) {
    AddTuple(tuple_2);
    tuple_2 = iter_2.GetNextTuple();
  }

  return FinalizeAdding();
}

bool ResultContainer::MergeSortResults(
    ResultContainer& result_1,
    ResultContainer& result_2,
    const std::string& table_name,
    const std::vector<uint32>& field_indexes) {
  std::vector<Column> columns;
  for (int index : field_indexes) {
    columns.emplace_back(table_name, "" /* column name doesn't matter */);
    columns.back().index = index;
  }

  return MergeSortResults(result_1, result_2, columns);
}

bool ResultContainer::MergeSortResultsRemoveDup(
    ResultContainer& result_1, ResultContainer& result_2,
    const std::vector<Column>& columns) {
  if (!result_1.SortByColumns(columns)) {
    return false;
  }
  if (!result_2.SortByColumns(columns)) {
    return false;
  }

  auto iter_1 = result_1.GetIterator();
  auto iter_2 = result_2.GetIterator();
  auto tuple_1 = iter_1.GetNextTuple();
  auto tuple_2 = iter_2.GetNextTuple();
  std::shared_ptr<Tuple> last_tuple;
  while (tuple_1 && tuple_2) {
    int re = Tuple::CompareBasedOnColumns(*tuple_1, *tuple_2, columns);
    if (re <= 0) {
      if (!last_tuple ||
          (last_tuple &&
           Tuple::CompareBasedOnColumns(*last_tuple, *tuple_1, columns) != 0)) {
        AddTuple(tuple_1);
        last_tuple = tuple_1;
      }
      tuple_1 = iter_1.GetNextTuple();
    } else {
      if (!last_tuple ||
          (last_tuple &&
           Tuple::CompareBasedOnColumns(*last_tuple, *tuple_2, columns) != 0)) {
        AddTuple(tuple_2);
        last_tuple = tuple_2;
      }
      tuple_2 = iter_2.GetNextTuple();
    }
  }

  while (tuple_1) {
    if (!last_tuple ||
        (last_tuple &&
         Tuple::CompareBasedOnColumns(*last_tuple, *tuple_1, columns)) != 0) {
      AddTuple(tuple_1);
      last_tuple = tuple_1;
    }
    tuple_1 = iter_1.GetNextTuple();
  }
  while (tuple_2) {
    if (!last_tuple ||
        (last_tuple &&
         Tuple::CompareBasedOnColumns(*last_tuple, *tuple_2, columns) != 0)) {
      AddTuple(tuple_2);
      last_tuple = tuple_2;
    }
    tuple_2 = iter_2.GetNextTuple();
  }

  return FinalizeAdding();
}

bool ResultContainer::MergeSortResultsRemoveDup(
    ResultContainer& result_1, ResultContainer& result_2,
    const std::string& table_name, const std::vector<uint32>& field_indexes) {
  std::vector<Column> columns;
  for (int index : field_indexes) {
    columns.emplace_back(table_name, "" /* column name doesn't matter */);
    columns.back().index = index;
  }

  return MergeSortResultsRemoveDup(result_1, result_2, columns);
}

void ResultContainer::reset() {
  query_ = nullptr;
  tuple_meta_ = nullptr;
  tables_.clear();

  tuples_.clear();
  ft_file_.reset();

  num_tuples_ = 0;
  tuples_size_ = 0;

  materialized_ = false;
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
