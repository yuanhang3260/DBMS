#include <algorithm>

#include "Query/Tuple.h"

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
    CHECK(index < record->NumFields(),
          "index %d out of range %d", index, record->NumFields());
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
  return const_cast<ResultRecord*>(GetTableRecord(table_name));
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
      LogERROR("Couldn't find meta for table %s",
               table_record_iter.first.c_str());
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

}  // namespace Query
