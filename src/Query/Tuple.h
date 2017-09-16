#ifndef QUERY_TUPLE_H_
#define QUERY_TUPLE_H_

#include "Database/Catalog_pb.h"
#include "Query/Common.h"

namespace Query {

// Result.
struct TableRecordMeta {
  std::vector<DB::TableField> fetched_fields;
  Storage::RecordType record_type = Storage::DATA_RECORD;

  void CreateDataRecordMeta(const DB::TableInfo& schema);
  void CreateIndexRecordMeta(const DB::TableInfo& schema,
                             const std::vector<uint32>& field_indexes);
};

struct ResultRecord {
  ResultRecord() = default;
  ResultRecord(std::shared_ptr<Storage::RecordBase> record_) :
      record(record_) {}

  std::shared_ptr<Storage::RecordBase> record;
  const TableRecordMeta* meta = nullptr;

  Storage::RecordType record_type() const;

  const Schema::Field* GetField(uint32 index) const;
  Schema::Field* MutableField(uint32 index);
  // Takes ownership of the argument.
  void AddField(Schema::Field* field);
};

using TupleMeta = std::map<std::string, TableRecordMeta>;

struct Tuple {
  // Table name -> ResultRecord
  std::map<std::string, ResultRecord> records;

  uint32 size() const;
  void Print() const;

  const ResultRecord* GetTableRecord(const std::string& table_name) const;
  ResultRecord* MutableTableRecord(const std::string& table_name);
  bool AddTableRecord(const std::string& table_name,
                      std::shared_ptr<Storage::RecordBase> record);
  bool AddMeta(const TupleMeta& meta);

  static std::shared_ptr<Tuple> MergeTuples(const Tuple& t1, const Tuple& t2);

  static int CompareBasedOnColumns(
      const Tuple& t1, const std::vector<Column>& columns_1,
      const Tuple& t2, const std::vector<Column>& columns_2);
  static int CompareBasedOnColumns(
      const Tuple& t1, const Tuple& t2, const std::vector<Column>& columns);
};

}  // namespace Query

#endif  // QUERY_TUPLE_H_
