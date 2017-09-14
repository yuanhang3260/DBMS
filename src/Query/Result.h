#ifndef QUERY_RESULT_H_
#define QUERY_RESULT_H_

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

struct FetchedResult {
  using Tuple = std::map<std::string, ResultRecord>;
  using TupleMeta = std::map<std::string, TableRecordMeta>;

  std::vector<Tuple> tuples;
  TupleMeta* tuple_meta;

  bool AddTuple(const Tuple& tuple);
  bool AddTuple(Tuple&& tuple);

  static uint32 TupleSize(const Tuple& tuple);
  static void PrintTuple(const Tuple& tuple);

  static bool AddTupleMeta(Tuple* tuple, TupleMeta* meta);

  static std::shared_ptr<Tuple> MergeTuples(const Tuple& t1, const Tuple& t2);

  int NumTuples() const { return tuples.size(); }

  void reset();

  static int CompareBasedOnColumns(
      const Tuple& t1, const std::vector<Column>& columns_1,
      const Tuple& t2, const std::vector<Column>& columns_2);
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

// Result aggregation.
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

}  // namespace Query

#endif  // QUERY_RESULT_H_
