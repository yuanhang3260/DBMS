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

class ResultContainer {
 public:
  // TODO: Random access should NOT be supported.
  std::shared_ptr<Tuple> GetTuple(uint32 index);

  void InitReading() { crt_tindex_ = 0; }
  std::shared_ptr<Tuple> GetNextTuple();

  bool AddTuple(std::shared_ptr<Tuple> tuple);
  bool AddTuple(const Tuple& tuple);
  bool AddTuple(Tuple&& tuple);

  uint32 NumTuples() const { return tuples_.size(); }

  void SetTupleMeta(TupleMeta* tuple_meta) { tuple_meta_ = tuple_meta; }
  TupleMeta* GetTupleMeta() { return tuple_meta_; }

  void reset();

  // Sort tuples by given columns.
  void SortByColumns(const std::vector<Column>& columns);
  void SortByColumns(const std::string& table_name,
                     const std::vector<uint32>& field_indexes);

  // Take two set of results, sort and merge them by columns.
  void MergeSortResults(ResultContainer& result_1,
                        ResultContainer& result_2,
                        const std::vector<Column>& columns);

  void MergeSortResults(ResultContainer& result_1,
                        ResultContainer& result_2,
                        const std::string& table_name,
                        const std::vector<uint32>& field_indexes);

  void MergeSortResultsRemoveDup(ResultContainer& result_1,
                                 ResultContainer& result_2,
                                 const std::vector<Column>& columns);

  void MergeSortResultsRemoveDup(ResultContainer& result_1,
                                 ResultContainer& result_2,
                                 const std::string& table_name,
                                 const std::vector<uint32>& field_indexes);

 private:
  TupleMeta* tuple_meta_;

  std::vector<std::shared_ptr<Tuple>> tuples_;

  uint32 num_tuples_ = 0;
  uint32 crt_tindex_ = 0;

  bool materialized_ = false;
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
