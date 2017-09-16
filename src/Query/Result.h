#ifndef QUERY_RESULT_H_
#define QUERY_RESULT_H_

#include "Base/MacroUtils.h"

#include "Database/Catalog_pb.h"
#include "Query/Common.h"
#include "Query/Tuple.h"
#include "Storage/FlatTupleFile.h"

namespace Query {

class SqlQuery;

// This is a generic tuple container for query result. Tuples can be either
// stored in memory or materialized to FlatTupleFile if their total size exceeds
// limit. All these details are transparent to user. The interface is AddTuple
// to add new tuple into container, and forward-only iterator to access tuples.
class ResultContainer {
 public:
  ResultContainer() = default;
  ResultContainer(SqlQuery* query);
  ~ResultContainer();

  bool AddTuple(std::shared_ptr<Tuple> tuple);
  bool AddTuple(const Tuple& tuple);
  bool AddTuple(Tuple&& tuple);
  bool FinalizeAdding();

  uint32 NumTuples() const { return num_tuples_; }

  void SetTupleMeta(TupleMeta* tuple_meta) { tuple_meta_ = tuple_meta; }
  TupleMeta* GetTupleMeta() { return tuple_meta_; }

  void reset();

  // Sort tuples by given columns.
  bool SortByColumns(const std::vector<Column>& columns);
  bool SortByColumns(const std::string& table_name,
                     const std::vector<uint32>& field_indexes);

  // Take two set of results, sort and merge them by columns.
  bool MergeSortResults(ResultContainer& result_1,
                        ResultContainer& result_2,
                        const std::vector<Column>& columns);

  bool MergeSortResults(ResultContainer& result_1,
                        ResultContainer& result_2,
                        const std::string& table_name,
                        const std::vector<uint32>& field_indexes);

  bool MergeSortResultsRemoveDup(ResultContainer& result_1,
                                 ResultContainer& result_2,
                                 const std::vector<Column>& columns);

  bool MergeSortResultsRemoveDup(ResultContainer& result_1,
                                 ResultContainer& result_2,
                                 const std::string& table_name,
                                 const std::vector<uint32>& field_indexes);

  class Iterator {
   public:
    Iterator() = default;
    Iterator(ResultContainer* results);

    std::shared_ptr<Tuple> GetNextTuple();

   private:
    bool materialized() const { return results_->materialized_; }

    ResultContainer* results_ = nullptr;

    // For in-memory container, with tuples stored in vector.
    uint32 crt_tindex_ = 0;

    Storage::FlatTupleFile::Iterator ftf_iterator_;
  };

  Iterator GetIterator();

 private:
  std::shared_ptr<Storage::FlatTupleFile>
  CreateFlatTupleFile(const std::vector<std::string>& tables) const;

  SqlQuery* query_ = nullptr;
  TupleMeta* tuple_meta_ = nullptr;
  // Which tables' records are contained in this result set. This field is
  // self-learning when AddTuple() is called.
  std::vector<std::string> tables_;

  std::vector<std::shared_ptr<Tuple>> tuples_;
  std::shared_ptr<Storage::FlatTupleFile> ft_file_;

  uint32 num_tuples_ = 0;
  uint32 tuples_size_ = 0;

  bool materialized_ = false;

  FORBID_COPY_AND_ASSIGN(ResultContainer);
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
