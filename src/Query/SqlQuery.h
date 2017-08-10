#ifndef QUERY_SQL_QUERY_H_
#define QUERY_SQL_QUERY_H_

#include <map>
#include <memory>
#include <set>
#include <vector>

#include "Database/Catalog_pb.h"
#include "Database/CatalogManager.h"
#include "Database/Database.h"
#include "Query/Common.h"
#include "Query/Expression.h"

namespace Database {
class Database;
}

namespace Query {

struct ColumnRequest {
  Column column;
  // This is the order of columns in the query. For example:
  //
  //   SELECT a, b, c, d FROM ...
  //
  // The 1st column is a, 2nd is b, etc.
  uint32 request_pos;
  // Only used for *-expanded columns of a table, to preserve its intrinsic
  // fields order.
  uint32 sub_request_pos = 0;

  // Column print size is (field print max size + 2)
  uint32 print_width = 0;

  std::string print_name;

  AggregationType aggregation_type = NO_AGGREGATION;

  bool operator==(const ColumnRequest& other) const {
    return column == other.column &&
           aggregation_type == other.aggregation_type;
  }

  bool operator<(const ColumnRequest& other) const {
    if (column != other.column) {
      return column < other.column;
    } else if (aggregation_type != other.aggregation_type) {
      return aggregation_type < other.aggregation_type;
    }
    return false;
  }

  std::string AsString(bool use_table_prefix) const;
};

class SqlQuery {
 public:
  SqlQuery(DB::Database* db);

  void SetExprNode(std::shared_ptr<Query::ExprTreeNode> node);
  std::shared_ptr<Query::ExprTreeNode> GetExprNode();

  DB::Database* GetDB() { return db_; }
  const std::set<std::string>& tables() const { return tables_; }
  std::string DefaultTable() const;
  FetchedResult::TupleMeta* mutable_tuple_meta();

  bool AddTable(const std::string& table);
  bool AddColumn(const std::string& column);
  bool AddColumn(const std::string& column, AggregationType aggregation_type);
  bool AddOrderByColumn(const std::string& column);
  bool AddOrderByColumn(const std::string& column,
                        AggregationType aggregation_type);
  bool AddGroupByColumn(const std::string& column);
  bool ParseTableColumn(const std::string& name, Column* column);

  DB::TableInfoManager* FindTable(const std::string& table);
  DB::FieldInfoManager* FindTableColumn(const Column& column);
  const ColumnRequest* FindColumnRequest(const Column& column) const;
  const ColumnRequest* FindColumnRequest(
      const Column& column, AggregationType aggregation_type) const;

  bool TableIsValid(const std::string& table);
  bool ColumnIsValid(const Column& column);

  bool FinalizeParsing();

  const Query::ExprTreeNode& expr_root() const { return *expr_node_; }
  const Query::FetchedResult& results() const { return results_; }

  // Generate physical plan for this query.
  const PhysicalPlan& PrepareQueryPlan();

  // Execute a SELECT query, returns the number of matching records.
  int ExecuteSelectQuery();

  // Aggregate rows if there is SUM(), AVG(), COUNT(), etc.
  void AggregateResults();

  // Print results.
  void PrintResults();

  void reset();
  std::string error_msg() const;
  void set_error_msg(const std::string& error_msg);

 private:
  bool GroupPhysicalQueries(ExprTreeNode* node);

  PhysicalPlan* GenerateQueryPhysicalPlan(ExprTreeNode* node);

  PhysicalPlan* PreGenerateUnitPhysicalPlan(ExprTreeNode* node);
  PhysicalPlan* GenerateUnitPhysicalPlan(ExprTreeNode* node);
  void EvaluateQueryConditions(PhysicalPlan* physical_plan);

  void CreateIteratorsRecursive(ExprTreeNode* node);

  int ExecuteSelectQueryFromNode(ExprTreeNode* node);

  bool IsConstExpression(ExprTreeNode* node);

  int Do_ExecutePhysicalQuery(ExprTreeNode* node);

  DB::Database* db_ = nullptr;
  DB::CatalogManager* catalog_m_ = nullptr;

  std::shared_ptr<ExprTreeNode> expr_node_;

  // Target tables.
  std::set<std::string> tables_;

  // Target columns.
  uint32 columns_num_ = 0;
  uint32 aggregated_columns_num_ = 0;
  std::set<ColumnRequest> columns_set_;
  std::set<ColumnRequest> columns_unknown_table_;
  std::set<ColumnRequest> star_columns_;
  std::vector<ColumnRequest> columns_;

  std::vector<ColumnRequest> order_by_columns_;
  std::vector<Column> group_by_columns_;

  // Meta data for each table's record in the result.
  FetchedResult::TupleMeta tuple_meta_;

  // Final result.
  FetchedResult results_;

  std::string error_msg_;

  friend class QueryTest;
};

}

#endif  // QUERY_SQL_QUERY_H_
