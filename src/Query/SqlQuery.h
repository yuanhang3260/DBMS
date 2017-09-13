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
#include "Query/ExecutePlan.h"
#include "Query/NodeValue.h"
#include "Query/Result.h"

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

  // Execute a SELECT query, returns the number of matching records.
  int ExecuteSelectQuery();
  // Execute a JOIN query, returns the number of matching records.
  int ExecuteJoinQuery();

  // Aggregate rows if there is SUM(), AVG(), COUNT(), etc.
  void AggregateResults();

  // Print results.
  void PrintResults();

  void reset();
  std::string error_msg() const;
  void set_error_msg(const std::string& error_msg);

 private:
  // Generate physical plan for this query.
  const PhysicalPlan& PrepareQueryPlan();
  const PhysicalPlan& PrepareQueryPlan(ExprTreeNode* node);

  // Group physical query units.
  bool GroupPhysicalQueries(ExprTreeNode* node);
  // Generate physical plan for the entire query.
  PhysicalPlan* GenerateQueryPhysicalPlan(ExprTreeNode* node);

  // Generate physical plan for a physical query unit.
  PhysicalPlan* GenerateUnitPhysicalPlan(ExprTreeNode* node);
  PhysicalPlan* PreGenerateUnitPhysicalPlan(ExprTreeNode* node);
  void EvaluateQueryConditions(PhysicalPlan* physical_plan);

  // Iterators for pipelined execution.
  void CreateIteratorsRecursive(ExprTreeNode* node);

  // (Deprecated) Static execution of select query.
  int ExecuteSelectQueryFromNode(ExprTreeNode* node);
  int Do_ExecutePhysicalQuery(ExprTreeNode* node);

  bool IsConstExpression(ExprTreeNode* node);

  // Analyze join expression - split the original complete JOIN query expression
  // to conditions for saprate tables and JOIN expr.
  //
  // Example:
  //   SELECT * from t1, t2 WHERE t1.a > 3 AND t2.b = 6 AND t1.c = t2.c
  //
  // table 1 will get expr: t1.a > 3
  // table 2 will get expr: t2.b = 6
  // JOIN expr is: t1.c = t2.c
  struct JoinQueryConditionGroups {
    std::vector<std::shared_ptr<ExprTreeNode>> table_1_exprs;
    std::vector<std::shared_ptr<ExprTreeNode>> table_2_exprs;
    std::vector<std::shared_ptr<ExprTreeNode>> join_exprs;
    bool discard = false;
  };
  std::shared_ptr<SqlQuery::JoinQueryConditionGroups>
  GroupJoinQueryConditions(std::shared_ptr<ExprTreeNode> node);

  std::shared_ptr<SqlQuery::JoinQueryConditionGroups>
  AnalyzeUnitJoinCondition(std::shared_ptr<ExprTreeNode> node);

  // Do nested block loop join.
  void NestedLoopJoin(const JoinQueryConditionGroups& exprs);

  // Build expression tree for a single table based on list of sub-conditions.
  // All sub-conditions are logically connected by AND.
  std::shared_ptr<ExprTreeNode> BuildExpressTree(
      const std::string& table_name,
      const std::vector<std::shared_ptr<ExprTreeNode>>& exprs);

  // Get tables that are referenced in an expression.
  std::vector<std::string> GetExprTables(ExprTreeNode* node);

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
