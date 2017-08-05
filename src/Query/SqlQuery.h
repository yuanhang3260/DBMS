#ifndef QUERY_SQL_QUERY_
#define QUERY_SQL_QUERY_

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
};

class SqlQuery {
 public:
  SqlQuery(DB::Database* db);

  void SetExprNode(std::shared_ptr<Query::ExprTreeNode> node);
  std::shared_ptr<Query::ExprTreeNode> GetExprNode();

  const std::set<std::string>& tables() const { return tables_; }
  std::string DefaultTable() const;

  bool AddTable(const std::string& table);
  bool AddColumn(const std::string& column);
  bool ParseTableColumn(const std::string& name, Column* column);

  DB::TableInfoManager* FindTable(const std::string& table);
  DB::FieldInfoManager* FindTableColumn(const Column& column);
  const ColumnRequest* FindColumnRequest(const Column& column);

  bool TableIsValid(const std::string& table);
  bool ColumnIsValid(const Column& column);

  bool FinalizeParsing();

  const Query::ExprTreeNode& expr_root() const { return *expr_node_; }
  const Query::FetchedResult& results() const { return expr_node_->results(); }

  // Generate physical plan for this query.
  const PhysicalPlan& PrepareQueryPlan();
  // Execute a SELECT query, returns the number of matching records.
  int ExecuteSelectQuery();

  void reset();
  std::string error_msg() const;
  void set_error_msg(const std::string& error_msg);

 private:
  bool GroupPhysicalQueries(ExprTreeNode* node);

  PhysicalPlan* GenerateQueryPhysicalPlan(ExprTreeNode* node);

  PhysicalPlan* PreGenerateUnitPhysicalPlan(ExprTreeNode* node);
  PhysicalPlan* GenerateUnitPhysicalPlan(ExprTreeNode* node);

  bool IsConstExpression(ExprTreeNode* node);

  void EvaluateQueryConditions(PhysicalPlan* physical_plan);

  int ExecuteSelectQueryFromNode(ExprTreeNode* node);

  int Do_ExecutePhysicalQuery(ExprTreeNode* node);

  DB::Database* db_ = nullptr;
  DB::CatalogManager* catalog_m_ = nullptr;

  std::shared_ptr<Query::ExprTreeNode> expr_node_;

  // Target tables.
  std::set<std::string> tables_;

  // Target columns.
  uint32 columns_num_ = 0;
  std::map<std::string, std::map<std::string, ColumnRequest>> columns_;

  std::string error_msg_;

  friend class QueryTest;
};

}

#endif  // QUERY_SQL_QUERY_
