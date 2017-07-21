#ifndef QUERY_SQL_QUERY_
#define QUERY_SQL_QUERY_

#include <map>
#include <memory>
#include <set>
#include <vector>

#include "Database/Catalog_pb.h"
#include "Database/CatalogManager.h"
#include "Query/Common.h"
#include "Query/Expression.h"

namespace Query {

struct ColumnRequest {
  Column column;
  // This is the order of columns in the query. For example:
  //
  //   SELECT a, b, c, d FROM ...
  //
  // The 1st column is a, 2nd is b, etc.
  uint32 request_pos;
};

class SqlQuery {
 public:
  SqlQuery(DB::CatalogManager* catalog_m);

  void SetExprNode(std::shared_ptr<Query::ExprTreeNode> node);
  std::shared_ptr<Query::ExprTreeNode> GetExprNode();

  const std::set<std::string>& tables() const { return tables_; }
  std::string DefaultTable() const;

  bool AddTable(const std::string& table);
  bool AddColumn(const std::string& column);
  bool ParseTableColumn(const std::string& name, Column* column);

  DB::TableInfoManager* FindTable(const std::string& table);
  DB::FieldInfoManager* FindTableColumn(const Column& column);

  bool TableIsValid(const std::string& table);
  bool ColumnIsValid(const Column& column);

  void reset();
  std::string error_msg() const;
  void set_error_msg(const std::string& error_msg);

 private:
  DB::CatalogManager* catalog_m_ = nullptr;

  std::shared_ptr<Query::ExprTreeNode> expr_node_;
  
  // Target tables.
  std::set<std::string> tables_;

  // Target columns.
  uint32 columns_num_ = 0;
  std::map<std::string, std::map<std::string, ColumnRequest>> columns_;

  std::string error_msg_;
};

}

#endif  // QUERY_SQL_QUERY_
