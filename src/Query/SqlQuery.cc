#include "Strings/Split.h"
#include "Strings/Utils.h"

#include "Query/SqlQuery.h"

namespace Query {

SqlQuery::SqlQuery(DB::CatalogManager* catalog_m) : catalog_m_(catalog_m) {}

void SqlQuery::reset() {
  expr_node_.reset();
  tables_.clear();
  columns_.clear();
  columns_num_ = 0;
}

void SqlQuery::SetExprNode(std::shared_ptr<Query::ExprTreeNode> node) {
  expr_node_ = node;
}

std::shared_ptr<Query::ExprTreeNode> SqlQuery::GetExprNode() {
  return expr_node_;
}

std::string SqlQuery::DefaultTable() const {
  if (tables_.size() == 1) {
    return *tables_.begin();
  }
  return "";
}

bool SqlQuery::AddTable(const std::string& table) {
  tables_.insert(table);
  return true;
}

bool SqlQuery::AddColumn(const std::string& name) {
  Column column;
  if (!ParseTableColumn(name, &column)) {
    return false;
  }

  auto& table_columns = columns_[column.table_name];
  if (table_columns.size() == 1 &&
      table_columns.begin()->first == "*") {
    return true;
  }

  if (column.column_name == "*") {
    table_columns.clear();
  }

  ColumnRequest column_request;
  column_request.column = column;
  column_request.request_pos = ++columns_num_;
  table_columns.emplace(column.column_name, column_request);

  return true;
}

bool SqlQuery::ParseTableColumn(const std::string& name, Column* column) {
  auto re = Strings::Split(name, ".");
  if (re.size() == 1) {
    column->column_name = re.at(0);
  } else if (re.size() == 2) {
    column->table_name = re.at(0);
    column->column_name = re.at(1);
  } else {
    error_msg_ = Strings::StrCat("Invalid column name ", name);
    return false;
  }
  return true;
}

bool SqlQuery::TableIsValid(const std::string& table_name) {
  auto table_m = catalog_m_->FindTableByName(table_name);
  if (table_m == nullptr) {
    error_msg_ = Strings::StrCat("Invalid table \"", table_name, "\"");
    return false;
  }

  return true;
}

bool SqlQuery::ColumnIsValid(const Column& column) {
  auto table_m = catalog_m_->FindTableByName(column.table_name);
  if (table_m == nullptr) {
    error_msg_ = Strings::StrCat("Invalid table \"", column.table_name, "\"");
    //LogERROR("ERROR - %s", error_msg_.c_str());
    return false;
  }

  if (column.column_name == "*") {
    return true;
  }

  auto field_m = table_m->FindFieldByName(column.column_name);
  if (field_m == nullptr) {
    error_msg_ = Strings::StrCat("Invalid column \"", column.column_name,
                                 "\" in table \"", column.table_name, "\"");
    //LogERROR("ERROR - %s", error_msg_.c_str());
    return false;
  }

  return true;
}

DB::TableInfoManager* SqlQuery::FindTable(const std::string& table) {
  auto re = catalog_m_->FindTableByName(table);
  if (re == nullptr) {
    error_msg_ = Strings::StrCat("Invalid table \"", table, "\"");
  }
  return re;
}

DB::FieldInfoManager* SqlQuery::FindTableColumn(const Column& column) {
  if (column.table_name.empty()) {
    error_msg_ = Strings::StrCat("Ambiguous column \"", column.column_name,
                                 "\", table name needed");
    //LogERROR("ERROR - %s", error_msg_.c_str());
    return nullptr;
  }

  auto re = catalog_m_->FindTableFieldByName(column);
  if (re == nullptr) {
    error_msg_ = Strings::StrCat("Invalid column \"", column.column_name,
                                 "\" in table \"", column.table_name, "\"");
    //LogERROR("ERROR - %s", error_msg_.c_str());
  }
  return re;
}

std::string SqlQuery::error_msg() const {
  return error_msg_;
}

void SqlQuery::set_error_msg(const std::string& error_msg) {
  error_msg_ = error_msg;
}

}  // namespace Query
