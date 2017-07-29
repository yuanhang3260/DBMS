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

  ColumnRequest column_request;
  column_request.column = column;
  column_request.request_pos = ++columns_num_;

  auto& table_columns = columns_[column.table_name];
  if (table_columns.find(column.column_name) != table_columns.end()) {
    return true;
  }
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

const ColumnRequest* SqlQuery::FindColumnRequest(const Column& column) {
  auto iter = columns_.find(column.table_name);
  if (iter == columns_.end()) {
    return nullptr;
  }

  auto columns = iter->second;
  auto column_request_iter = columns.find(column.column_name);
  if (column_request_iter == columns.end()) {
    return nullptr;
  }

  return &(column_request_iter->second);
}

bool SqlQuery::FinalizeParsing() {
  // Apply default table.
  std::string default_table = DefaultTable();
  auto iter = columns_.find("");
  if (iter != columns_.end()) {
    if (default_table.empty()) {
      error_msg_ = Strings::StrCat("Default table not found.");
      return false;
    }
    auto& columns = iter->second;
    for (auto& column_request_iter : columns) {
      column_request_iter.second.column.table_name = default_table;
      auto& table_columns = columns_[default_table];
      if (table_columns.find(column_request_iter.first) !=
          table_columns.end()) {
        continue;
      }
      table_columns.emplace(column_request_iter.first,
                            column_request_iter.second);
    }
    columns_.erase(iter);
  }

  // Expand * columns
  for (auto& columns_iter : columns_) {
    const auto& table_name = columns_iter.first;
    auto& columns = columns_iter.second;
    auto iter = columns.find("*");
    if (iter == columns.end()) {
      continue;
    }
    auto table_m = FindTable(table_name);
    CHECK(table_m != nullptr, Strings::StrCat("Can't find table ", table_name));
    for (uint32 i = 0; i < table_m->NumFields(); i++) {
      auto field_m = table_m->FindFieldByIndex(i);
      CHECK(field_m != nullptr,
            Strings::StrCat("Can't find field ", std::to_string(i),
                            " in table ", table_name));
      if (columns.find(field_m->name()) != columns.end()) {
        continue;
      }
      ColumnRequest column_request;
      column_request.column.table_name = table_name;
      column_request.column.column_name = field_m->name();
      column_request.request_pos = iter->second.request_pos;
      column_request.sub_request_pos = i;
      columns.emplace(field_m->name(), column_request);
    }
    columns.erase(iter);
  }

  // Re-assign column request positions.
  std::vector<ColumnRequest*> columns_list;
  for (auto& iter : columns_) {
    for (auto& column_request_iter : iter.second) {
      columns_list.push_back(&column_request_iter.second);
    }
  }

  auto comparator = [&] (const ColumnRequest* c1,
                         const ColumnRequest* c2) {
    if (c1->request_pos < c2->request_pos) {
      return true;
    } else if (c1->request_pos > c2->request_pos) {
      return false;
    } else {
      if (c1->sub_request_pos < c2->sub_request_pos) {
        return true;
      } else if (c1->sub_request_pos > c2->sub_request_pos) {
        return false;
      } else {
        return false;
      }
    }
  };

  std::sort(columns_list.begin(), columns_list.end(), comparator);
  for (uint32 i = 0; i < columns_list.size(); i++) {
    columns_list.at(i)->request_pos = i;
  }

  // Check all tables and columns are valid.
  for (const auto& table_name : tables_) {
    if (!TableIsValid(table_name)) {
      return false;
    }
  }

  for (const auto& iter : columns_) {
    auto columns = iter.second;
    for (auto& column_request_iter : columns) {
      if (!ColumnIsValid(column_request_iter.second.column)) {
        return false;
      }
    }
  }

  return true;
}

std::string SqlQuery::error_msg() const {
  return error_msg_;
}

void SqlQuery::set_error_msg(const std::string& error_msg) {
  error_msg_ = error_msg;
}

}  // namespace Query
