#include <sstream>

#include "Strings/Split.h"
#include "Strings/Utils.h"

#include "Query/Interpreter.h"

namespace Query {

Interpreter::Interpreter(DB::CatalogManager* catalog_m) :
  m_scanner(*this),
  m_parser(m_scanner, *this),
  m_location(0),
  catalog_m_(catalog_m) {}

bool Interpreter::parse() {
  m_location = 0;
  return m_parser.parse() == 0;
}

bool Interpreter::parse(const std::string& str) {
  std::stringstream ss;
  ss << str;
  switchInputStream(&ss);
  m_location = 0;
  return m_parser.parse() == 0;
}

void Interpreter::clear() {
  m_location = 0;
}

std::string Interpreter::str() const {
  return "";
}

void Interpreter::switchInputStream(std::istream *is) {
  m_scanner.switch_streams(is, NULL);
}

void Interpreter::increaseLocation(unsigned int loc) {
  m_location += loc;
  std::cout << "increaseLocation(): " << loc
            << ", total = " << m_location << std::endl;
}

unsigned int Interpreter::location() const {
  return m_location;
}

void Interpreter::reset() {
  tables_.clear();
  columns_.clear();
  node_.reset();
}

void Interpreter::AddTable(const std::string& table) {
  tables_.insert(table);
}

void Interpreter::AddColumn(const std::string& name) {
  Column column;
  ParseTableColumn(name, &column);

  auto& table_columns = columns_[column.table_name];
  if (table_columns.size() == 1 && *table_columns.begin() == "*") {
    return;
  }

  if (column.column_name == "*") {
    table_columns.clear();
    table_columns.insert("*");
  } else {
    
    table_columns.insert(column.column_name);
  }

  return;
}

bool Interpreter::ParseTableColumn(const std::string& name, Column* column) {
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

bool Interpreter::TableIsValid(
    const std::string& table_name, std::string* error_msg) const {
  auto table_m = catalog_m_->FindTableByName(table_name);
  if (table_m == nullptr) {
    if (error_msg) {
      *error_msg = Strings::StrCat("Invalid table \"", table_name, "\"");
    }
    return false;
  }

  return true;
}

bool Interpreter::ColumnIsValid(
    const Column& column, std::string* error_msg) const {
  auto table_m = catalog_m_->FindTableByName(column.table_name);
  if (table_m == nullptr) {
    if (error_msg) {
      *error_msg = Strings::StrCat("Invalid table \"", column.table_name, "\"");
    }
    return false;
  }

  if (column.column_name == "*") {
    return true;
  }

  auto field_m = table_m->FindFieldByName(column.column_name);
  if (field_m == nullptr) {
    if (error_msg) {
      *error_msg = Strings::StrCat("Invalid column \"", column.column_name,
                                   "\" in table \"", column.table_name, "\"");
    }
    return false;
  }

  return true;
}

}  // namespace Query
