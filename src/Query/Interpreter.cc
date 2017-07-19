#include <sstream>

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
  table_list_.clear();
  column_list_.clear();
  node_.reset();
}

void Interpreter::AddTable(const std::string& table) {
  table_list_.push_back(table);
}

void Interpreter::AddColumn(const std::string& column) {
  if (column_list_.size() == 1 && column_list_.at(0) == "*") {
    return;
  }

  if (column == "*") {
    column_list_.clear();
    column_list_.push_back("*");
  } else {
    column_list_.push_back(column);
  }
}

}  // namespace Query
