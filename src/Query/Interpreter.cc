#include <sstream>

#include "Strings/Split.h"
#include "Strings/Utils.h"

#include "Query/Interpreter.h"

namespace Query {

Interpreter::Interpreter(DB::CatalogManager* catalog_m) :
    m_scanner(*this),
    m_parser(m_scanner, *this),
    m_location(0) {
  query_ = std::make_shared<SqlQuery>(catalog_m);
}

bool Interpreter::Parse() {
  m_location = 0;
  return m_parser.parse() == 0;
}

bool Interpreter::Parse(const std::string& str) {
  std::stringstream ss;
  ss << str;
  SwitchInputStream(&ss);
  m_location = 0;
  return m_parser.parse() == 0;
}

void Interpreter::SwitchInputStream(std::istream *is) {
  m_scanner.switch_streams(is, NULL);
}

void Interpreter::IncreaseLocation(unsigned int loc) {
  m_location += loc;
  // std::cout << "increaseLocation(): " << loc
  //           << ", total = " << m_location << std::endl;
}

unsigned int Interpreter::location() const {
  return m_location;
}

void Interpreter::reset() {
  m_location = 0;
  query_->reset();
}

}  // namespace Query
