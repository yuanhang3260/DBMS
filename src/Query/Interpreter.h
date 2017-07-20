#ifndef QUERY_INTERPRETER_H_
#define QUERY_INTERPRETER_H_

#include <map>
#include <memory>
#include <set>
#include <vector>

#include "Sql/parser.hh"
#include "Sql/scanner.hh"
#include "Query/Expression.h"

#include "Database/Catalog_pb.h"
#include "Database/CatalogManager.h"
#include "Storage/Record.h"

// autogenerated by Bison, don't panic
// if your IDE can't resolve it - call make first

namespace Query {

// This class is the interface for our scanner/lexer. The end user
// is expected to use this. It drives scanner/lexer. keeps
// parsed AST and generally is a good place to store additional
// context data. Both parser and lexer have access to it via internal 
// references.
class Interpreter {
 public:
  explicit Interpreter(DB::CatalogManager* catalog_m);

  // Parse SQL query, return 0 in success.
  bool Parse();
  bool Parse(const std::string& str);

  // Switch scanner input stream. Default is standard input (std::cin).
  void SwitchInputStream(std::istream *is);

  std::shared_ptr<Query::ExprTreeNode> GetCurrentNode() { return node_; }

  const std::set<std::string>& tables() const { return tables_; }

  void AddTable(const std::string& table);
  void AddColumn(const std::string& column);
  bool ParseTableColumn(const std::string& name, Column* column);

  bool TableIsValid(const std::string& table, std::string* error_msg) const;
  bool ColumnIsValid(const Column& column, std::string* error_msg) const;

  bool debug() const { return debug_; }
  void set_debug(bool d) { debug_ = d; }
  void reset();

 private:
  // Used internally by Scanner YY_USER_ACTION to update location indicator
  void IncreaseLocation(unsigned int loc);

  // Used to get last Scanner location. Used in error messages.
  unsigned int location() const;

  Sql::Scanner m_scanner;
  Sql::Parser m_parser;
  unsigned int m_location;  // Used by scanner

  DB::CatalogManager* catalog_m_ = nullptr;

  std::shared_ptr<Query::ExprTreeNode> node_;
  
  std::set<std::string> tables_;

  uint32 columns_num_ = 0;
  struct ColumnRequest {
    Column column;
    // This is the order of columns in the query. For example:
    //
    //   SELECT a, b, c, d FROM ...
    //
    // The 1st column is a, 2nd is b, etc.
    uint32 request_pos;
  };
  std::map<std::string, std::map<std::string, ColumnRequest>> columns_;

  std::string error_msg_;
  bool debug_ = false;

  // This is needed so that Scanner and Parser can call some methods that we
  // want to keep hidden from the end user.
  friend class Sql::Parser;
  friend class Sql::Scanner;
};

}  // namespace Query

#endif  // QUERY_INTERPRETER_H_
