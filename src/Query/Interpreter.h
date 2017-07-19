#ifndef QUERY_INTERPRETER_H_
#define QUERY_INTERPRETER_H_

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
  bool parse();
  bool parse(const std::string& str);

  std::string str() const;
  void clear();

  //Switch scanner input stream. Default is standard input (std::cin).
  void switchInputStream(std::istream *is);

  std::shared_ptr<Query::ExprTreeNode> GetCurrentNode() { return node_; }
  const std::vector<std::string>& table_list() const { return table_list_; }
  const std::vector<std::string>& column_list() const { return column_list_; }

  void AddTable(const std::string& table);
  void AddColumn(const std::string& column);

  //This is needed so that Scanner and Parser can call some
  //methods that we want to keep hidden from the end user.
  friend class Sql::Parser;
  friend class Sql::Scanner;

  bool debug() const { return debug_; }
  void set_debug(bool d) { debug_ = d; }
  void reset();

 private:
  // Used internally by Scanner YY_USER_ACTION to update location indicator
  void increaseLocation(unsigned int loc);

  // Used to get last Scanner location. Used in error messages.
  unsigned int location() const;

  Sql::Scanner m_scanner;
  Sql::Parser m_parser;
  unsigned int m_location;  // Used by scanner

  DB::CatalogManager* catalog_m_ = nullptr;

  std::shared_ptr<Query::ExprTreeNode> node_;
  std::vector<std::string> table_list_;
  std::vector<std::string> column_list_;
  bool debug_ = false;
};

}  // namespace Query

#endif  // QUERY_INTERPRETER_H_
