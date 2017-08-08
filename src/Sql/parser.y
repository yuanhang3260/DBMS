// Bison parser for SQL

%skeleton "lalr1.cc" /* -*- C++ -*- */
%require "3.0"
%defines
%define parser_class_name { Parser }

%define api.token.constructor
%define api.value.type variant
%define parse.assert
%define api.namespace { Sql }
%code requires
{
#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <stdint.h>

#include "Base/BaseTypes.h"
#include "Strings/Utils.h"

#include "Query/Expression.h"

using namespace std;

namespace Sql {
class Scanner;
}

namespace Query {
class Interpreter;
}

}

// Bison calls yylex() function that must be provided by us to suck tokens
// from the scanner. This block will be placed at the beginning of
// IMPLEMENTATION file (cpp).
// 
// We define this function here (function! not method).
// This function is called only inside Bison, so we make it static to limit
// symbol visibility for the linker to avoid potential linking conflicts.
%code top
{
#include <iostream>

#include "scanner.hh"
#include "parser.hh"
#include "location.hh"

#include "Query/Interpreter.h"

// yylex() can take user arguments, defined by %parse-param below.
//
// scanner.get_next_token() is defined to macro YY_DECL, whose body is defined
// in the generated scanner.cpp. This is the most critical function generated
// by lexer, which does all heavy work.
//
// For more explaination, the magic here is:
//
//   1. YY_DECL must be #defined a function as: Sql::Parser::symbol_type f(void)
//      which is implemented in the generated lexer cpp code.
//
//   2. yylex is #defined or function-defined. It must call YY_DECL, maybe as
//      simple as just a YY_DECL inside, or take additional user arguments
//      (e.g. Interpreter) and do more complicated work.
static Sql::Parser::symbol_type yylex(Sql::Scanner &scanner,
                                      Query::Interpreter &driver) {
  // Is driver unused? Probably not. Maybe it can be used to record errors in
  // lexer token parsing.
  return scanner.get_next_token();
}

// you can accomplish the same thing by inlining the code using preprocessor
// x and y are same as in above static function
// #define yylex(scanner, driver) scanner.get_next_token()

#define ABORT_PARSING  \
    driver.node_.reset();  \
    YYABORT;  \
}

%lex-param { Sql::Scanner &scanner }
%lex-param { Query::Interpreter &driver }
%parse-param { Sql::Scanner &scanner }
%parse-param { Query::Interpreter &driver }
%locations
%define parse.trace
%define parse.error verbose

%define api.token.prefix {TOKEN_}

%token END 0 "EOF"

%token <int64> INTEGER "Integer";
%token <double> DOUBLE "Double";
%token <std::string> STRING  "String";
%token <char> CHAR  "Char";
%token <bool> BOOL  "Boolean";

%token ADD "+";
%token SUB "-";
%token MUL "*";
%token DIV "/";
%token MOD "%%";
%token <std::string> COMPARATOR1  "Comparator1";
%token <std::string> COMPARATOR2  "Comparator2";

%token AND "AND";
%token OR "OR";
%token NOT "NOR";

%token <std::string> IDENTIFIER  "Identifier";

%token SELECT "SELECT";
%token FROM "FROM";
%token WHERE "WHERE";
%token ORDERBY "ORDER BY";
%token GROUPBY "GROUP BY";

%token SUM "SUM";
%token COUNT "COUNT";
%token AVG "AVG";
%token MAX "MAX";
%token MIN "MIN";

%token LEFTPAR "leftparen";
%token RIGHTPAR "rightparen";
%token SEMICOLON "semicolon";
%token COMMA "comma";

%type<std::shared_ptr<Query::ExprTreeNode>> expr;

%start query

%left SELECT FROM WHERE ORDERBY
%left OR;
%left AND;
%left NOT;
%left COMPARATOR1 COMPARATOR2
%left ADD SUB;
%left MUL DIV MOD;
%left UMINUS

%%

expr: INTEGER {
        $$ = std::shared_ptr<Query::ExprTreeNode>(
            new Query::ConstValueNode(Query::NodeValue::IntValue($1)));
        driver.query_->SetExprNode($$);
      }
    | DOUBLE {
        $$ = std::shared_ptr<Query::ExprTreeNode>(
            new Query::ConstValueNode(Query::NodeValue::DoubleValue($1)));
        driver.query_->SetExprNode($$);
      }
    | STRING {
        $$ = std::shared_ptr<Query::ExprTreeNode>(
            new Query::ConstValueNode(Query::NodeValue::StringValue($1)));
        driver.query_->SetExprNode($$);
      }
    | CHAR {
        $$ = std::shared_ptr<Query::ExprTreeNode>(
            new Query::ConstValueNode(Query::NodeValue::CharValue($1)));
        driver.query_->SetExprNode($$);
      }
    | BOOL {
        $$ = std::shared_ptr<Query::ExprTreeNode>(
            new Query::ConstValueNode(Query::NodeValue::BoolValue($1)));
        driver.query_->SetExprNode($$);
      }
    | IDENTIFIER {
        if (driver.debug()) {
          std::cout << "Column value: " << $1 << std::endl;
        }
        Query::Column column;
        if (!driver.query_->ParseTableColumn($1, &column)) {
          YYABORT;
        }
        // Use default table if table name not specified.
        if (column.table_name.empty()) {
          column.table_name = driver.query_->DefaultTable();
        }
        auto field_m = driver.query_->FindTableColumn(column);
        if (field_m == nullptr) {
          YYABORT;
        }
        column.index = field_m->index();
        column.type = field_m->type();
        $$.reset(new Query::ColumnNode(column));
        if (!$$->valid()) {
          driver.set_error_msg($$->error_msg());
          YYABORT;
        }
        driver.query_->SetExprNode($$);
      }
    ;

expr: expr ADD expr {
        if (driver.debug()) {
          std::cout << '+' << std::endl;
        }
        $$ = std::shared_ptr<Query::ExprTreeNode>(
                new Query::OperatorNode(Query::ADD, $1, $3));
        if (!$$->valid()) {
          driver.set_error_msg($$->error_msg());
          YYABORT;
        }
        driver.query_->SetExprNode($$);
      }
    | expr SUB expr {
        if (driver.debug()) {
          std::cout << '-' << std::endl;
        }
        $$ = std::shared_ptr<Query::ExprTreeNode>(
                new Query::OperatorNode(Query::SUB, $1, $3));
        if (!$$->valid()) {
          driver.set_error_msg($$->error_msg());
          YYABORT;
        }
        driver.query_->SetExprNode($$);
      }
    | expr MUL expr {
        if (driver.debug()) {
          std::cout << '*' << std::endl;
        }
        $$ = std::shared_ptr<Query::ExprTreeNode>(
                new Query::OperatorNode(Query::MUL, $1, $3));
        if (!$$->valid()) {
          driver.set_error_msg($$->error_msg());
          YYABORT;
        }
        driver.query_->SetExprNode($$);
      }
    | expr DIV expr {
        if (driver.debug()) {
          std::cout << '/' << std::endl;
        }
        $$ = std::shared_ptr<Query::ExprTreeNode>(
                new Query::OperatorNode(Query::DIV, $1, $3));
        if (!$$->valid()) {
          driver.set_error_msg($$->error_msg());
          YYABORT;
        }
        driver.query_->SetExprNode($$);
      }
    | expr MOD expr {
        if (driver.debug()) {
          std::cout << '%' << std::endl;
        }
        $$ = std::shared_ptr<Query::ExprTreeNode>(
                new Query::OperatorNode(Query::MOD, $1, $3));
        if (!$$->valid()) {
          driver.set_error_msg($$->error_msg());
          YYABORT;
        }
        driver.query_->SetExprNode($$);
      }
    | SUB expr %prec UMINUS {
        // '-' as negative sign.
        if (driver.debug()) {
          std::cout << "negative" << std::endl;
        }

        // Check node type. It can't be LOGICAL NODE.
        if ($2->type() != Query::ExprTreeNode::CONST_VALUE &&
            $2->type() != Query::ExprTreeNode::TABLE_COLUMN &&
            $2->type() != Query::ExprTreeNode::OPERATOR) {
          $2->set_valid(false);
          $2->set_error_msg(Strings::StrCat(
              "Parse error - Can't apply negative sign on node ",
              Query::ExprTreeNode::NodeTypeStr($2->type())));
        }
        // Check node value type. STRING type is not acceptable.
        if ($2->value().type == Query::STRING ||
            $2->value().type == Query::UNKNOWN_VALUE_TYPE) {
          $2->set_valid(false);
          $2->set_error_msg(Strings::StrCat(
              "Can use negative sign on type ",
              Query::ValueTypeStr($2->value().type).c_str()));
        }
        $2->set_negative(true);
        $$ = $2;
        if (!$$->valid()) {
          driver.set_error_msg($$->error_msg());
          YYABORT;
        }
        driver.query_->SetExprNode($$);
      }
    | expr COMPARATOR1 expr {
        if (driver.debug()) {
          std::cout << $2 << std::endl;
        }
        $$ = std::shared_ptr<Query::ExprTreeNode>(
                new Query::OperatorNode(Query::StrToOp($2), $1, $3));
        if (!$$->valid()) {
          driver.set_error_msg($$->error_msg());
          YYABORT;
        }
        driver.query_->SetExprNode($$);
      }
    | expr COMPARATOR2 expr {
        if (driver.debug()) {
          std::cout << $2 << std::endl;
        }
        $$ = std::shared_ptr<Query::ExprTreeNode>(
                new Query::OperatorNode(Query::StrToOp($2), $1, $3));
        if (!$$->valid()) {
          driver.set_error_msg($$->error_msg());
          YYABORT;
        }
        driver.query_->SetExprNode($$);
      }
    | expr AND expr {
        if (driver.debug()) {
          std::cout << "AND" << std::endl;
        }
        $$ = std::shared_ptr<Query::ExprTreeNode>(
                new Query::OperatorNode(Query::AND, $1, $3));
        if (!$$->valid()) {
          driver.set_error_msg($$->error_msg());
          YYABORT;
        }
        driver.query_->SetExprNode($$);
      }
    | expr OR expr {
        if (driver.debug()) {
          std::cout << "OR" << std::endl;
        }
        $$ = std::shared_ptr<Query::ExprTreeNode>(
                new Query::OperatorNode(Query::OR, $1, $3));
        if (!$$->valid()) {
          driver.set_error_msg($$->error_msg());
          YYABORT;
        }
        driver.query_->SetExprNode($$);
      }
    | NOT expr {
        if (driver.debug()) {
          std::cout << "NOT" << std::endl;
        }
        $$ = std::shared_ptr<Query::ExprTreeNode>(
                new Query::OperatorNode(Query::NOT, $2, nullptr));
        if (!$$->valid()) {
          driver.set_error_msg($$->error_msg());
          YYABORT;
        }
        driver.query_->SetExprNode($$);
      }
    | LEFTPAR expr RIGHTPAR {
      $$ = $2;
      driver.query_->SetExprNode($$);
    }
    ;

// Query
query: expr { /* For interpreter testing. DBMS should reject this "query". */ }
    | select_query { /* nill */ }

table_list: IDENTIFIER {
      if (!driver.query_->AddTable($1)) {
        YYABORT;
      }
    }
    | table_list COMMA IDENTIFIER  {
      if (!driver.query_->AddTable($3)) {
        YYABORT;
      }
    }

column_target: IDENTIFIER {
      if (!driver.query_->AddColumn($1)) {
        YYABORT;
      }
    }
    | SUM LEFTPAR IDENTIFIER RIGHTPAR {
      if (!driver.query_->AddColumn($3, Query::AggregationType::SUM)) {
        YYABORT;
      }
    }
    | COUNT LEFTPAR IDENTIFIER RIGHTPAR {
      if (!driver.query_->AddColumn($3, Query::AggregationType::COUNT)) {
        YYABORT;
      }
    }
    | AVG LEFTPAR IDENTIFIER RIGHTPAR {
      if (!driver.query_->AddColumn($3, Query::AggregationType::AVG)) {
        YYABORT;
      }
    }
    | MAX LEFTPAR IDENTIFIER RIGHTPAR {
      if (!driver.query_->AddColumn($3, Query::AggregationType::MAX)) {
        YYABORT;
      }
    }
    | MIN LEFTPAR IDENTIFIER RIGHTPAR {
      if (!driver.query_->AddColumn($3, Query::AggregationType::MIN)) {
        YYABORT;
      }
    }
    | MUL {
      // I know this is wired. Here the * is a wildcard, not multiply symbol.
      if (!driver.query_->AddColumn("*")) {
        YYABORT;
      }
    }
    | COUNT LEFTPAR MUL RIGHTPAR {
      if (!driver.query_->AddColumn("*", Query::AggregationType::COUNT)) {
        YYABORT;
      }
    }

column_list: column_target {
      
    }
    | column_list COMMA column_target {

    }

order_by_column_target : IDENTIFIER {
      if (!driver.query_->AddOrderByColumn($1)) {
        YYABORT;
      }
    }
    | SUM LEFTPAR IDENTIFIER RIGHTPAR {
      if (!driver.query_->AddOrderByColumn($3, Query::AggregationType::SUM)) {
        YYABORT;
      }
    }
    | COUNT LEFTPAR IDENTIFIER RIGHTPAR {
      if (!driver.query_->AddOrderByColumn($3, Query::AggregationType::COUNT)) {
        YYABORT;
      }
    }
    | AVG LEFTPAR IDENTIFIER RIGHTPAR {
      if (!driver.query_->AddOrderByColumn($3, Query::AggregationType::AVG)) {
        YYABORT;
      }
    }
    | MAX LEFTPAR IDENTIFIER RIGHTPAR {
      if (!driver.query_->AddOrderByColumn($3, Query::AggregationType::MAX)) {
        YYABORT;
      }
    }
    | MIN LEFTPAR IDENTIFIER RIGHTPAR {
      if (!driver.query_->AddOrderByColumn($3, Query::AggregationType::MIN)) {
        YYABORT;
      }
    }

order_by_column_list : order_by_column_target {
      
    }
    | order_by_column_target COMMA order_by_column_target {

    }

group_by_column_target : IDENTIFIER {
      if (!driver.query_->AddGroupByColumn($1)) {
        YYABORT;
      }
    }

group_by_column_list : group_by_column_target {
      
    }
    | group_by_column_target COMMA group_by_column_target {

    }

// **************************** SELECT query ******************************** //
select_query: select_stmt from_stmt opt_where_stmt opt_group_by_stmt opt_order_by_stmt {
      if (driver.debug()) {
        std::cout << "Query SELECT" << std::endl;
      }
    }

// SELECT statement.
select_stmt: SELECT column_list {
      if (driver.debug()) {
        // std::cout << "SELECT - " << Strings::Join(driver.columns(), ", ")
        //           << std::endl;
      }
    }

// FROM statement.
from_stmt: FROM table_list {
      if (driver.debug()) {
        // std::cout << "FROM - " << Strings::Join(driver.tables(), ", ")
        //           << std::endl;
      }
    }

// WHERE statement.
opt_where_stmt: { /* nill */ }
    | WHERE expr {
      if (driver.debug()) {
        std::cout << "WHERE stmt parsed" << std::endl;
      }
    }

// ORDER BY statement
opt_order_by_stmt: { /* nill */ }
    | ORDERBY order_by_column_list {
      // if (driver.debug()) {
      //   std::cout << "ORDER BY " << std::endl;
      // }
    }

// ORDER BY statement
opt_group_by_stmt: { /* nill */ }
    | GROUPBY group_by_column_list {
      // if (driver.debug()) {
      //   std::cout << "ORDER BY " << std::endl;
      // }
    }

%%

// Bison expects us to provide implementation - otherwise linker complains.
void Sql::Parser::error(const location &loc , const std::string &message) {
  // Location should be initialized inside scanner action, but is not in this
  // example. Let's grab location directly from driver class.
  //cout << "Error: " << message << endl << "Location: " << loc << endl;
  std::cout << "Error: " << message << endl << "Error location: "
            << driver.location() << endl;
}
