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

using namespace Sql;
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

%token LEFTPAR "leftparen";
%token RIGHTPAR "rightparen";
%token SEMICOLON "semicolon";
%token COMMA "comma";

%type<std::shared_ptr<Query::ExprTreeNode>> expr;

%start expr

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
        driver.node_ = $$;
      }
    | DOUBLE {
        $$ = std::shared_ptr<Query::ExprTreeNode>(
            new Query::ConstValueNode(Query::NodeValue::DoubleValue($1)));
        driver.node_ = $$;
      }
    | STRING {
        $$ = std::shared_ptr<Query::ExprTreeNode>(
            new Query::ConstValueNode(Query::NodeValue::StringValue($1)));
        driver.node_ = $$;
      }
    | CHAR {
        $$ = std::shared_ptr<Query::ExprTreeNode>(
            new Query::ConstValueNode(Query::NodeValue::CharValue($1)));
        driver.node_ = $$;
      }
    | BOOL {
        $$ = std::shared_ptr<Query::ExprTreeNode>(
            new Query::ConstValueNode(Query::NodeValue::BoolValue($1)));
        driver.node_ = $$;
      }
    | IDENTIFIER {
        $$ = std::shared_ptr<Query::ExprTreeNode>(new Query::ColumnNode($1));
        driver.node_ = $$;
      }
    ;

expr: expr ADD expr {
        if (driver.debug()) {
          std::cout << '+' << std::endl;
        }
        $$ = std::shared_ptr<Query::ExprTreeNode>(
                new Query::OperatorNode(Query::ADD, $1, $3));
        // TODO: Check the OperatorNode is valid (Init() returns success);
        driver.node_ = $$;
      }
    | expr SUB expr {
        if (driver.debug()) {
          std::cout << '-' << std::endl;
        }
        $$ = std::shared_ptr<Query::ExprTreeNode>(
                new Query::OperatorNode(Query::SUB, $1, $3));
        driver.node_ = $$;
      }
    | expr MUL expr {
        if (driver.debug()) {
          std::cout << '*' << std::endl;
        }
        $$ = std::shared_ptr<Query::ExprTreeNode>(
                new Query::OperatorNode(Query::MUL, $1, $3));
        driver.node_ = $$;
      }
    | expr DIV expr {
        if (driver.debug()) {
          std::cout << '/' << std::endl;
        }
        $$ = std::shared_ptr<Query::ExprTreeNode>(
                new Query::OperatorNode(Query::DIV, $1, $3));
        driver.node_ = $$;
      }
    | expr MOD expr {
        if (driver.debug()) {
          std::cout << '%' << std::endl;
        }
        $$ = std::shared_ptr<Query::ExprTreeNode>(
                new Query::OperatorNode(Query::MOD, $1, $3));
        driver.node_ = $$;
      }
    | SUB expr %prec UMINUS {
        // '-' as negative sign.
        //
        // TODO: It should only be applied to ConstValueNode and ColumnNode,
        // and not meaningful STRING type. Add a check here and return syntax
        // error.
        if (driver.debug()) {
          std::cout << "negative" << std::endl;
        }
        $2->set_negative(true);
        $$ = $2;
        driver.node_ = $$;
      }
    | expr COMPARATOR1 expr {
        if (driver.debug()) {
          std::cout << $2 << std::endl;
        }
        $$ = std::shared_ptr<Query::ExprTreeNode>(
                new Query::OperatorNode(Query::StrToOp($2), $1, $3));
        driver.node_ = $$;
      }
    | expr COMPARATOR2 expr {
        if (driver.debug()) {
          std::cout << $2 << std::endl;
        }
        $$ = std::shared_ptr<Query::ExprTreeNode>(
                new Query::OperatorNode(Query::StrToOp($2), $1, $3));
        driver.node_ = $$;
      }
    | expr AND expr {

      }
    | expr OR expr {

      }
    | NOT expr {

      }
    | LEFTPAR expr RIGHTPAR {
      $$ = $2;
      driver.node_ = $$;
    }
    ;

%%

// Bison expects us to provide implementation - otherwise linker complains
void Sql::Parser::error(const location &loc , const std::string &message) {
  // Location should be initialized inside scanner action, but is not in this example.
  // Let's grab location directly from driver class.
  // cout << "Error: " << message << endl << "Location: " << loc << endl;
  std::cout << "Error: " << message << endl << "Error location: "
            << driver.location() << endl;
}
