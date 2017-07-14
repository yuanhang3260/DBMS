%{
#include <iostream>
#include <cstdlib>

#include "Strings/Convert.h"
#include "Strings/Utils.h"
#include "Sql/scanner.h"
#include "Sql/interpreter.h"
#include "Sql/parser.hpp"
#include "Sql/location.hh"

// Original yyterminate() macro returns int. Since we're using Bison 3
// variants as tokens, we must redefine it to change type from `int` to
// `Parser::semantic_type`
#define yyterminate() Sql::Parser::make_END(Sql::location());

// This will track current scanner location.
// Action is called when length of the token is known.
//#define YY_USER_ACTION m_driver.increaseLocation(yyleng);

// Location API is used, but the location is not initialized, 'cause I'm lazy.
// When making a token with make_{something} method you can pass detailed
// token location. Current location is accessible with m_driver.location()
// method. All puzzle elements are there - just pass location value in every
// action code block below. I'm going to waste more time writing this excuse
// than putting this boilerplate below...
//
// Location class can be found in location.hh and posistion.hh files. It's just
// a bit too much boilerplate for this small example. Bummer.
%}

%option nodefault
%option noyywrap
%option c++
%option yyclass="Scanner"
%option prefix="Sql_"

/* Data types */
INTERGER    ([1-9][0-9]*)
DOUBLE      ([0-9]+(\.[0-9]+)?)
STRING      (\"[^"\n]*\")
CHAR        ('.')
BOOL        (TRUE|True|true|FALSE|False|false)

/* Value operators */
ADD \+
SUB \-
MUL \*
DIV \/
MOD %
COMPARATOR1 ([<>=])
COMPARATOR2 (>=|<=|!=)

/* Conditional logic */
AND         (AND|and)
OR          (OR|or)
NOT         (NOT|not)

/* Indentifier can be database name, table name or column name */
IDENTIFIER  ([a-z0-9_]+(\.[a-z0-9_]+)*)

/* Sql keywords */
SELECT      (SELECT|select)
FROM        (FROM|from)
WHERE       (WHERE|where)

%%

{INTERGER}  {
              //std::cout << "==> INTERGER " << yytext << std::endl;
              int64 number;
              if (Strings::ToInt64Safe(yytext, &number)) {
                return Sql::Parser::make_INTEGER(number, Sql::location());
              } else {
                return yyterminate();
              }
            }

{DOUBLE}    {
              //std::cout << "==> DOUBLE " << yytext << std::endl;
              double d;
              if (Strings::ToDoubleSafe(yytext, &d)) {
                return Sql::Parser::make_DOUBLE(d, Sql::location());
              } else {
                return yyterminate();
              }
            }

{STRING}    {
              //std::cout << "==> STRING " << yytext << std::endl;
              std::string str = Strings::Strip(std::string(yytext), "\"");
              return Sql::Parser::make_STRING(str, Sql::location()); 
            }

{CHAR}      {
              //std::cout << "==> CHAR " << yytext << std::endl;
              char c = yytext[1];
              return Sql::Parser::make_CHAR(c, Sql::location()); 
            }

{BOOL}      {
              //std::cout << "==> BOOL " << yytext << std::endl;
              std::string str_value(yytext);
              bool semantic_bool = (str_value == "TRUE" ||
                                    str_value == "True" ||
                                    str_value == "true") ? true : false;
              return Sql::Parser::make_BOOL(semantic_bool, Sql::location()); 
            }

{ADD}       {
              //std::cout << "==> ADD " << yytext << std::endl;
              return Sql::Parser::make_ADD(Sql::location());
            }

{SUB}       {
              //std::cout << "==> ADD " << yytext << std::endl;
              return Sql::Parser::make_SUB(Sql::location());
            }

{MUL}       {
              //std::cout << "==> ADD " << yytext << std::endl;
              return Sql::Parser::make_MUL(Sql::location());
            }

{DIV}       {
              //std::cout << "==> ADD " << yytext << std::endl;
              return Sql::Parser::make_DIV(Sql::location());
            }

{MOD}       {
              //std::cout << "==> ADD " << yytext << std::endl;
              return Sql::Parser::make_MOD(Sql::location());
            }

{COMPARATOR1}   {
                  //std::cout << "==> COMPARATOR " << yytext << std::endl;
                  return Sql::Parser::make_COMPARATOR1(std::string(yytext, 1), Sql::location());
                }

{COMPARATOR2}   {
                  //std::cout << "==> COMPARATOR " << yytext << std::endl;
                  return Sql::Parser::make_COMPARATOR2(std::string(yytext, 2), Sql::location());
                }

{AND}     {
            //std::cout << "==> AND" << std::endl;
            return Sql::Parser::make_AND(Sql::location());
          }

{OR}      {
            //std::cout << "==> OR" << std::endl;
            return Sql::Parser::make_OR(Sql::location());
          }

{NOT}     {
            //std::cout << "==> NOT" << std::endl;
            return Sql::Parser::make_NOT(Sql::location());
          }

{SELECT}  {
            //std::cout << "==> SELECT" << std::endl;
            return Sql::Parser::make_SELECT(Sql::location());
          }

{FROM}    {
            //std::cout << "==> FROM" << std::endl;
            return Sql::Parser::make_FROM(Sql::location());
          }

{WHERE}   {
            //std::cout << "==> WHERE" << std::endl;
            return Sql::Parser::make_WHERE(Sql::location()); }

{IDENTIFIER}  {
                //std::cout << "==> INDENTIFIER " << yytext << std::endl;
                return Sql::Parser::make_IDENTIFIER(yytext, Sql::location());
              }

\(        {
            //std::cout << "==> (" << std::endl;
            return Sql::Parser::make_LEFTPAR(Sql::location());
          }

\)        {
            //std::cout << "==> )" << std::endl;
            return Sql::Parser::make_RIGHTPAR(Sql::location());
          }

;         {
            //std::cout << "==> ;" << std::endl;
            return yyterminate();
          }

,         {
            //cout << "==> ," << endl;
            return Sql::Parser::make_COMMA(Sql::location());
          }

[\n\t ]   { /* cout << "==> whitespace (ignored)" << endl; */ }

.         {
            std::cout << "==> unknown char [" << yytext << "]" << std::endl;
          }

<<EOF>>   { return yyterminate(); }

%%
