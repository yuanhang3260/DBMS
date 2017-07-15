#ifndef SQL_SCANNER_H_
#define SQL_SCANNER_H_

// Generated Flex class name is yyFlexLexer by default. If we want to use more
// flex-classes we should name them differently. See scanner.l prefix option.
// 
// Unfortunately the implementation relies on this trick with redefining class
// name with a preprocessor macro.
// See GNU Flex manual, "Generating C++ Scanners" section
#if ! defined(yyFlexLexerOnce)
#undef yyFlexLexer
#define yyFlexLexer Sql_FlexLexer // the trick with prefix; no namespace here :(
#include <FlexLexer.h>
#endif

// Scanner method signature is defined by this macro. Original yylex() returns
// int. Sinice Bison 3 uses symbol_type, we must change returned type. We also
// rename it to something sane, since you cannot overload return type.
#undef YY_DECL
#define YY_DECL Sql::Parser::symbol_type Sql::Scanner::get_next_token()

#include "parser.hh" // this is needed for symbol_type

namespace Query {
class Interpreter; 
}

namespace Sql {
    
class Scanner : public yyFlexLexer {
public:
  Scanner(Query::Interpreter& driver) : m_driver(driver) {}
	virtual ~Scanner() {}
	virtual Sql::Parser::symbol_type get_next_token();
        
private:
  Query::Interpreter& m_driver;
};

}  // namespace Sql

#endif  // SQL_SCANNER_H_
