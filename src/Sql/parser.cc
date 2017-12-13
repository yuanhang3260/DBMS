// A Bison parser, made by GNU Bison 3.0.4.

// Skeleton implementation for Bison LALR(1) parsers in C++

// Copyright (C) 2002-2015 Free Software Foundation, Inc.

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

// As a special exception, you may create a larger work that contains
// part or all of the Bison parser skeleton and distribute that work
// under terms of your choice, so long as that work isn't itself a
// parser generator using the skeleton or a modified version thereof
// as a parser skeleton.  Alternatively, if you modify or redistribute
// the parser skeleton itself, you may (at your option) remove this
// special exception, which will cause the skeleton and the resulting
// Bison output files to be licensed under the GNU General Public
// License without this special exception.

// This special exception was added by the Free Software Foundation in
// version 2.2 of Bison.
// //                    "%code top" blocks.
#line 45 "src/Sql/parser.y" // lalr1.cc:397

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

#line 73 "src/Sql/parser.cc" // lalr1.cc:397


// First part of user declarations.

#line 78 "src/Sql/parser.cc" // lalr1.cc:404

# ifndef YY_NULLPTR
#  if defined __cplusplus && 201103L <= __cplusplus
#   define YY_NULLPTR nullptr
#  else
#   define YY_NULLPTR 0
#  endif
# endif

#include "parser.hh"

// User implementation prologue.

#line 92 "src/Sql/parser.cc" // lalr1.cc:412


#ifndef YY_
# if defined YYENABLE_NLS && YYENABLE_NLS
#  if ENABLE_NLS
#   include <libintl.h> // FIXME: INFRINGES ON USER NAME SPACE.
#   define YY_(msgid) dgettext ("bison-runtime", msgid)
#  endif
# endif
# ifndef YY_
#  define YY_(msgid) msgid
# endif
#endif

#define YYRHSLOC(Rhs, K) ((Rhs)[K].location)
/* YYLLOC_DEFAULT -- Set CURRENT to span from RHS[1] to RHS[N].
   If N is 0, then set CURRENT to the empty location which ends
   the previous symbol: RHS[0] (always defined).  */

# ifndef YYLLOC_DEFAULT
#  define YYLLOC_DEFAULT(Current, Rhs, N)                               \
    do                                                                  \
      if (N)                                                            \
        {                                                               \
          (Current).begin  = YYRHSLOC (Rhs, 1).begin;                   \
          (Current).end    = YYRHSLOC (Rhs, N).end;                     \
        }                                                               \
      else                                                              \
        {                                                               \
          (Current).begin = (Current).end = YYRHSLOC (Rhs, 0).end;      \
        }                                                               \
    while (/*CONSTCOND*/ false)
# endif


// Suppress unused-variable warnings by "using" E.
#define YYUSE(E) ((void) (E))

// Enable debugging if requested.
#if YYDEBUG

// A pseudo ostream that takes yydebug_ into account.
# define YYCDEBUG if (yydebug_) (*yycdebug_)

# define YY_SYMBOL_PRINT(Title, Symbol)         \
  do {                                          \
    if (yydebug_)                               \
    {                                           \
      *yycdebug_ << Title << ' ';               \
      yy_print_ (*yycdebug_, Symbol);           \
      *yycdebug_ << std::endl;                  \
    }                                           \
  } while (false)

# define YY_REDUCE_PRINT(Rule)          \
  do {                                  \
    if (yydebug_)                       \
      yy_reduce_print_ (Rule);          \
  } while (false)

# define YY_STACK_PRINT()               \
  do {                                  \
    if (yydebug_)                       \
      yystack_print_ ();                \
  } while (false)

#else // !YYDEBUG

# define YYCDEBUG if (false) std::cerr
# define YY_SYMBOL_PRINT(Title, Symbol)  YYUSE(Symbol)
# define YY_REDUCE_PRINT(Rule)           static_cast<void>(0)
# define YY_STACK_PRINT()                static_cast<void>(0)

#endif // !YYDEBUG

#define yyerrok         (yyerrstatus_ = 0)
#define yyclearin       (yyla.clear ())

#define YYACCEPT        goto yyacceptlab
#define YYABORT         goto yyabortlab
#define YYERROR         goto yyerrorlab
#define YYRECOVERING()  (!!yyerrstatus_)

#line 11 "src/Sql/parser.y" // lalr1.cc:479
namespace  Sql  {
#line 178 "src/Sql/parser.cc" // lalr1.cc:479

  /* Return YYSTR after stripping away unnecessary quotes and
     backslashes, so that it's suitable for yyerror.  The heuristic is
     that double-quoting is unnecessary unless the string contains an
     apostrophe, a comma, or backslash (other than backslash-backslash).
     YYSTR is taken from yytname.  */
  std::string
   Parser ::yytnamerr_ (const char *yystr)
  {
    if (*yystr == '"')
      {
        std::string yyr = "";
        char const *yyp = yystr;

        for (;;)
          switch (*++yyp)
            {
            case '\'':
            case ',':
              goto do_not_strip_quotes;

            case '\\':
              if (*++yyp != '\\')
                goto do_not_strip_quotes;
              // Fall through.
            default:
              yyr += *yyp;
              break;

            case '"':
              return yyr;
            }
      do_not_strip_quotes: ;
      }

    return yystr;
  }


  /// Build a parser object.
   Parser :: Parser  (Sql::Scanner &scanner_yyarg, Query::Interpreter &driver_yyarg)
    :
#if YYDEBUG
      yydebug_ (false),
      yycdebug_ (&std::cerr),
#endif
      scanner (scanner_yyarg),
      driver (driver_yyarg)
  {}

   Parser ::~ Parser  ()
  {}


  /*---------------.
  | Symbol types.  |
  `---------------*/



  // by_state.
  inline
   Parser ::by_state::by_state ()
    : state (empty_state)
  {}

  inline
   Parser ::by_state::by_state (const by_state& other)
    : state (other.state)
  {}

  inline
  void
   Parser ::by_state::clear ()
  {
    state = empty_state;
  }

  inline
  void
   Parser ::by_state::move (by_state& that)
  {
    state = that.state;
    that.clear ();
  }

  inline
   Parser ::by_state::by_state (state_type s)
    : state (s)
  {}

  inline
   Parser ::symbol_number_type
   Parser ::by_state::type_get () const
  {
    if (state == empty_state)
      return empty_symbol;
    else
      return yystos_[state];
  }

  inline
   Parser ::stack_symbol_type::stack_symbol_type ()
  {}


  inline
   Parser ::stack_symbol_type::stack_symbol_type (state_type s, symbol_type& that)
    : super_type (s, that.location)
  {
      switch (that.type_get ())
    {
      case 7: // "Boolean"
        value.move< bool > (that.value);
        break;

      case 6: // "Char"
        value.move< char > (that.value);
        break;

      case 4: // "Double"
        value.move< double > (that.value);
        break;

      case 3: // "Integer"
        value.move< int64 > (that.value);
        break;

      case 35: // expr
        value.move< std::shared_ptr<Query::ExprTreeNode> > (that.value);
        break;

      case 5: // "String"
      case 13: // "Comparator1"
      case 14: // "Comparator2"
      case 18: // "Identifier"
        value.move< std::string > (that.value);
        break;

      default:
        break;
    }

    // that is emptied.
    that.type = empty_symbol;
  }

  inline
   Parser ::stack_symbol_type&
   Parser ::stack_symbol_type::operator= (const stack_symbol_type& that)
  {
    state = that.state;
      switch (that.type_get ())
    {
      case 7: // "Boolean"
        value.copy< bool > (that.value);
        break;

      case 6: // "Char"
        value.copy< char > (that.value);
        break;

      case 4: // "Double"
        value.copy< double > (that.value);
        break;

      case 3: // "Integer"
        value.copy< int64 > (that.value);
        break;

      case 35: // expr
        value.copy< std::shared_ptr<Query::ExprTreeNode> > (that.value);
        break;

      case 5: // "String"
      case 13: // "Comparator1"
      case 14: // "Comparator2"
      case 18: // "Identifier"
        value.copy< std::string > (that.value);
        break;

      default:
        break;
    }

    location = that.location;
    return *this;
  }


  template <typename Base>
  inline
  void
   Parser ::yy_destroy_ (const char* yymsg, basic_symbol<Base>& yysym) const
  {
    if (yymsg)
      YY_SYMBOL_PRINT (yymsg, yysym);
  }

#if YYDEBUG
  template <typename Base>
  void
   Parser ::yy_print_ (std::ostream& yyo,
                                     const basic_symbol<Base>& yysym) const
  {
    std::ostream& yyoutput = yyo;
    YYUSE (yyoutput);
    symbol_number_type yytype = yysym.type_get ();
    // Avoid a (spurious) G++ 4.8 warning about "array subscript is
    // below array bounds".
    if (yysym.empty ())
      std::abort ();
    yyo << (yytype < yyntokens_ ? "token" : "nterm")
        << ' ' << yytname_[yytype] << " ("
        << yysym.location << ": ";
    YYUSE (yytype);
    yyo << ')';
  }
#endif

  inline
  void
   Parser ::yypush_ (const char* m, state_type s, symbol_type& sym)
  {
    stack_symbol_type t (s, sym);
    yypush_ (m, t);
  }

  inline
  void
   Parser ::yypush_ (const char* m, stack_symbol_type& s)
  {
    if (m)
      YY_SYMBOL_PRINT (m, s);
    yystack_.push (s);
  }

  inline
  void
   Parser ::yypop_ (unsigned int n)
  {
    yystack_.pop (n);
  }

#if YYDEBUG
  std::ostream&
   Parser ::debug_stream () const
  {
    return *yycdebug_;
  }

  void
   Parser ::set_debug_stream (std::ostream& o)
  {
    yycdebug_ = &o;
  }


   Parser ::debug_level_type
   Parser ::debug_level () const
  {
    return yydebug_;
  }

  void
   Parser ::set_debug_level (debug_level_type l)
  {
    yydebug_ = l;
  }
#endif // YYDEBUG

  inline  Parser ::state_type
   Parser ::yy_lr_goto_state_ (state_type yystate, int yysym)
  {
    int yyr = yypgoto_[yysym - yyntokens_] + yystate;
    if (0 <= yyr && yyr <= yylast_ && yycheck_[yyr] == yystate)
      return yytable_[yyr];
    else
      return yydefgoto_[yysym - yyntokens_];
  }

  inline bool
   Parser ::yy_pact_value_is_default_ (int yyvalue)
  {
    return yyvalue == yypact_ninf_;
  }

  inline bool
   Parser ::yy_table_value_is_error_ (int yyvalue)
  {
    return yyvalue == yytable_ninf_;
  }

  int
   Parser ::parse ()
  {
    // State.
    int yyn;
    /// Length of the RHS of the rule being reduced.
    int yylen = 0;

    // Error handling.
    int yynerrs_ = 0;
    int yyerrstatus_ = 0;

    /// The lookahead symbol.
    symbol_type yyla;

    /// The locations where the error started and ended.
    stack_symbol_type yyerror_range[3];

    /// The return value of parse ().
    int yyresult;

    // FIXME: This shoud be completely indented.  It is not yet to
    // avoid gratuitous conflicts when merging into the master branch.
    try
      {
    YYCDEBUG << "Starting parse" << std::endl;


    /* Initialize the stack.  The initial state will be set in
       yynewstate, since the latter expects the semantical and the
       location values to have been already stored, initialize these
       stacks with a primary value.  */
    yystack_.clear ();
    yypush_ (YY_NULLPTR, 0, yyla);

    // A new symbol was pushed on the stack.
  yynewstate:
    YYCDEBUG << "Entering state " << yystack_[0].state << std::endl;

    // Accept?
    if (yystack_[0].state == yyfinal_)
      goto yyacceptlab;

    goto yybackup;

    // Backup.
  yybackup:

    // Try to take a decision without lookahead.
    yyn = yypact_[yystack_[0].state];
    if (yy_pact_value_is_default_ (yyn))
      goto yydefault;

    // Read a lookahead token.
    if (yyla.empty ())
      {
        YYCDEBUG << "Reading a token: ";
        try
          {
            symbol_type yylookahead (yylex (scanner, driver));
            yyla.move (yylookahead);
          }
        catch (const syntax_error& yyexc)
          {
            error (yyexc);
            goto yyerrlab1;
          }
      }
    YY_SYMBOL_PRINT ("Next token is", yyla);

    /* If the proper action on seeing token YYLA.TYPE is to reduce or
       to detect an error, take that action.  */
    yyn += yyla.type_get ();
    if (yyn < 0 || yylast_ < yyn || yycheck_[yyn] != yyla.type_get ())
      goto yydefault;

    // Reduce or error.
    yyn = yytable_[yyn];
    if (yyn <= 0)
      {
        if (yy_table_value_is_error_ (yyn))
          goto yyerrlab;
        yyn = -yyn;
        goto yyreduce;
      }

    // Count tokens shifted since error; after three, turn off error status.
    if (yyerrstatus_)
      --yyerrstatus_;

    // Shift the lookahead token.
    yypush_ ("Shifting", yyn, yyla);
    goto yynewstate;

  /*-----------------------------------------------------------.
  | yydefault -- do the default action for the current state.  |
  `-----------------------------------------------------------*/
  yydefault:
    yyn = yydefact_[yystack_[0].state];
    if (yyn == 0)
      goto yyerrlab;
    goto yyreduce;

  /*-----------------------------.
  | yyreduce -- Do a reduction.  |
  `-----------------------------*/
  yyreduce:
    yylen = yyr2_[yyn];
    {
      stack_symbol_type yylhs;
      yylhs.state = yy_lr_goto_state_(yystack_[yylen].state, yyr1_[yyn]);
      /* Variants are always initialized to an empty instance of the
         correct type. The default '$$ = $1' action is NOT applied
         when using variants.  */
        switch (yyr1_[yyn])
    {
      case 7: // "Boolean"
        yylhs.value.build< bool > ();
        break;

      case 6: // "Char"
        yylhs.value.build< char > ();
        break;

      case 4: // "Double"
        yylhs.value.build< double > ();
        break;

      case 3: // "Integer"
        yylhs.value.build< int64 > ();
        break;

      case 35: // expr
        yylhs.value.build< std::shared_ptr<Query::ExprTreeNode> > ();
        break;

      case 5: // "String"
      case 13: // "Comparator1"
      case 14: // "Comparator2"
      case 18: // "Identifier"
        yylhs.value.build< std::string > ();
        break;

      default:
        break;
    }


      // Compute the default @$.
      {
        slice<stack_symbol_type, stack_type> slice (yystack_, yylen);
        YYLLOC_DEFAULT (yylhs.location, slice, yylen);
      }

      // Perform the reduction.
      YY_REDUCE_PRINT (yyn);
      try
        {
          switch (yyn)
            {
  case 2:
#line 148 "src/Sql/parser.y" // lalr1.cc:859
    {
        yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > () = std::shared_ptr<Query::ExprTreeNode>(
            new Query::ConstValueNode(Query::NodeValue::IntValue(yystack_[0].value.as< int64 > ())));
        driver.query_->SetExprNode(yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > ());
      }
#line 639 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 3:
#line 153 "src/Sql/parser.y" // lalr1.cc:859
    {
        yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > () = std::shared_ptr<Query::ExprTreeNode>(
            new Query::ConstValueNode(Query::NodeValue::DoubleValue(yystack_[0].value.as< double > ())));
        driver.query_->SetExprNode(yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > ());
      }
#line 649 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 4:
#line 158 "src/Sql/parser.y" // lalr1.cc:859
    {
        yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > () = std::shared_ptr<Query::ExprTreeNode>(
            new Query::ConstValueNode(Query::NodeValue::StringValue(yystack_[0].value.as< std::string > ())));
        driver.query_->SetExprNode(yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > ());
      }
#line 659 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 5:
#line 163 "src/Sql/parser.y" // lalr1.cc:859
    {
        yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > () = std::shared_ptr<Query::ExprTreeNode>(
            new Query::ConstValueNode(Query::NodeValue::CharValue(yystack_[0].value.as< char > ())));
        driver.query_->SetExprNode(yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > ());
      }
#line 669 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 6:
#line 168 "src/Sql/parser.y" // lalr1.cc:859
    {
        yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > () = std::shared_ptr<Query::ExprTreeNode>(
            new Query::ConstValueNode(Query::NodeValue::BoolValue(yystack_[0].value.as< bool > ())));
        driver.query_->SetExprNode(yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > ());
      }
#line 679 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 7:
#line 173 "src/Sql/parser.y" // lalr1.cc:859
    {
        if (driver.debug()) {
          std::cout << "Column value: " << yystack_[0].value.as< std::string > () << std::endl;
        }
        Query::Column column;
        if (!driver.query_->ParseTableColumn(yystack_[0].value.as< std::string > (), &column)) {
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
        yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > ().reset(new Query::ColumnNode(column));
        if (!yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > ()->valid()) {
          driver.set_error_msg(yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > ()->error_msg());
          YYABORT;
        }
        driver.query_->SetExprNode(yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > ());
      }
#line 709 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 8:
#line 200 "src/Sql/parser.y" // lalr1.cc:859
    {
        if (driver.debug()) {
          std::cout << '+' << std::endl;
        }
        yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > () = std::shared_ptr<Query::ExprTreeNode>(
                new Query::OperatorNode(Query::ADD, yystack_[2].value.as< std::shared_ptr<Query::ExprTreeNode> > (), yystack_[0].value.as< std::shared_ptr<Query::ExprTreeNode> > ()));
        if (!yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > ()->valid()) {
          driver.set_error_msg(yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > ()->error_msg());
          YYABORT;
        }
        driver.query_->SetExprNode(yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > ());
      }
#line 726 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 9:
#line 212 "src/Sql/parser.y" // lalr1.cc:859
    {
        if (driver.debug()) {
          std::cout << '-' << std::endl;
        }
        yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > () = std::shared_ptr<Query::ExprTreeNode>(
                new Query::OperatorNode(Query::SUB, yystack_[2].value.as< std::shared_ptr<Query::ExprTreeNode> > (), yystack_[0].value.as< std::shared_ptr<Query::ExprTreeNode> > ()));
        if (!yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > ()->valid()) {
          driver.set_error_msg(yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > ()->error_msg());
          YYABORT;
        }
        driver.query_->SetExprNode(yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > ());
      }
#line 743 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 10:
#line 224 "src/Sql/parser.y" // lalr1.cc:859
    {
        if (driver.debug()) {
          std::cout << '*' << std::endl;
        }
        yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > () = std::shared_ptr<Query::ExprTreeNode>(
                new Query::OperatorNode(Query::MUL, yystack_[2].value.as< std::shared_ptr<Query::ExprTreeNode> > (), yystack_[0].value.as< std::shared_ptr<Query::ExprTreeNode> > ()));
        if (!yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > ()->valid()) {
          driver.set_error_msg(yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > ()->error_msg());
          YYABORT;
        }
        driver.query_->SetExprNode(yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > ());
      }
#line 760 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 11:
#line 236 "src/Sql/parser.y" // lalr1.cc:859
    {
        if (driver.debug()) {
          std::cout << '/' << std::endl;
        }
        yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > () = std::shared_ptr<Query::ExprTreeNode>(
                new Query::OperatorNode(Query::DIV, yystack_[2].value.as< std::shared_ptr<Query::ExprTreeNode> > (), yystack_[0].value.as< std::shared_ptr<Query::ExprTreeNode> > ()));
        if (!yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > ()->valid()) {
          driver.set_error_msg(yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > ()->error_msg());
          YYABORT;
        }
        driver.query_->SetExprNode(yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > ());
      }
#line 777 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 12:
#line 248 "src/Sql/parser.y" // lalr1.cc:859
    {
        if (driver.debug()) {
          std::cout << '%' << std::endl;
        }
        yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > () = std::shared_ptr<Query::ExprTreeNode>(
                new Query::OperatorNode(Query::MOD, yystack_[2].value.as< std::shared_ptr<Query::ExprTreeNode> > (), yystack_[0].value.as< std::shared_ptr<Query::ExprTreeNode> > ()));
        if (!yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > ()->valid()) {
          driver.set_error_msg(yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > ()->error_msg());
          YYABORT;
        }
        driver.query_->SetExprNode(yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > ());
      }
#line 794 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 13:
#line 260 "src/Sql/parser.y" // lalr1.cc:859
    {
        // '-' as negative sign.
        if (driver.debug()) {
          std::cout << "negative" << std::endl;
        }

        // Check node type. It can't be LOGICAL NODE.
        if (yystack_[0].value.as< std::shared_ptr<Query::ExprTreeNode> > ()->type() != Query::ExprTreeNode::CONST_VALUE &&
            yystack_[0].value.as< std::shared_ptr<Query::ExprTreeNode> > ()->type() != Query::ExprTreeNode::TABLE_COLUMN &&
            yystack_[0].value.as< std::shared_ptr<Query::ExprTreeNode> > ()->type() != Query::ExprTreeNode::OPERATOR) {
          yystack_[0].value.as< std::shared_ptr<Query::ExprTreeNode> > ()->set_valid(false);
          yystack_[0].value.as< std::shared_ptr<Query::ExprTreeNode> > ()->set_error_msg(Strings::StrCat(
              "Parse error - Can't apply negative sign on node ",
              Query::ExprTreeNode::NodeTypeStr(yystack_[0].value.as< std::shared_ptr<Query::ExprTreeNode> > ()->type())));
        }
        // Check node value type. STRING type is not acceptable.
        if (yystack_[0].value.as< std::shared_ptr<Query::ExprTreeNode> > ()->value().type == Query::STRING ||
            yystack_[0].value.as< std::shared_ptr<Query::ExprTreeNode> > ()->value().type == Query::UNKNOWN_VALUE_TYPE) {
          yystack_[0].value.as< std::shared_ptr<Query::ExprTreeNode> > ()->set_valid(false);
          yystack_[0].value.as< std::shared_ptr<Query::ExprTreeNode> > ()->set_error_msg(Strings::StrCat(
              "Can use negative sign on type ",
              Query::ValueTypeStr(yystack_[0].value.as< std::shared_ptr<Query::ExprTreeNode> > ()->value().type).c_str()));
        }
        yystack_[0].value.as< std::shared_ptr<Query::ExprTreeNode> > ()->set_negative(true);
        yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > () = yystack_[0].value.as< std::shared_ptr<Query::ExprTreeNode> > ();
        if (!yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > ()->valid()) {
          driver.set_error_msg(yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > ()->error_msg());
          YYABORT;
        }
        driver.query_->SetExprNode(yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > ());
      }
#line 830 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 14:
#line 291 "src/Sql/parser.y" // lalr1.cc:859
    {
        if (driver.debug()) {
          std::cout << yystack_[1].value.as< std::string > () << std::endl;
        }
        yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > () = std::shared_ptr<Query::ExprTreeNode>(
                new Query::OperatorNode(Query::StrToOp(yystack_[1].value.as< std::string > ()), yystack_[2].value.as< std::shared_ptr<Query::ExprTreeNode> > (), yystack_[0].value.as< std::shared_ptr<Query::ExprTreeNode> > ()));
        if (!yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > ()->valid()) {
          driver.set_error_msg(yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > ()->error_msg());
          YYABORT;
        }
        driver.query_->SetExprNode(yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > ());
      }
#line 847 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 15:
#line 303 "src/Sql/parser.y" // lalr1.cc:859
    {
        if (driver.debug()) {
          std::cout << yystack_[1].value.as< std::string > () << std::endl;
        }
        yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > () = std::shared_ptr<Query::ExprTreeNode>(
                new Query::OperatorNode(Query::StrToOp(yystack_[1].value.as< std::string > ()), yystack_[2].value.as< std::shared_ptr<Query::ExprTreeNode> > (), yystack_[0].value.as< std::shared_ptr<Query::ExprTreeNode> > ()));
        if (!yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > ()->valid()) {
          driver.set_error_msg(yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > ()->error_msg());
          YYABORT;
        }
        driver.query_->SetExprNode(yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > ());
      }
#line 864 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 16:
#line 315 "src/Sql/parser.y" // lalr1.cc:859
    {
        if (driver.debug()) {
          std::cout << "AND" << std::endl;
        }
        yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > () = std::shared_ptr<Query::ExprTreeNode>(
                new Query::OperatorNode(Query::AND, yystack_[2].value.as< std::shared_ptr<Query::ExprTreeNode> > (), yystack_[0].value.as< std::shared_ptr<Query::ExprTreeNode> > ()));
        if (!yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > ()->valid()) {
          driver.set_error_msg(yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > ()->error_msg());
          YYABORT;
        }
        driver.query_->SetExprNode(yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > ());
      }
#line 881 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 17:
#line 327 "src/Sql/parser.y" // lalr1.cc:859
    {
        if (driver.debug()) {
          std::cout << "OR" << std::endl;
        }
        yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > () = std::shared_ptr<Query::ExprTreeNode>(
                new Query::OperatorNode(Query::OR, yystack_[2].value.as< std::shared_ptr<Query::ExprTreeNode> > (), yystack_[0].value.as< std::shared_ptr<Query::ExprTreeNode> > ()));
        if (!yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > ()->valid()) {
          driver.set_error_msg(yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > ()->error_msg());
          YYABORT;
        }
        driver.query_->SetExprNode(yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > ());
      }
#line 898 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 18:
#line 339 "src/Sql/parser.y" // lalr1.cc:859
    {
        if (driver.debug()) {
          std::cout << "NOT" << std::endl;
        }
        yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > () = std::shared_ptr<Query::ExprTreeNode>(
                new Query::OperatorNode(Query::NOT, yystack_[0].value.as< std::shared_ptr<Query::ExprTreeNode> > (), nullptr));
        if (!yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > ()->valid()) {
          driver.set_error_msg(yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > ()->error_msg());
          YYABORT;
        }
        driver.query_->SetExprNode(yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > ());
      }
#line 915 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 19:
#line 351 "src/Sql/parser.y" // lalr1.cc:859
    {
      yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > () = yystack_[1].value.as< std::shared_ptr<Query::ExprTreeNode> > ();
      driver.query_->SetExprNode(yylhs.value.as< std::shared_ptr<Query::ExprTreeNode> > ());
    }
#line 924 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 20:
#line 358 "src/Sql/parser.y" // lalr1.cc:859
    { /* For interpreter testing. DBMS should reject this "query". */ }
#line 930 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 21:
#line 359 "src/Sql/parser.y" // lalr1.cc:859
    { /* nill */ }
#line 936 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 22:
#line 361 "src/Sql/parser.y" // lalr1.cc:859
    {
      if (!driver.query_->AddTable(yystack_[0].value.as< std::string > ())) {
        YYABORT;
      }
    }
#line 946 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 23:
#line 366 "src/Sql/parser.y" // lalr1.cc:859
    {
      if (!driver.query_->AddTable(yystack_[0].value.as< std::string > ())) {
        YYABORT;
      }
    }
#line 956 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 24:
#line 372 "src/Sql/parser.y" // lalr1.cc:859
    {
      if (!driver.query_->AddColumn(yystack_[0].value.as< std::string > ())) {
        YYABORT;
      }
    }
#line 966 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 25:
#line 377 "src/Sql/parser.y" // lalr1.cc:859
    {
      if (!driver.query_->AddColumn(yystack_[1].value.as< std::string > (), Query::AggregationType::SUM)) {
        YYABORT;
      }
    }
#line 976 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 26:
#line 382 "src/Sql/parser.y" // lalr1.cc:859
    {
      if (!driver.query_->AddColumn(yystack_[1].value.as< std::string > (), Query::AggregationType::COUNT)) {
        YYABORT;
      }
    }
#line 986 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 27:
#line 387 "src/Sql/parser.y" // lalr1.cc:859
    {
      if (!driver.query_->AddColumn(yystack_[1].value.as< std::string > (), Query::AggregationType::AVG)) {
        YYABORT;
      }
    }
#line 996 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 28:
#line 392 "src/Sql/parser.y" // lalr1.cc:859
    {
      if (!driver.query_->AddColumn(yystack_[1].value.as< std::string > (), Query::AggregationType::MAX)) {
        YYABORT;
      }
    }
#line 1006 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 29:
#line 397 "src/Sql/parser.y" // lalr1.cc:859
    {
      if (!driver.query_->AddColumn(yystack_[1].value.as< std::string > (), Query::AggregationType::MIN)) {
        YYABORT;
      }
    }
#line 1016 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 30:
#line 402 "src/Sql/parser.y" // lalr1.cc:859
    {
      // I know this is wired. Here the * is a wildcard, not multiply symbol.
      if (!driver.query_->AddColumn("*")) {
        YYABORT;
      }
    }
#line 1027 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 31:
#line 408 "src/Sql/parser.y" // lalr1.cc:859
    {
      if (!driver.query_->AddColumn("*", Query::AggregationType::COUNT)) {
        YYABORT;
      }
    }
#line 1037 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 32:
#line 414 "src/Sql/parser.y" // lalr1.cc:859
    {
      
    }
#line 1045 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 33:
#line 417 "src/Sql/parser.y" // lalr1.cc:859
    {

    }
#line 1053 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 34:
#line 421 "src/Sql/parser.y" // lalr1.cc:859
    {
      if (!driver.query_->AddOrderByColumn(yystack_[0].value.as< std::string > ())) {
        YYABORT;
      }
    }
#line 1063 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 35:
#line 426 "src/Sql/parser.y" // lalr1.cc:859
    {
      if (!driver.query_->AddOrderByColumn(yystack_[1].value.as< std::string > (), Query::AggregationType::SUM)) {
        YYABORT;
      }
    }
#line 1073 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 36:
#line 431 "src/Sql/parser.y" // lalr1.cc:859
    {
      if (!driver.query_->AddOrderByColumn(yystack_[1].value.as< std::string > (), Query::AggregationType::COUNT)) {
        YYABORT;
      }
    }
#line 1083 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 37:
#line 436 "src/Sql/parser.y" // lalr1.cc:859
    {
      if (!driver.query_->AddOrderByColumn(yystack_[1].value.as< std::string > (), Query::AggregationType::AVG)) {
        YYABORT;
      }
    }
#line 1093 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 38:
#line 441 "src/Sql/parser.y" // lalr1.cc:859
    {
      if (!driver.query_->AddOrderByColumn(yystack_[1].value.as< std::string > (), Query::AggregationType::MAX)) {
        YYABORT;
      }
    }
#line 1103 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 39:
#line 446 "src/Sql/parser.y" // lalr1.cc:859
    {
      if (!driver.query_->AddOrderByColumn(yystack_[1].value.as< std::string > (), Query::AggregationType::MIN)) {
        YYABORT;
      }
    }
#line 1113 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 40:
#line 452 "src/Sql/parser.y" // lalr1.cc:859
    {
      
    }
#line 1121 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 41:
#line 455 "src/Sql/parser.y" // lalr1.cc:859
    {

    }
#line 1129 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 42:
#line 459 "src/Sql/parser.y" // lalr1.cc:859
    {
      if (!driver.query_->AddGroupByColumn(yystack_[0].value.as< std::string > ())) {
        YYABORT;
      }
    }
#line 1139 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 43:
#line 465 "src/Sql/parser.y" // lalr1.cc:859
    {
      
    }
#line 1147 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 44:
#line 468 "src/Sql/parser.y" // lalr1.cc:859
    {

    }
#line 1155 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 45:
#line 473 "src/Sql/parser.y" // lalr1.cc:859
    {
      if (driver.debug()) {
        std::cout << "Query SELECT" << std::endl;
      }
    }
#line 1165 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 46:
#line 480 "src/Sql/parser.y" // lalr1.cc:859
    {
      if (driver.debug()) {
        // std::cout << "SELECT - " << Strings::Join(driver.columns(), ", ")
        //           << std::endl;
      }
    }
#line 1176 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 47:
#line 488 "src/Sql/parser.y" // lalr1.cc:859
    {
      if (driver.debug()) {
        // std::cout << "FROM - " << Strings::Join(driver.tables(), ", ")
        //           << std::endl;
      }
    }
#line 1187 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 48:
#line 496 "src/Sql/parser.y" // lalr1.cc:859
    { /* nill */ }
#line 1193 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 49:
#line 497 "src/Sql/parser.y" // lalr1.cc:859
    {
      if (driver.debug()) {
        std::cout << "WHERE stmt parsed" << std::endl;
      }
    }
#line 1203 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 50:
#line 504 "src/Sql/parser.y" // lalr1.cc:859
    { /* nill */ }
#line 1209 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 51:
#line 505 "src/Sql/parser.y" // lalr1.cc:859
    {
      // if (driver.debug()) {
      //   std::cout << "ORDER BY " << std::endl;
      // }
    }
#line 1219 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 52:
#line 512 "src/Sql/parser.y" // lalr1.cc:859
    { /* nill */ }
#line 1225 "src/Sql/parser.cc" // lalr1.cc:859
    break;

  case 53:
#line 513 "src/Sql/parser.y" // lalr1.cc:859
    {
      // if (driver.debug()) {
      //   std::cout << "ORDER BY " << std::endl;
      // }
    }
#line 1235 "src/Sql/parser.cc" // lalr1.cc:859
    break;


#line 1239 "src/Sql/parser.cc" // lalr1.cc:859
            default:
              break;
            }
        }
      catch (const syntax_error& yyexc)
        {
          error (yyexc);
          YYERROR;
        }
      YY_SYMBOL_PRINT ("-> $$ =", yylhs);
      yypop_ (yylen);
      yylen = 0;
      YY_STACK_PRINT ();

      // Shift the result of the reduction.
      yypush_ (YY_NULLPTR, yylhs);
    }
    goto yynewstate;

  /*--------------------------------------.
  | yyerrlab -- here on detecting error.  |
  `--------------------------------------*/
  yyerrlab:
    // If not already recovering from an error, report this error.
    if (!yyerrstatus_)
      {
        ++yynerrs_;
        error (yyla.location, yysyntax_error_ (yystack_[0].state, yyla));
      }


    yyerror_range[1].location = yyla.location;
    if (yyerrstatus_ == 3)
      {
        /* If just tried and failed to reuse lookahead token after an
           error, discard it.  */

        // Return failure if at end of input.
        if (yyla.type_get () == yyeof_)
          YYABORT;
        else if (!yyla.empty ())
          {
            yy_destroy_ ("Error: discarding", yyla);
            yyla.clear ();
          }
      }

    // Else will try to reuse lookahead token after shifting the error token.
    goto yyerrlab1;


  /*---------------------------------------------------.
  | yyerrorlab -- error raised explicitly by YYERROR.  |
  `---------------------------------------------------*/
  yyerrorlab:

    /* Pacify compilers like GCC when the user code never invokes
       YYERROR and the label yyerrorlab therefore never appears in user
       code.  */
    if (false)
      goto yyerrorlab;
    yyerror_range[1].location = yystack_[yylen - 1].location;
    /* Do not reclaim the symbols of the rule whose action triggered
       this YYERROR.  */
    yypop_ (yylen);
    yylen = 0;
    goto yyerrlab1;

  /*-------------------------------------------------------------.
  | yyerrlab1 -- common code for both syntax error and YYERROR.  |
  `-------------------------------------------------------------*/
  yyerrlab1:
    yyerrstatus_ = 3;   // Each real token shifted decrements this.
    {
      stack_symbol_type error_token;
      for (;;)
        {
          yyn = yypact_[yystack_[0].state];
          if (!yy_pact_value_is_default_ (yyn))
            {
              yyn += yyterror_;
              if (0 <= yyn && yyn <= yylast_ && yycheck_[yyn] == yyterror_)
                {
                  yyn = yytable_[yyn];
                  if (0 < yyn)
                    break;
                }
            }

          // Pop the current state because it cannot handle the error token.
          if (yystack_.size () == 1)
            YYABORT;

          yyerror_range[1].location = yystack_[0].location;
          yy_destroy_ ("Error: popping", yystack_[0]);
          yypop_ ();
          YY_STACK_PRINT ();
        }

      yyerror_range[2].location = yyla.location;
      YYLLOC_DEFAULT (error_token.location, yyerror_range, 2);

      // Shift the error token.
      error_token.state = yyn;
      yypush_ ("Shifting", error_token);
    }
    goto yynewstate;

    // Accept.
  yyacceptlab:
    yyresult = 0;
    goto yyreturn;

    // Abort.
  yyabortlab:
    yyresult = 1;
    goto yyreturn;

  yyreturn:
    if (!yyla.empty ())
      yy_destroy_ ("Cleanup: discarding lookahead", yyla);

    /* Do not reclaim the symbols of the rule whose action triggered
       this YYABORT or YYACCEPT.  */
    yypop_ (yylen);
    while (1 < yystack_.size ())
      {
        yy_destroy_ ("Cleanup: popping", yystack_[0]);
        yypop_ ();
      }

    return yyresult;
  }
    catch (...)
      {
        YYCDEBUG << "Exception caught: cleaning lookahead and stack"
                 << std::endl;
        // Do not try to display the values of the reclaimed symbols,
        // as their printer might throw an exception.
        if (!yyla.empty ())
          yy_destroy_ (YY_NULLPTR, yyla);

        while (1 < yystack_.size ())
          {
            yy_destroy_ (YY_NULLPTR, yystack_[0]);
            yypop_ ();
          }
        throw;
      }
  }

  void
   Parser ::error (const syntax_error& yyexc)
  {
    error (yyexc.location, yyexc.what());
  }

  // Generate an error message.
  std::string
   Parser ::yysyntax_error_ (state_type yystate, const symbol_type& yyla) const
  {
    // Number of reported tokens (one for the "unexpected", one per
    // "expected").
    size_t yycount = 0;
    // Its maximum.
    enum { YYERROR_VERBOSE_ARGS_MAXIMUM = 5 };
    // Arguments of yyformat.
    char const *yyarg[YYERROR_VERBOSE_ARGS_MAXIMUM];

    /* There are many possibilities here to consider:
       - If this state is a consistent state with a default action, then
         the only way this function was invoked is if the default action
         is an error action.  In that case, don't check for expected
         tokens because there are none.
       - The only way there can be no lookahead present (in yyla) is
         if this state is a consistent state with a default action.
         Thus, detecting the absence of a lookahead is sufficient to
         determine that there is no unexpected or expected token to
         report.  In that case, just report a simple "syntax error".
       - Don't assume there isn't a lookahead just because this state is
         a consistent state with a default action.  There might have
         been a previous inconsistent state, consistent state with a
         non-default action, or user semantic action that manipulated
         yyla.  (However, yyla is currently not documented for users.)
       - Of course, the expected token list depends on states to have
         correct lookahead information, and it depends on the parser not
         to perform extra reductions after fetching a lookahead from the
         scanner and before detecting a syntax error.  Thus, state
         merging (from LALR or IELR) and default reductions corrupt the
         expected token list.  However, the list is correct for
         canonical LR with one exception: it will still contain any
         token that will not be accepted due to an error action in a
         later state.
    */
    if (!yyla.empty ())
      {
        int yytoken = yyla.type_get ();
        yyarg[yycount++] = yytname_[yytoken];
        int yyn = yypact_[yystate];
        if (!yy_pact_value_is_default_ (yyn))
          {
            /* Start YYX at -YYN if negative to avoid negative indexes in
               YYCHECK.  In other words, skip the first -YYN actions for
               this state because they are default actions.  */
            int yyxbegin = yyn < 0 ? -yyn : 0;
            // Stay within bounds of both yycheck and yytname.
            int yychecklim = yylast_ - yyn + 1;
            int yyxend = yychecklim < yyntokens_ ? yychecklim : yyntokens_;
            for (int yyx = yyxbegin; yyx < yyxend; ++yyx)
              if (yycheck_[yyx + yyn] == yyx && yyx != yyterror_
                  && !yy_table_value_is_error_ (yytable_[yyx + yyn]))
                {
                  if (yycount == YYERROR_VERBOSE_ARGS_MAXIMUM)
                    {
                      yycount = 1;
                      break;
                    }
                  else
                    yyarg[yycount++] = yytname_[yyx];
                }
          }
      }

    char const* yyformat = YY_NULLPTR;
    switch (yycount)
      {
#define YYCASE_(N, S)                         \
        case N:                               \
          yyformat = S;                       \
        break
        YYCASE_(0, YY_("syntax error"));
        YYCASE_(1, YY_("syntax error, unexpected %s"));
        YYCASE_(2, YY_("syntax error, unexpected %s, expecting %s"));
        YYCASE_(3, YY_("syntax error, unexpected %s, expecting %s or %s"));
        YYCASE_(4, YY_("syntax error, unexpected %s, expecting %s or %s or %s"));
        YYCASE_(5, YY_("syntax error, unexpected %s, expecting %s or %s or %s or %s"));
#undef YYCASE_
      }

    std::string yyres;
    // Argument number.
    size_t yyi = 0;
    for (char const* yyp = yyformat; *yyp; ++yyp)
      if (yyp[0] == '%' && yyp[1] == 's' && yyi < yycount)
        {
          yyres += yytnamerr_ (yyarg[yyi++]);
          ++yyp;
        }
      else
        yyres += *yyp;
    return yyres;
  }


  const signed char  Parser ::yypact_ninf_ = -14;

  const signed char  Parser ::yytable_ninf_ = -1;

  const signed char
   Parser ::yypact_[] =
  {
      27,   -14,   -14,   -14,   -14,   -14,    54,    54,   -14,    -8,
      54,    76,     3,   -14,   -11,   -14,    56,   -14,   -14,    13,
      18,    19,    20,    21,   -14,    22,    65,    54,    54,    54,
      54,    54,    54,    54,    54,    54,   -14,    34,    32,    37,
      25,    44,    64,    75,    -8,   -14,    -4,    -4,   -14,   -14,
     -14,    29,    29,    56,    88,   -14,    62,    54,    81,    77,
      78,    79,    80,    82,    83,   -14,    87,    76,    93,    84,
     -14,   -14,   -14,   -14,   -14,   -14,   -14,   -14,    85,   -14,
     -13,   -14,    93,   -14,    86,    89,    90,    91,    92,    94,
     -14,   -14,    96,    98,   104,   105,   106,   -13,    95,    97,
      99,   100,   101,   -14,   -14,   -14,   -14,   -14,   -14
  };

  const unsigned char
   Parser ::yydefact_[] =
  {
       0,     2,     3,     4,     5,     6,     0,     0,     7,     0,
       0,    20,     0,    21,     0,    13,    18,    30,    24,     0,
       0,     0,     0,     0,    32,    46,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     1,     0,    48,     0,
       0,     0,     0,     0,     0,    19,     8,     9,    10,    11,
      12,    14,    15,    16,    17,    22,    47,     0,    52,     0,
       0,     0,     0,     0,     0,    33,     0,    49,     0,    50,
      25,    31,    26,    27,    28,    29,    23,    42,    43,    53,
       0,    45,     0,    34,     0,     0,     0,     0,     0,    40,
      51,    44,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,    41,    35,    36,    37,    38,    39
  };

  const signed char
   Parser ::yypgoto_[] =
  {
     -14,    -6,   -14,   -14,   102,   -14,    31,   -14,    50,   -14,
     -14,   -14,   -14,   -14,   -14,   -14
  };

  const signed char
   Parser ::yydefgoto_[] =
  {
      -1,    11,    12,    56,    24,    25,    89,    90,    78,    79,
      13,    14,    38,    58,    81,    69
  };

  const unsigned char
   Parser ::yytable_[] =
  {
      15,    16,    17,    36,    26,    83,    29,    30,    31,    37,
      18,    84,    85,    86,    87,    88,    19,    20,    21,    22,
      23,    46,    47,    48,    49,    50,    51,    52,    53,    54,
       1,     2,     3,     4,     5,    60,     6,    27,    28,    29,
      30,    31,    39,    61,     7,     8,     9,    40,    41,    42,
      43,    67,    55,    57,    44,    59,    10,     1,     2,     3,
       4,     5,    62,     6,    27,    28,    29,    30,    31,    32,
      33,     7,     8,    27,    28,    29,    30,    31,    32,    33,
      34,    35,    63,    10,    27,    28,    29,    30,    31,    32,
      33,    34,    35,    64,    66,    45,    27,    28,    29,    30,
      31,    32,    33,    34,    68,    76,    80,    70,    71,    72,
      73,    77,    74,    75,    98,    92,    99,    82,    93,    94,
      95,    96,   100,   101,   102,   104,    97,   105,   103,   106,
     107,   108,    91,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    65
  };

  const signed char
   Parser ::yycheck_[] =
  {
       6,     7,    10,     0,    10,    18,    10,    11,    12,    20,
      18,    24,    25,    26,    27,    28,    24,    25,    26,    27,
      28,    27,    28,    29,    30,    31,    32,    33,    34,    35,
       3,     4,     5,     6,     7,    10,     9,     8,     9,    10,
      11,    12,    29,    18,    17,    18,    19,    29,    29,    29,
      29,    57,    18,    21,    32,    18,    29,     3,     4,     5,
       6,     7,    18,     9,     8,     9,    10,    11,    12,    13,
      14,    17,    18,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    18,    29,     8,     9,    10,    11,    12,    13,
      14,    15,    16,    18,    32,    30,     8,     9,    10,    11,
      12,    13,    14,    15,    23,    18,    22,    30,    30,    30,
      30,    18,    30,    30,    18,    29,    18,    32,    29,    29,
      29,    29,    18,    18,    18,    30,    32,    30,    97,    30,
      30,    30,    82,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    44
  };

  const unsigned char
   Parser ::yystos_[] =
  {
       0,     3,     4,     5,     6,     7,     9,    17,    18,    19,
      29,    35,    36,    44,    45,    35,    35,    10,    18,    24,
      25,    26,    27,    28,    38,    39,    35,     8,     9,    10,
      11,    12,    13,    14,    15,    16,     0,    20,    46,    29,
      29,    29,    29,    29,    32,    30,    35,    35,    35,    35,
      35,    35,    35,    35,    35,    18,    37,    21,    47,    18,
      10,    18,    18,    18,    18,    38,    32,    35,    23,    49,
      30,    30,    30,    30,    30,    30,    18,    18,    42,    43,
      22,    48,    32,    18,    24,    25,    26,    27,    28,    40,
      41,    42,    29,    29,    29,    29,    29,    32,    18,    18,
      18,    18,    18,    40,    30,    30,    30,    30,    30
  };

  const unsigned char
   Parser ::yyr1_[] =
  {
       0,    34,    35,    35,    35,    35,    35,    35,    35,    35,
      35,    35,    35,    35,    35,    35,    35,    35,    35,    35,
      36,    36,    37,    37,    38,    38,    38,    38,    38,    38,
      38,    38,    39,    39,    40,    40,    40,    40,    40,    40,
      41,    41,    42,    43,    43,    44,    45,    46,    47,    47,
      48,    48,    49,    49
  };

  const unsigned char
   Parser ::yyr2_[] =
  {
       0,     2,     1,     1,     1,     1,     1,     1,     3,     3,
       3,     3,     3,     2,     3,     3,     3,     3,     2,     3,
       1,     1,     1,     3,     1,     4,     4,     4,     4,     4,
       1,     4,     1,     3,     1,     4,     4,     4,     4,     4,
       1,     3,     1,     1,     3,     5,     2,     2,     0,     2,
       0,     2,     0,     2
  };



  // YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
  // First, the terminals, then, starting at \a yyntokens_, nonterminals.
  const char*
  const  Parser ::yytname_[] =
  {
  "\"EOF\"", "error", "$undefined", "\"Integer\"", "\"Double\"",
  "\"String\"", "\"Char\"", "\"Boolean\"", "\"+\"", "\"-\"", "\"*\"",
  "\"/\"", "\"%%\"", "\"Comparator1\"", "\"Comparator2\"", "\"AND\"",
  "\"OR\"", "\"NOR\"", "\"Identifier\"", "\"SELECT\"", "\"FROM\"",
  "\"WHERE\"", "\"ORDER BY\"", "\"GROUP BY\"", "\"SUM\"", "\"COUNT\"",
  "\"AVG\"", "\"MAX\"", "\"MIN\"", "\"leftparen\"", "\"rightparen\"",
  "\"semicolon\"", "\"comma\"", "UMINUS", "$accept", "expr", "query",
  "table_list", "column_target", "column_list", "order_by_column_target",
  "order_by_column_list", "group_by_column_target", "group_by_column_list",
  "select_query", "select_stmt", "from_stmt", "opt_where_stmt",
  "opt_order_by_stmt", "opt_group_by_stmt", YY_NULLPTR
  };

#if YYDEBUG
  const unsigned short int
   Parser ::yyrline_[] =
  {
       0,   148,   148,   153,   158,   163,   168,   173,   200,   212,
     224,   236,   248,   260,   291,   303,   315,   327,   339,   351,
     358,   359,   361,   366,   372,   377,   382,   387,   392,   397,
     402,   408,   414,   417,   421,   426,   431,   436,   441,   446,
     452,   455,   459,   465,   468,   473,   480,   488,   496,   497,
     504,   505,   512,   513
  };

  // Print the state stack on the debug stream.
  void
   Parser ::yystack_print_ ()
  {
    *yycdebug_ << "Stack now";
    for (stack_type::const_iterator
           i = yystack_.begin (),
           i_end = yystack_.end ();
         i != i_end; ++i)
      *yycdebug_ << ' ' << i->state;
    *yycdebug_ << std::endl;
  }

  // Report on the debug stream that the rule \a yyrule is going to be reduced.
  void
   Parser ::yy_reduce_print_ (int yyrule)
  {
    unsigned int yylno = yyrline_[yyrule];
    int yynrhs = yyr2_[yyrule];
    // Print the symbols being reduced, and their result.
    *yycdebug_ << "Reducing stack by rule " << yyrule - 1
               << " (line " << yylno << "):" << std::endl;
    // The symbols being reduced.
    for (int yyi = 0; yyi < yynrhs; yyi++)
      YY_SYMBOL_PRINT ("   $" << yyi + 1 << " =",
                       yystack_[(yynrhs) - (yyi + 1)]);
  }
#endif // YYDEBUG


#line 11 "src/Sql/parser.y" // lalr1.cc:1167
} //  Sql 
#line 1686 "src/Sql/parser.cc" // lalr1.cc:1167
#line 519 "src/Sql/parser.y" // lalr1.cc:1168


// Bison expects us to provide implementation - otherwise linker complains.
void Sql::Parser::error(const location &loc , const std::string &message) {
  // Location should be initialized inside scanner action, but is not in this
  // example. Let's grab location directly from driver class.
  //cout << "Error: " << message << endl << "Location: " << loc << endl;
  std::cout << "Error: " << message << endl << "Error location: "
            << driver.location() << endl;
}
