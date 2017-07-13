#ifndef QUERY_EXPRESSION_H_
#define QUERY_EXPRESSION_H_

#include <memory>
#include <string>

#include "Base/BaseTypes.h"

#include "Schema/SchemaType.h"

namespace Query {

struct NodeValue {
  struct Value {
    // Value() = default;
    // Value(const Value& other) {
    //   v_int64 = other.v_int64;
    //   v_double = other.v_double;
    //   v_str = other.v_str;
    //   v_char = other.v_char;
    // }

    int64 v_int64;
    double v_double;
    std::string v_str;
    char v_char;
  };

  enum Type {
    UNKNOWN_TYPE,
    INT64,
    DOUBLE,
    STRING,
    CHAR,
  };

  NodeValue() : type(UNKNOWN_TYPE) {}

  template<typename T>
  NodeValue(T v, Type type_arg) : type(type_arg) {
    switch(type) {
      case INT64:
        value.v_int64 = v;
        break;
      case DOUBLE:
        value.v_double = v;   
        break;
      case STRING:
        value.v_str = v;
        break;
      case CHAR:
        value.v_char = v;
        break;
      default:
        break;
    }
  }

  Value value;
  Type type;
};

class ExprTreeNode {
 public:
  enum Type {
    UNKNOWN_TYPE,
    CONST_VALUE,    // const value
    TABLE_COLUMN,   // table column indentifier.
    OPERATOR,       // e.g. person.age > 18;
    LOGICAL,        // e.g. expr AND|OR exprt, NOT expr
  };

  virtual Type type() const = 0;
  virtual const NodeValue& value() const = 0;
};

class ConstValueNode : public ExprTreeNode {
 public:
  ConstValueNode(const NodeValue& value) : value_(value) {}

  Type type() const override { return ExprTreeNode::CONST_VALUE; }
  const NodeValue& value() const override { return value_; }

 private:
  NodeValue value_;
};

}  // namespace Query

#endif  // QUERY_EXPRESSION_H_
