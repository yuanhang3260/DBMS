#ifndef QUERY_EXPRESSION_H_
#define QUERY_EXPRESSION_H_

#include <memory>
#include <string>

#include "Base/BaseTypes.h"

#include "Schema/SchemaType.h"

namespace Query {

struct NodeValue {
  int64 v_int64 = 0;
  double v_double = 0;
  std::string v_str;
  char v_char = 0;
  bool v_bool = false;

  enum Type {
    UNKNOWN_TYPE,
    INT64,
    DOUBLE,
    STRING,
    CHAR,
    BOOL,
  };

  Type type;

  NodeValue() : type(UNKNOWN_TYPE) {}
  NodeValue(Type type_arg) : type(type_arg) {}

  NodeValue static IntValue(int64 v);
  NodeValue static DoubleValue(double v);
  NodeValue static StringValue(const std::string& v);
  NodeValue static CharValue(char v);
  NodeValue static BoolValue(bool v);

  std::string AsString() const;
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

  ExprTreeNode() {}
  ExprTreeNode(std::shared_ptr<ExprTreeNode> left,
               std::shared_ptr<ExprTreeNode> right) :
      left_(left),
      right_(right) {}

  void set_parent(std::shared_ptr<ExprTreeNode> parent) {
    parent_ = parent;
  }

  void set_left_child(std::shared_ptr<ExprTreeNode> left) {
    left_ = left;
  }

  void set_right_child(std::shared_ptr<ExprTreeNode> right) {
    right_ = right;
  }

  virtual Type type() const = 0;
  virtual const NodeValue& value() { return value_; };

  virtual void Print() const = 0;

  bool valid() const { return valid_; }
  // Implement this for different node types.
  virtual void CheckValid() {}

 protected:
  NodeValue value_;
  bool valid_ = true;

  std::shared_ptr<ExprTreeNode> left_;
  std::shared_ptr<ExprTreeNode> right_;
  std::shared_ptr<ExprTreeNode> parent_;
};


class ConstValueNode : public ExprTreeNode {
 public:
  ConstValueNode(const NodeValue& value) {
    value_ = value;
  }

  Type type() const override { return ExprTreeNode::CONST_VALUE; }

  void Print() const override;
};


class ColumnNode : public ExprTreeNode {
 public:
  ColumnNode(const std::string& table, const std::string& column) :
      table_(table),
      column_(column) {}
  ColumnNode(const std::string& name);

  Type type() const override { return ExprTreeNode::TABLE_COLUMN; }

  void Print() const override;

 private:
  std::string table_;
  std::string column_;
};

class OperatorNode : public ExprTreeNode {
 public:
  OperatorNode(char op) : op_(op) {}
  OperatorNode(char op,
               std::shared_ptr<ExprTreeNode> left,
               std::shared_ptr<ExprTreeNode> right);

  Type type() const override { return ExprTreeNode::OPERATOR; }

  void Print() const override;

 private:
  char op_;
};

}  // namespace Query

#endif  // QUERY_EXPRESSION_H_
