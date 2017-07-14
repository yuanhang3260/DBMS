#ifndef QUERY_EXPRESSION_H_
#define QUERY_EXPRESSION_H_

#include <memory>
#include <string>

#include "Base/BaseTypes.h"

#include "Query/Common.h"
#include "Schema/SchemaType.h"

namespace Query {

struct NodeValue {
  int64 v_int64 = 0;
  double v_double = 0;
  std::string v_str;
  char v_char = 0;
  bool v_bool = false;

  ValueType type = UNKNOWN_VALUE_TYPE;
  bool negative = false;

  NodeValue() : type(UNKNOWN_VALUE_TYPE) {}
  NodeValue(ValueType type_arg) : type(type_arg) {}

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
    UNKNOWN_NODE_TYPE,
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
  static std::string NodeTypeStr(ExprTreeNode::Type node_type);

  const NodeValue& value() { return value_; }
  void set_value_type(ValueType value_type) { value_.type = value_type; }

  // Careful! Usually this can only be applied to ConstValueNode and ColumnNode.
  void set_negative() { value_.negative = true; }

  virtual void Print() const {}

  bool valid() const { return valid_; }
  std::string error_msg() const { return error_msg_; }

 protected:
  NodeValue value_;
  bool valid_ = true;
  std::string error_msg_;

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

  // TODO: Init Column node. It takes table schema and try to get the value
  // type.
  // void Init()

  Type type() const override { return ExprTreeNode::TABLE_COLUMN; }

  void Print() const override;

 private:
  std::string table_;
  std::string column_;
};


class OperatorNode : public ExprTreeNode {
 public:
  OperatorNode(OperatorType op,
               std::shared_ptr<ExprTreeNode> left,
               std::shared_ptr<ExprTreeNode> right);

  Type type() const override { return ExprTreeNode::OPERATOR; }

  void Print() const override;

 private:
  // Init() does a lot of work:
  //   1. Check binary operator must have two children nodes.
  //   2. Check children node type must be CONST_VALUE/TABLE_COLUMN/OPERATOR.
  //   3. Check children node value type and make sure they are meaningful for
  //      this operator. Derive value type of this node.
  //   4. Set valid and error msg for this node, based on above checks.
  bool Init();

  ValueType DeriveResultValueType(ValueType t1,
                                        ValueType t2);

  OperatorType op_;
};

}  // namespace Query

#endif  // QUERY_EXPRESSION_H_
