#ifndef QUERY_EXPRESSION_H_
#define QUERY_EXPRESSION_H_

#include <memory>
#include <string>

#include "Base/BaseTypes.h"
#include "Base/MacroUtils.h"

#include "Database/CatalogManager.h"
#include "Query/Common.h"
#include "Query/ExecutePlan.h"
#include "Query/Iterator.h"
#include "Query/NodeValue.h"
#include "Schema/SchemaType.h"
#include "Storage/Record.h"

namespace Query {

// struct EvaluateArgs {
//   EvaluateArgs(DB::CatalogManager* catalog_m_,
//                const FetchedResult::Tuple& tuple_) :
//       catalog_m(catalog_m_),
//       tuple(tuple_) {}
//   DB::CatalogManager* catalog_m;
//   const FetchedResult::Tuple& tuple;
// };

class ExprTreeNode {
 public:
  enum Type {
    UNKNOWN_NODE_TYPE,
    CONST_VALUE,    // const value
    TABLE_COLUMN,   // table column indentifier.
    OPERATOR,       // e.g. person.age > 18;
  };

  ExprTreeNode() {}
  ExprTreeNode(std::shared_ptr<ExprTreeNode> left,
               std::shared_ptr<ExprTreeNode> right) :
      left_(left),
      right_(right) {}

  ExprTreeNode* left() const { return left_.get(); }
  ExprTreeNode* right() const { return right_.get(); }

  std::shared_ptr<ExprTreeNode> shared_left() const { return left_; }
  std::shared_ptr<ExprTreeNode> shared_right() const { return right_; }

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
  ValueType value_type() const { return value_.type; }
  void set_value_type(ValueType value_type) { value_.type = value_type; }

  // Careful! This can only be applied to ConstValueNode and ColumnNode.
  void set_negative(bool neg);

  virtual void Print() const {}

  bool valid() const { return valid_; }
  void set_valid(bool valid) { valid_ = valid; }
  std::string error_msg() const { return error_msg_; }
  void set_error_msg(const std::string error_msg) { error_msg_ = error_msg; }

  DEFINE_ACCESSOR(physical_query_root, bool);

  const PhysicalPlan& physical_plan() const { return physical_plan_; }
  PhysicalPlan* mutable_physical_plan() { return &physical_plan_; }

  const FetchedResult& results() const { return results_; }
  FetchedResult* mutable_results() { return &results_; }

  virtual NodeValue Evaluate(const FetchedResult::Tuple& arg) const = 0;

  // Takes ownership of iterator.
  void SetIterator(Iterator* iterator) { tuple_iter_.reset(iterator); }
  Iterator* GetIterator() { return tuple_iter_.get(); }

 protected:
  NodeValue value_;
  bool valid_ = true;
  std::string error_msg_;

  std::shared_ptr<ExprTreeNode> left_;
  std::shared_ptr<ExprTreeNode> right_;
  std::shared_ptr<ExprTreeNode> parent_;

  bool physical_query_root_ = false;
  PhysicalPlan physical_plan_;

  FetchedResult results_;
  std::shared_ptr<Iterator> tuple_iter_;
};


class ConstValueNode : public ExprTreeNode {
 public:
  explicit ConstValueNode(const NodeValue& value) {
    value_ = value;
  }

  Type type() const override { return ExprTreeNode::CONST_VALUE; }

  void Print() const override;

  NodeValue Evaluate(const FetchedResult::Tuple& arg) const;
};


class ColumnNode : public ExprTreeNode {
 public:
  ColumnNode(const Column& column);

  Type type() const override { return ExprTreeNode::TABLE_COLUMN; }

  const Column& column() const { return column_; }

  void Print() const override;

  NodeValue Evaluate(const FetchedResult::Tuple& arg) const override;

 private:
  bool Init();

  Column column_;
};


class OperatorNode : public ExprTreeNode {
 public:
  OperatorNode(OperatorType op,
               std::shared_ptr<ExprTreeNode> left,
               std::shared_ptr<ExprTreeNode> right);

  Type type() const override { return ExprTreeNode::OPERATOR; }

  OperatorType OpType() const { return op_; }

  void Print() const override;

  NodeValue Evaluate(const FetchedResult::Tuple& arg) const override;

 private:
  // Init() does a lot of work to make sure the node is valid:
  //
  //   1. Check binary operator must have two children nodes.
  //   2. Check children node type must be CONST_VALUE/TABLE_COLUMN/OPERATOR.
  //   3. Check children node value type and make sure they are meaningful for
  //      this operator. Derive value type of this node.
  //   4. Set valid and error msg for this node, based on above checks.
  bool Init();

  ValueType DeriveResultValueType(ValueType t1, ValueType t2);

  OperatorType op_;
};

}  // namespace Query

#endif  // QUERY_EXPRESSION_H_
