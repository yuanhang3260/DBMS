#ifndef QUERY_EXPRESSION_H_
#define QUERY_EXPRESSION_H_

#include <memory>
#include <string>

#include "Base/BaseTypes.h"
#include "Base/MacroUtils.h"

#include "Database/CatalogManager.h"
#include "Query/Common.h"
#include "Schema/SchemaType.h"
#include "Storage/Record.h"

namespace Query {

struct NodeValue {
  int64 v_int64 = 0;
  double v_double = 0;
  std::string v_str;
  char v_char = 0;
  bool v_bool = false;

  ValueType type = UNKNOWN_VALUE_TYPE;
  bool negative = false;

  bool has_value() const { return has_value_flags_ != 0; }
  byte has_value_flags_ = 0;

  NodeValue() : type(UNKNOWN_VALUE_TYPE) {}
  explicit NodeValue(ValueType type_arg) : type(type_arg) {}

  NodeValue static IntValue(int64 v);
  NodeValue static DoubleValue(double v);
  NodeValue static StringValue(const std::string& v);
  NodeValue static CharValue(char v);
  NodeValue static BoolValue(bool v);

  std::string AsString() const;
};

struct EvaluateArgs {
  EvaluateArgs(DB::CatalogManager* catalog_m_,
               const Storage::RecordBase& record_,
               Storage::RecordType record_type_,
               const std::vector<int> field_indexes_) :
      catalog_m(catalog_m_),
      record(record_),
      record_type(record_type_),
      field_indexes(field_indexes_) {}

  DB::CatalogManager* catalog_m;
  const Storage::RecordBase& record;
  Storage::RecordType record_type;
  // If record is a index record, we need to know the index nubmers.
  const std::vector<int> field_indexes; 
};

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

  // Careful! This can only be applied to ConstValueNode and ColumnNode.
  void set_negative(bool neg);

  virtual void Print() const {}

  bool valid() const { return valid_; }
  void set_valid(bool valid) { valid_ = valid; }
  std::string error_msg() const { return error_msg_; }
  void set_error_msg(const std::string error_msg) { error_msg_ = error_msg; }

  virtual NodeValue Evaluate(const EvaluateArgs& arg) const = 0;

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
  explicit ConstValueNode(const NodeValue& value) {
    value_ = value;
  }

  Type type() const override { return ExprTreeNode::CONST_VALUE; }

  void Print() const override;

  NodeValue Evaluate(const EvaluateArgs& arg) const override { return value_; }
};


class ColumnNode : public ExprTreeNode {
 public:
  ColumnNode(const std::string& table, const std::string& column,
             DB::CatalogManager* catalog_m);
  ColumnNode(const std::string& name,
             DB::CatalogManager* catalog_m,
             const std::string& default_table);

  Type type() const override { return ExprTreeNode::TABLE_COLUMN; }
  DEFINE_ACCESSOR(table_name, std::string);
  DEFINE_ACCESSOR(column_name, std::string);

  void Print() const override;

  NodeValue Evaluate(const EvaluateArgs& arg) const override;

 private:
  bool Init(DB::CatalogManager* catalog_m);

  std::string table_name_;
  std::string column_name_;
  int32 field_index_ = -1;
  Schema::FieldType field_type_ = Schema::FieldType::UNKNOWN_TYPE;
};


class OperatorNode : public ExprTreeNode {
 public:
  OperatorNode(OperatorType op,
               std::shared_ptr<ExprTreeNode> left,
               std::shared_ptr<ExprTreeNode> right);

  Type type() const override { return ExprTreeNode::OPERATOR; }

  void Print() const override;

  NodeValue Evaluate(const EvaluateArgs& arg) const override;

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
