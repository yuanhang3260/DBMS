#include "Strings/Split.h"
#include "Strings/Utils.h"

#include "Storage/Record.h"
#include "Query/SqlQuery.h"

namespace Query {

namespace {
const double kSearchThreshold = 0.3;
}

SqlQuery::SqlQuery(DB::CatalogManager* catalog_m) : catalog_m_(catalog_m) {}

void SqlQuery::reset() {
  expr_node_.reset();
  tables_.clear();
  columns_.clear();
  columns_num_ = 0;
}

void SqlQuery::SetExprNode(std::shared_ptr<Query::ExprTreeNode> node) {
  expr_node_ = node;
}

std::shared_ptr<Query::ExprTreeNode> SqlQuery::GetExprNode() {
  return expr_node_;
}

std::string SqlQuery::DefaultTable() const {
  if (tables_.size() == 1) {
    return *tables_.begin();
  }
  return "";
}

bool SqlQuery::AddTable(const std::string& table) {
  tables_.insert(table);
  return true;
}

bool SqlQuery::AddColumn(const std::string& name) {
  Column column;
  if (!ParseTableColumn(name, &column)) {
    return false;
  }

  ColumnRequest column_request;
  column_request.column = column;
  column_request.request_pos = ++columns_num_;

  auto& table_columns = columns_[column.table_name];
  if (table_columns.find(column.column_name) != table_columns.end()) {
    return true;
  }
  table_columns.emplace(column.column_name, column_request);

  return true;
}

bool SqlQuery::ParseTableColumn(const std::string& name, Column* column) {
  auto re = Strings::Split(name, ".");
  if (re.size() == 1) {
    column->column_name = re.at(0);
  } else if (re.size() == 2) {
    column->table_name = re.at(0);
    column->column_name = re.at(1);
  } else {
    error_msg_ = Strings::StrCat("Invalid column name ", name);
    return false;
  }
  return true;
}

bool SqlQuery::TableIsValid(const std::string& table_name) {
  auto table_m = catalog_m_->FindTableByName(table_name);
  if (table_m == nullptr) {
    error_msg_ = Strings::StrCat("Invalid table \"", table_name, "\"");
    return false;
  }

  return true;
}

bool SqlQuery::ColumnIsValid(const Column& column) {
  auto table_m = catalog_m_->FindTableByName(column.table_name);
  if (table_m == nullptr) {
    error_msg_ = Strings::StrCat("Invalid table \"", column.table_name, "\"");
    //LogERROR("ERROR - %s", error_msg_.c_str());
    return false;
  }

  if (column.column_name == "*") {
    return true;
  }

  auto field_m = table_m->FindFieldByName(column.column_name);
  if (field_m == nullptr) {
    error_msg_ = Strings::StrCat("Invalid column \"", column.column_name,
                                 "\" in table \"", column.table_name, "\"");
    //LogERROR("ERROR - %s", error_msg_.c_str());
    return false;
  }

  return true;
}

DB::TableInfoManager* SqlQuery::FindTable(const std::string& table) {
  auto re = catalog_m_->FindTableByName(table);
  if (re == nullptr) {
    error_msg_ = Strings::StrCat("Invalid table \"", table, "\"");
  }
  return re;
}

DB::FieldInfoManager* SqlQuery::FindTableColumn(const Column& column) {
  if (column.table_name.empty()) {
    error_msg_ = Strings::StrCat("Ambiguous column \"", column.column_name,
                                 "\", table name needed");
    //LogERROR("ERROR - %s", error_msg_.c_str());
    return nullptr;
  }

  auto re = catalog_m_->FindTableFieldByName(column);
  if (re == nullptr) {
    error_msg_ = Strings::StrCat("Invalid column \"", column.column_name,
                                 "\" in table \"", column.table_name, "\"");
    //LogERROR("ERROR - %s", error_msg_.c_str());
  }
  return re;
}

const ColumnRequest* SqlQuery::FindColumnRequest(const Column& column) {
  auto iter = columns_.find(column.table_name);
  if (iter == columns_.end()) {
    return nullptr;
  }

  auto columns = iter->second;
  auto column_request_iter = columns.find(column.column_name);
  if (column_request_iter == columns.end()) {
    return nullptr;
  }

  return &(column_request_iter->second);
}

bool SqlQuery::FinalizeParsing() {
  // Apply default table.
  std::string default_table = DefaultTable();
  auto iter = columns_.find("");
  if (iter != columns_.end()) {
    if (default_table.empty()) {
      error_msg_ = Strings::StrCat("Default table not found.");
      return false;
    }
    auto& columns = iter->second;
    for (auto& column_request_iter : columns) {
      column_request_iter.second.column.table_name = default_table;
      auto& table_columns = columns_[default_table];
      if (table_columns.find(column_request_iter.first) !=
          table_columns.end()) {
        continue;
      }
      table_columns.emplace(column_request_iter.first,
                            column_request_iter.second);
    }
    columns_.erase(iter);
  }

  // Expand * columns
  for (auto& columns_iter : columns_) {
    const auto& table_name = columns_iter.first;
    auto& columns = columns_iter.second;
    auto iter = columns.find("*");
    if (iter == columns.end()) {
      continue;
    }
    auto table_m = FindTable(table_name);
    CHECK(table_m != nullptr, Strings::StrCat("Can't find table ", table_name));
    for (uint32 i = 0; i < table_m->NumFields(); i++) {
      auto field_m = table_m->FindFieldByIndex(i);
      CHECK(field_m != nullptr,
            Strings::StrCat("Can't find field ", std::to_string(i),
                            " in table ", table_name));
      if (columns.find(field_m->name()) != columns.end()) {
        continue;
      }
      ColumnRequest column_request;
      column_request.column.table_name = table_name;
      column_request.column.column_name = field_m->name();
      column_request.request_pos = iter->second.request_pos;
      column_request.sub_request_pos = i;
      columns.emplace(field_m->name(), column_request);
    }
    columns.erase(iter);
  }

  // Re-assign column request positions.
  std::vector<ColumnRequest*> columns_list;
  for (auto& iter : columns_) {
    for (auto& column_request_iter : iter.second) {
      columns_list.push_back(&column_request_iter.second);
    }
  }

  auto comparator = [&] (const ColumnRequest* c1,
                         const ColumnRequest* c2) {
    if (c1->request_pos < c2->request_pos) {
      return true;
    } else if (c1->request_pos > c2->request_pos) {
      return false;
    } else {
      if (c1->sub_request_pos < c2->sub_request_pos) {
        return true;
      } else if (c1->sub_request_pos > c2->sub_request_pos) {
        return false;
      } else {
        return false;
      }
    }
  };

  std::sort(columns_list.begin(), columns_list.end(), comparator);
  for (uint32 i = 0; i < columns_list.size(); i++) {
    columns_list.at(i)->request_pos = i;
  }

  // Check all tables and columns are valid.
  for (const auto& table_name : tables_) {
    if (!TableIsValid(table_name)) {
      return false;
    }
  }

  for (auto& iter : columns_) {
    const auto& table_name = iter.first;
    if (tables_.find(table_name) == tables_.end()) {
      error_msg_ = Strings::StrCat("Table %s ", table_name.c_str(),
                                   " not in table selection list");
      return false;
    }
    auto& columns = iter.second;
    for (auto& column_request_iter : columns) {
      auto field_m = FindTableColumn(column_request_iter.second.column);
      if (field_m == nullptr) {
        return false;
      }
      column_request_iter.second.column.index = field_m->index();
      column_request_iter.second.column.type = field_m->type();
    }
  }

  // Check expression tree root returns bool.
  if (!expr_node_) {
    return false;
  }
  if (expr_node_->value().type != ValueType::BOOL) {
    return false;
  }

  return true;
}

bool SqlQuery::GroupPhysicalQueries(ExprTreeNode* node) {
  if (node == nullptr) {
    return true;
  }

  bool left_re = GroupPhysicalQueries(node->left());
  bool right_re = GroupPhysicalQueries(node->right());

  bool re = left_re && right_re;
  if (re) {
    if (node->type() == ExprTreeNode::OPERATOR) {
      OperatorNode* op_node = dynamic_cast<OperatorNode*>(node);
      if (op_node->OpType() == OR || op_node->OpType() == NOT) {
        re = false;
      }
    }
  }

  // Set this node as a root of "physical query", which means from this node,
  // a physical plan may be executed.
  if (re) {
    node->set_physical_query_root(true);
    if (node->left()) {
      node->left()->set_physical_query_root(false);
    }
    if (node->right()) {
      node->right()->set_physical_query_root(false);
    }
  }

  return re;
}

PhysicalPlan* SqlQuery::EvaluateNodePhysicalPlans(ExprTreeNode* node) {
  CHECK(node != nullptr, "nullptr passed to EvaluatePhysicalQueryPlans");

  if (node->physical_query_root()) {
    // Evaluate physical query root.
    auto physical_plan = PlanPhysicalQuery(node);
    EvaluateQueryConditions(physical_plan);
    return physical_plan;
  }

  const PhysicalPlan *left_plan = nullptr, *right_plan = nullptr;
  if (node->left()) {
    left_plan = EvaluateNodePhysicalPlans(node->left());
  }
  if (node->right()) {
    right_plan = EvaluateNodePhysicalPlans(node->right());
  }

  CHECK(node->type() == ExprTreeNode::OPERATOR,
        Strings::StrCat("Expect OPERATOR node, but got %s",
                        ExprTreeNode::NodeTypeStr(node->type()).c_str()));

  OperatorNode* op_node = dynamic_cast<OperatorNode*>(node);
  PhysicalPlan* this_plan = node->mutable_physical_plan();
  if (op_node->OpType() == AND) {
    CHECK(left_plan != nullptr, "AND node has no left child");
    CHECK(right_plan != nullptr, "AND node has no right child");
    // If AND node has a child returning a direct False, this AND node also
    // returns direct False.
    if (left_plan->plan == PhysicalPlan::CONST_FALSE_SKIP ||
        right_plan->plan == PhysicalPlan::CONST_FALSE_SKIP) {
      this_plan->plan = PhysicalPlan::CONST_FALSE_SKIP;
      this_plan->query_ratio = 0.0;
    } else if (left_plan->plan == PhysicalPlan::CONST_TRUE_SCAN &&
               right_plan->plan == PhysicalPlan::CONST_TRUE_SCAN) {
      this_plan->plan = PhysicalPlan::CONST_TRUE_SCAN;
      node->set_physical_query_root(true);
      this_plan->query_ratio = 1.0;
    } else {
      this_plan->query_ratio = std::min(left_plan->query_ratio,
                                        right_plan->query_ratio);
      if (left_plan->plan == PhysicalPlan::SEARCH ||
          right_plan->plan == PhysicalPlan::SEARCH) {
        this_plan->plan = PhysicalPlan::POP;
        this_plan->pop_node =
            left_plan->query_ratio < right_plan->query_ratio ?
                PhysicalPlan::LEFT : PhysicalPlan::RIGHT;
      } else {
        this_plan->plan = PhysicalPlan::SCAN;
        node->set_physical_query_root(true);
        this_plan->query_ratio = 1.0;
      }
    }
  } else if (op_node->OpType() == OR) {
    CHECK(left_plan != nullptr, "OR node has no left child");
    CHECK(right_plan != nullptr, "OR node has no right child");

    if (left_plan->plan == PhysicalPlan::CONST_FALSE_SKIP &&
        right_plan->plan == PhysicalPlan::CONST_FALSE_SKIP) {
      this_plan->plan = PhysicalPlan::CONST_FALSE_SKIP;
      this_plan->query_ratio = 0.0;
    } else if (left_plan->plan == PhysicalPlan::CONST_TRUE_SCAN ||
               right_plan->plan == PhysicalPlan::CONST_TRUE_SCAN) {
      this_plan->plan = PhysicalPlan::CONST_TRUE_SCAN;
      node->set_physical_query_root(true);
      this_plan->query_ratio = 1.0;
    } else {
      this_plan->query_ratio = left_plan->query_ratio + right_plan->query_ratio;
      this_plan->query_ratio = std::min(1.0, this_plan->query_ratio);
      if (left_plan->plan == PhysicalPlan::SCAN ||
          right_plan->plan == PhysicalPlan::SCAN) {
        this_plan->plan = PhysicalPlan::SCAN;
        node->set_physical_query_root(true);
        this_plan->query_ratio = 1.0;
      } else {
        this_plan->plan = PhysicalPlan::POP;
        this_plan->pop_node = PhysicalPlan::BOTH;
      }
    }
  } else if (op_node->OpType() == NOT) {
    CHECK(left_plan != nullptr, "NOT node has no child expression");
    if (left_plan->plan == PhysicalPlan::CONST_FALSE_SKIP) {
      this_plan->plan = PhysicalPlan::CONST_TRUE_SCAN;
      node->set_physical_query_root(true);
      this_plan->query_ratio = 1.0;
    } else if (left_plan->plan == PhysicalPlan::CONST_TRUE_SCAN) {
      this_plan->plan = PhysicalPlan::CONST_FALSE_SKIP;
      this_plan->query_ratio = 0;
    } else {
      this_plan->plan = PhysicalPlan::SCAN;
      node->set_physical_query_root(true);
      this_plan->query_ratio = 1.0;
    }
  } else {
    LogFATAL("Expect logical operator type, but got %s",
             OpTypeStr(op_node->OpType()).c_str());
  }

  if (this_plan->query_ratio > kSearchThreshold) {
    if (this_plan->plan == PhysicalPlan::SEARCH) {
      this_plan->plan = PhysicalPlan::SCAN;
    }
    node->set_physical_query_root(true);
    this_plan->query_ratio = 1.0;
  }

  return this_plan;
}

PhysicalPlan* SqlQuery::PlanPhysicalQuery(ExprTreeNode* node) {
  CHECK(node->value_type() == ValueType::BOOL,
        Strings::StrCat("Expect physical query expr returns BOOL, but got %s",
                        ValueTypeStr(node->value_type()).c_str()));

  auto* this_plan = node->mutable_physical_plan();
  if (IsConstExpression(node)) {
    Storage::RecordBase dummy_record; 
    EvaluateArgs evalute_args(catalog_m_, dummy_record,
                              Storage::UNKNOWN_RECORDTYPE, {});

    NodeValue result = node->Evaluate(evalute_args);
    CHECK(result.type == ValueType::BOOL,
          Strings::StrCat("Expect const expression value as BOOL, but got %s",
                          ValueTypeStr(result.type).c_str()));
    if (node->value().v_bool) {
      this_plan->plan = PhysicalPlan::CONST_FALSE_SKIP;
      this_plan->query_ratio = 0.0;
    } else {
      this_plan->plan = PhysicalPlan::CONST_TRUE_SCAN;
      this_plan->query_ratio = 1.0;
    }
  } else if (node->type() == ExprTreeNode::OPERATOR) {
    CHECK(node->left() != nullptr, "Op node has no left child");
    CHECK(node->right() != nullptr, "Op node has no right child");

    OperatorNode* op_node = dynamic_cast<OperatorNode*>(node);
    if (op_node->OpType() == AND) {
      PhysicalPlan* left_plan = PlanPhysicalQuery(node->left());
      PhysicalPlan* right_plan = PlanPhysicalQuery(node->right());

      if (left_plan->plan == PhysicalPlan::CONST_FALSE_SKIP ||
          right_plan->plan == PhysicalPlan::CONST_FALSE_SKIP) {
        this_plan->plan = PhysicalPlan::CONST_FALSE_SKIP;
        this_plan->query_ratio = 0.0;
      } else if (left_plan->plan == PhysicalPlan::CONST_TRUE_SCAN &&
                 right_plan->plan == PhysicalPlan::CONST_TRUE_SCAN) {
        this_plan->plan = PhysicalPlan::CONST_TRUE_SCAN;
        this_plan->query_ratio = 1.0;
      } else if (left_plan->plan == PhysicalPlan::SEARCH ||
                 right_plan->plan == PhysicalPlan::SEARCH) {
        this_plan->plan = PhysicalPlan::SEARCH;
        if (left_plan->plan == PhysicalPlan::SEARCH) {
          this_plan->conditions.insert(this_plan->conditions.end(),
                                       left_plan->conditions.begin(),
                                       left_plan->conditions.end());
        }
        if (right_plan->plan == PhysicalPlan::SEARCH) {
          this_plan->conditions.insert(this_plan->conditions.end(),
                                       right_plan->conditions.begin(),
                                       right_plan->conditions.end());
        }
      } else {
        this_plan->plan = PhysicalPlan::SCAN;
        this_plan->query_ratio = 1.0;
      }
    } else if (IsCompareOp(op_node->OpType())) {
      if (node->left()->type() == ExprTreeNode::TABLE_COLUMN &&
          node->right()->type() == ExprTreeNode::CONST_VALUE) {
        // Add condition selector into plan.
        QueryCondition condition;
        condition.column = dynamic_cast<ColumnNode*>(node->left())->column();
        condition.op = op_node->OpType();
        condition.value = node->right()->value();
        this_plan->plan = PhysicalPlan::SEARCH;
        this_plan->conditions.push_back(condition);
      } else if (node->left()->type() == ExprTreeNode::CONST_VALUE &&
                 node->right()->type() == ExprTreeNode::TABLE_COLUMN) {
        // Add condition selector into plan.
        QueryCondition condition;
        condition.column = dynamic_cast<ColumnNode*>(node->right())->column();
        condition.op = FlipOp(op_node->OpType());
        condition.value = node->left()->value();
        this_plan->plan = PhysicalPlan::SEARCH;
        this_plan->conditions.push_back(condition);
      } else {
        this_plan->plan = PhysicalPlan::SCAN;
        this_plan->query_ratio = 1.0;
      }
    } else {
      LogFATAL("Unexpected Operator type %s for physical query root",
               OpTypeStr(op_node->OpType()).c_str());
    }
  }

  return this_plan;
}

bool SqlQuery::IsConstExpression(ExprTreeNode* node) {
  if (!node) {
    return true;
  }

  if (node->type() == ExprTreeNode::CONST_VALUE) {
    return true;
  } else if (node->type() == ExprTreeNode::TABLE_COLUMN) {
    return false;
  } else if (node->type() == ExprTreeNode::OPERATOR) {
    bool left_re = IsConstExpression(node->left());
    bool right_re = IsConstExpression(node->right());
    return left_re && right_re;
  } else {
    LogFATAL("Invalid node type %s",
             ExprTreeNode::NodeTypeStr(node->type()).c_str());
  }
  return false;
}

void SqlQuery::EvaluateQueryConditions(PhysicalPlan* physical_plan) {
  if (physical_plan->plan == PhysicalPlan::CONST_FALSE_SKIP ||
      physical_plan->plan == PhysicalPlan::CONST_TRUE_SCAN ||
      physical_plan->plan == PhysicalPlan::SCAN) {
    return;
  }

  auto& conditions = physical_plan->conditions;
  if (conditions.empty()) {
    return;
  }

  for (auto& condition : conditions) {
    CastValueType(&condition);
  }

  // Group conditions by column.
  auto column_comparator = [&] (const QueryCondition& c1,
                                const QueryCondition& c2) {
    return c1.column.index < c2.column.index;
  };

  std::sort(conditions.begin(), conditions.end(), column_comparator);

  std::vector<std::vector<const QueryCondition*>> condition_groups;
  int crt_index = -1;
  for (const auto& condition : conditions) {
    if (condition.column.index != crt_index) {
      condition_groups.push_back(std::vector<const QueryCondition*>());
      crt_index = condition.column.index;
    }
    condition_groups.back().push_back(&condition);
  }

  // Analyze one group.
  auto analyze_condition_group =
  [&] (const std::vector<const QueryCondition*>& group) {
    PhysicalPlan group_plan;
    const QueryCondition* equal_condition = nullptr;
    std::vector<const QueryCondition*> comparing_conditions;
    // Pass 1: Rule out const false conditions and conflict equal conditions.
    for (const auto& condition : group) {
      if (condition->is_const) {
        if (!condition->const_result) {
          group_plan.plan = PhysicalPlan::CONST_FALSE_SKIP;
          group_plan.query_ratio = 0.0;
          return group_plan;
        } else {
          continue;
        }
      }

      if (condition->op == EQUAL) {
        if (equal_condition && condition->value != equal_condition->value) {
          // Two differernt equal conditions confict. This group will produce
          // const false;
          group_plan.plan = PhysicalPlan::CONST_FALSE_SKIP;
          group_plan.query_ratio = 0.0;
          return group_plan;
        }
        equal_condition = condition;
      }
    }

    // Pass 2: Rule out confict equal-nonequal conditions, and other conficts.
    for (const auto& condition : group) {
      if (condition->is_const) {
        // Const false conditions has been ruled out. Skip const true.
        continue;
      }
      if (condition->op == EQUAL) {
        continue;
      }

      if (condition->op == NONEQUAL) {
        if (equal_condition && condition->value == equal_condition->value) {
          // A equal == condition conficts with a non-equal condition.
          group_plan.plan = PhysicalPlan::CONST_FALSE_SKIP;
          group_plan.query_ratio = 0.0;
          return group_plan;
        }
      } else {  // <, >, <=, or >= conditions
        if (equal_condition) {
          // Previous equal condition conficts with <, >, <=, or >= conditions.
          if ((condition->op == LT &&
               condition->value <= equal_condition->value) ||
              (condition->op == GT &&
               condition->value >= equal_condition->value) ||
              (condition->op == LE &&
               condition->value < equal_condition->value) ||
              (condition->op == GE &&
               condition->value > equal_condition->value)) {
            group_plan.plan = PhysicalPlan::CONST_FALSE_SKIP;
            group_plan.query_ratio = 0.0;
            return group_plan;
          }
        } else {
          comparing_conditions.push_back(condition);
        }
      }
    }

    // Merge conditions and conclude physical query plan.
    if (equal_condition) {
      group_plan.plan = PhysicalPlan::SEARCH;
      group_plan.conditions.push_back(*equal_condition);
    } else if (!comparing_conditions.empty()) {
      // Split conditions to two groups {<, <=} and {>, >=}.
      std::vector<const QueryCondition*> downlimit_conditions;
      std::vector<const QueryCondition*> uplimit_conditions;
      for (const auto& condition: comparing_conditions) {
        if (condition->op == GT || condition->op == GE) {
          downlimit_conditions.push_back(condition);
        }
        if (condition->op == LT || condition->op == LE) {
          uplimit_conditions.push_back(condition);
        }
      }

      auto value_comparator = [&] (const QueryCondition* c1,
                                   const QueryCondition* c2) {
        if (c1->value < c2->value) {
          return true;
        } else if (c1->value > c2->value) {
          return false;
        } else {
          return c1->op < c2->op;
        }
      };
      std::sort(downlimit_conditions.begin(), downlimit_conditions.end(),
                value_comparator);
      std::sort(uplimit_conditions.begin(), uplimit_conditions.end(),
                value_comparator);

      if (!downlimit_conditions.empty() && !uplimit_conditions.empty()) {
        const QueryCondition* downlimit = *downlimit_conditions.end();
        const QueryCondition* uplimit = *uplimit_conditions.begin();
        if (downlimit->value > uplimit->value) {
          group_plan.plan = PhysicalPlan::CONST_FALSE_SKIP;
          group_plan.query_ratio = 0.0;
        } else if (downlimit->value == uplimit->value) {
          if (downlimit->op == GT && uplimit->op == LT) {
            group_plan.plan = PhysicalPlan::CONST_FALSE_SKIP;
            group_plan.query_ratio = 0.0;
          } else {
            group_plan.plan = PhysicalPlan::SEARCH;
            group_plan.conditions.push_back(*downlimit);
            group_plan.conditions.back().op = EQUAL;
          }
        } else {
          group_plan.plan = PhysicalPlan::SEARCH;
          group_plan.conditions.push_back(*downlimit);
          group_plan.conditions.push_back(*uplimit);
        }
      } else if (!downlimit_conditions.empty()) {
        group_plan.plan = PhysicalPlan::SEARCH;
        group_plan.conditions.push_back(**downlimit_conditions.end());
      } else if (!uplimit_conditions.empty()) {
        group_plan.plan = PhysicalPlan::SEARCH;
        group_plan.conditions.push_back(**uplimit_conditions.begin());
      }
    } else {
      group_plan.plan = PhysicalPlan::SCAN;
      group_plan.query_ratio = 1.0;
    }

    // (TODO): Evaluate query ratio for this group.

    return group_plan;
  };

  for (const auto& group : condition_groups) {
    // (TODO): If this column has no index, we have to scan. Otherwise analyze
    // this group conditions and find the search range.
    analyze_condition_group(group);
  }
}

std::string SqlQuery::error_msg() const {
  return error_msg_;
}

void SqlQuery::set_error_msg(const std::string& error_msg) {
  error_msg_ = error_msg;
}

}  // namespace Query
