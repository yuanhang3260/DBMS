#include "Strings/Split.h"
#include "Strings/Utils.h"

#include "Storage/Record.h"
#include "Query/SqlQuery.h"

namespace Query {

namespace {
const double kIndexSearchFactor = 2;
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
  if (!expr_node_ || !expr_node_->valid()) {
    return false;
  }
  if (expr_node_->value().type != ValueType::BOOL) {
    return false;
  }

  return true;
}

const PhysicalPlan& SqlQuery::PrepareQueryPlan() {
  GroupPhysicalQueries(expr_node_.get());
  GenerateQueryPhysicalPlan(expr_node_.get());
  return expr_node_->physical_plan();
}

int SqlQuery::ExecuteSelectQuery() {
  PrepareQueryPlan();
  return ExecuteSelectQueryFromNode(expr_node_.get());
}

int SqlQuery::ExecuteSelectQueryFromNode(ExprTreeNode* node) {
  const auto& this_plan = node->physical_plan();
  if (this_plan.plan == PhysicalPlan::CONST_FALSE_SKIP) {
    return 0;
  }

  if (node->physical_query_root()) {
    return Do_ExecutePhysicalQuery(node);
  }

  CHECK(node->type() == ExprTreeNode::OPERATOR,
        Strings::StrCat("Expect OPERATOR node, but got ",
                        ExprTreeNode::NodeTypeStr(node->type()).c_str()));

  OperatorNode* op_node = dynamic_cast<OperatorNode*>(node);
  if (op_node->OpType() == AND) {
    CHECK(this_plan.plan == PhysicalPlan::POP,
          Strings::StrCat("Expect AND node plan to be POP, but got ",
                          PhysicalPlan::PlanStr(this_plan.plan)));
    ExprTreeNode* pop_node = nullptr, other_node = nullptr;
    if (plan.pop_node == PhysicalPlan::LEFT) {
      pop_node = node->left();
      other_node = node->right();
    } else if (plan.pop_node == PhysicalPlan::RIGHT) {
      pop_node = node->right();
      other_node = node->left();
    } else {
      LogFATAL("No pop node to fetch result for AND node");
    }
    int re = ExecuteSelectQueryFromNode(pop_node);
    // Evaluate the record on the other node.
    for (const auto& tuple : pop_node->results()) {
      EvaluateArgs evalute_args(catalog_m_, *record, Storage::INDEX_DATA, {});

      NodeValue result = node->Evaluate(evalute_args);
      if (result.v_bool) {
        node->mutable_result.push_back(tuple);
      }
    }
    return re;
  } else if (op_node->OpType() == OR) {
    CHECK(this_plan.plan == PhysicalPlan::POP,
          Strings::StrCat("Expect OR node plan to be POP, but got ",
                          PhysicalPlan::PlanStr(this_plan.plan)));

    int left_re = ExecuteSelectQueryFromNode(node->left());
    int right_re = ExecuteSelectQueryFromNode(node->right());

  }
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

PhysicalPlan* SqlQuery::GenerateQueryPhysicalPlan(ExprTreeNode* node) {
  CHECK(node != nullptr, "nullptr passed to EvaluatePhysicalQueryPlans");

  if (node->physical_query_root()) {
    // Evaluate physical query root.
    return GenerateUnitPhysicalPlan(node);
  }

  const PhysicalPlan *left_plan = nullptr, *right_plan = nullptr;
  if (node->left()) {
    left_plan = GenerateQueryPhysicalPlan(node->left());
  }
  if (node->right()) {
    right_plan = GenerateQueryPhysicalPlan(node->right());
  }

  CHECK(node->type() == ExprTreeNode::OPERATOR,
        Strings::StrCat("Expect OPERATOR node, but got ",
                        ExprTreeNode::NodeTypeStr(node->type()).c_str()));

  OperatorNode* op_node = dynamic_cast<OperatorNode*>(node);
  PhysicalPlan* this_plan = node->mutable_physical_plan();
  if (op_node->OpType() == AND) {
    CHECK(left_plan != nullptr, "AND node has no left child");
    CHECK(right_plan != nullptr, "AND node has no right child");
    CHECK(left_plan->plan != PhysicalPlan::NO_PLAN,
          "AND node has no plan on left child");
    CHECK(right_plan->plan != PhysicalPlan::NO_PLAN,
          "AND node has no plan on right child");

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
    CHECK(left_plan->plan != PhysicalPlan::NO_PLAN,
          "OR node has no plan on left child");
    CHECK(right_plan->plan != PhysicalPlan::NO_PLAN,
          "OR node has no plan on right child");

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
      if (this_plan->query_ratio >= 1.0) {
        this_plan->plan = PhysicalPlan::SCAN;
        node->set_physical_query_root(true);
      } else if (left_plan->plan == PhysicalPlan::SCAN ||
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
    CHECK(left_plan->plan != PhysicalPlan::NO_PLAN,
          "NOT node has no plan on left child");

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

  return this_plan;
}

PhysicalPlan* SqlQuery::GenerateUnitPhysicalPlan(ExprTreeNode* node) {
  auto physical_plan = PreGenerateUnitPhysicalPlan(node);
  EvaluateQueryConditions(physical_plan);
  return physical_plan;
}

PhysicalPlan* SqlQuery::PreGenerateUnitPhysicalPlan(ExprTreeNode* node) {
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
    if (!result.v_bool) {
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
      PhysicalPlan* left_plan = PreGenerateUnitPhysicalPlan(node->left());
      PhysicalPlan* right_plan = PreGenerateUnitPhysicalPlan(node->right());

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
  } else if (node->type() == ExprTreeNode::TABLE_COLUMN) {
    // This is a special case. We allow bool conditions like
    // "... WHERE some_bool_field".
    ColumnNode* column_node = dynamic_cast<ColumnNode*>(node);
    CHECK(column_node->value_type() == BOOL,
          Strings::StrCat("Expect single TABLE_COLUMN node to be BOOL, but got",
                          ValueTypeStr(column_node->value_type())));

    // (TODO): Scan instead of search?
    QueryCondition condition;
    condition.column = column_node->column();
    condition.op = EQUAL;
    condition.value = NodeValue::BoolValue(true);
    this_plan->plan = PhysicalPlan::SEARCH;
    this_plan->conditions.push_back(condition);
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

#define EVALUATE_CONDITION_SEARCH_RATIO(schema_type, field_type, literal_type, cpp_type, value_type)  \
  if (field_m->type() == Schema::FieldType::schema_type) {  \
    Schema::ValueRange<cpp_type> literal_type##_range;  \
    literal_type##_range.min = field_m->min_value().limit_##literal_type();  \
    literal_type##_range.max = field_m->max_value().limit_##literal_type();        \
    for (const auto& condition : group_plan.conditions) {  \
      if (condition.op == EQUAL) {  \
        const auto& condition = group_plan.conditions.front();  \
        literal_type##_range.set_single_value(condition.value.v_##value_type);  \
      } else if (condition.op == GT) {  \
        literal_type##_range.set_left_value(condition.value.v_##value_type);  \
        literal_type##_range.left_open = true;  \
      } else if (condition.op == GE) {  \
        literal_type##_range.set_left_value(condition.value.v_##value_type);  \
        literal_type##_range.left_open = false;  \
      } else if (condition.op == LE) {  \
        literal_type##_range.set_right_value(condition.value.v_##value_type);  \
        literal_type##_range.right_open = false;  \
      } else if (condition.op == LT) {  \
        literal_type##_range.set_right_value(condition.value.v_##value_type);  \
        literal_type##_range.right_open = true;  \
      } else {  \
        LogFATAL("Unexpected Op %s", OpTypeStr(condition.op).c_str());  \
      }  \
    }  \
    search_ratio = Schema::field_type##Field::EvaluateValueRatio(literal_type##_range);  \
  }  \

void SqlQuery::EvaluateQueryConditions(PhysicalPlan* physical_plan) {
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
  [&] (const std::vector<const QueryCondition*>& group,
       DB::FieldInfoManager* field_m) {
    PhysicalPlan group_plan;
    group_plan.plan = PhysicalPlan::SCAN;
    group_plan.query_ratio = 1.0;
    const QueryCondition* equal_condition = nullptr;
    std::vector<const QueryCondition*> comparing_conditions;

    // Iterate all conditions in this group.
    //
    // Pass 1: Rule out const false conditions and conflict equal conditions.
    //std::cout << "group size = " << group.size() << std::endl;
    for (const auto& condition : group) {
      if (condition->is_const) {
        if (!condition->const_result) {
          group_plan.plan = PhysicalPlan::CONST_FALSE_SKIP;
          group_plan.query_ratio = 0.0;
          group_plan.conditions.clear();
          return group_plan;
        } else {
          group_plan.plan = PhysicalPlan::CONST_TRUE_SCAN;
          group_plan.query_ratio = 1.0;
          continue;
        }
      }

      if (condition->op == EQUAL) {
        if (equal_condition && condition->value != equal_condition->value) {
          // Two differernt equal conditions confict. This group will produce
          // const false;
          group_plan.plan = PhysicalPlan::CONST_FALSE_SKIP;
          group_plan.query_ratio = 0.0;
          group_plan.conditions.clear();
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
          group_plan.conditions.clear();
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
            group_plan.conditions.clear();
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
        const QueryCondition* downlimit = downlimit_conditions.back();
        const QueryCondition* uplimit = uplimit_conditions.front();
        if (downlimit->value > uplimit->value) {
          group_plan.plan = PhysicalPlan::CONST_FALSE_SKIP;
          group_plan.query_ratio = 0.0;
          group_plan.conditions.clear();
          return group_plan;
        } else if (downlimit->value == uplimit->value) {
          if (downlimit->op == GE && uplimit->op == LE) {
            // e.g. WHERE (field >= 3 AND field <= 3)  ----->  (field == 3)
            group_plan.plan = PhysicalPlan::SEARCH;
            group_plan.conditions.push_back(*downlimit);
            group_plan.conditions.back().op = EQUAL;
          } else {
            group_plan.plan = PhysicalPlan::CONST_FALSE_SKIP;
            group_plan.query_ratio = 0.0;
            group_plan.conditions.clear();
            return group_plan;
          }
        } else {
          group_plan.plan = PhysicalPlan::SEARCH;
          group_plan.conditions.push_back(*downlimit);
          group_plan.conditions.push_back(*uplimit);
        }
      } else if (!downlimit_conditions.empty()) {
        group_plan.plan = PhysicalPlan::SEARCH;
        group_plan.conditions.push_back(*downlimit_conditions.back());
      } else if (!uplimit_conditions.empty()) {
        group_plan.plan = PhysicalPlan::SEARCH;
        group_plan.conditions.push_back(*uplimit_conditions.front());
      }
    }

    // Evaluate query search ratio based on conditions.
    if (group_plan.conditions.empty()) {
      return group_plan;
    }
    // for (const auto& condition: group_plan.conditions) {
    //   std::cout << condition.AsString() << std::endl;
    // }

    double search_ratio = 1.0;
    EVALUATE_CONDITION_SEARCH_RATIO(INT, Int, int32, int32, int64)
    EVALUATE_CONDITION_SEARCH_RATIO(LONGINT, LongInt, int64, int64, int64)
    EVALUATE_CONDITION_SEARCH_RATIO(DOUBLE, Double, double, double, double)
    EVALUATE_CONDITION_SEARCH_RATIO(BOOL, Bool, bool, bool, bool)
    EVALUATE_CONDITION_SEARCH_RATIO(CHAR, Char, char, char, char)
    EVALUATE_CONDITION_SEARCH_RATIO(STRING, String, str, std::string, str)
    EVALUATE_CONDITION_SEARCH_RATIO(CHARARRAY, CharArray, chararray, std::string, str)

    CHECK(group_plan.plan != PhysicalPlan::NO_PLAN,
          Strings::StrCat("No plan for conditions of column group ",
                          group_plan.conditions.front().column.AsString()));

    if (search_ratio < 0) {
      group_plan.plan = PhysicalPlan::CONST_FALSE_SKIP;
      group_plan.query_ratio = 0.0;
      group_plan.conditions.clear();
    } else if (search_ratio > 1) {
      group_plan.plan = PhysicalPlan::CONST_TRUE_SCAN;
      group_plan.query_ratio = 1.0;
    } else {
      group_plan.plan = PhysicalPlan::SEARCH;
      group_plan.query_ratio = search_ratio;
    }
    return group_plan;
  };

  auto* table_m = FindTable(conditions.begin()->column.table_name);
  CHECK(table_m != nullptr,
        Strings::StrCat("Couldn't find table %s",
                        conditions.begin()->column.table_name));

  physical_plan->plan = PhysicalPlan::NO_PLAN;
  physical_plan->query_ratio = 10.0;
  for (const auto& group : condition_groups) {
    // If this column has no index, we have to scan. Otherwise analyze this
    // group conditions and find the search range.
    int field_index = (*group.begin())->column.index;
    auto field_m = table_m->FindFieldByIndex(field_index);
    CHECK(field_m != nullptr,
          Strings::StrCat("Couldn't find column \"",
                          (*group.begin())->column.column_name,
                          "\" in table \"", table_m->name(), "\""));
    if (!table_m->HasIndex({field_index})) {
      continue;
    }

    auto group_plan = analyze_condition_group(group, field_m);
    // Index search needs to be factored.
    if (!table_m->IsPrimaryIndex({field_m->index()})) {
      group_plan.query_ratio *= kIndexSearchFactor;
      group_plan.query_ratio = std::min(1.0, group_plan.query_ratio);
    }

    // Any group is direct false, the whole physical query is then skipped.
    if (group_plan.plan == PhysicalPlan::CONST_FALSE_SKIP) {
      *physical_plan = group_plan;
      break;
    }
    if (physical_plan->plan == PhysicalPlan::CONST_TRUE_SCAN) {
      // Const true should only be used if all conditions are const true.
      *physical_plan = group_plan;
    }
    if (group_plan.query_ratio < physical_plan->query_ratio) {
      *physical_plan = group_plan;
    }
    //printf("\n");
  }
}

std::string SqlQuery::error_msg() const {
  return error_msg_;
}

void SqlQuery::set_error_msg(const std::string& error_msg) {
  error_msg_ = error_msg;
}

}  // namespace Query
