#include "Strings/Split.h"
#include "Strings/Utils.h"

#include "Database/Database.h"
#include "Database/Operation.h"
#include "Query/SqlQuery.h"
#include "Storage/Record.h"

namespace Query {

namespace {
const double kIndexSearchFactor = 2;
}

SqlQuery::SqlQuery(DB::Database* db) :
    db_(db),
    catalog_m_(db_->mutable_catalog_manager()) {}

void SqlQuery::reset() {
  expr_node_.reset();
  tables_.clear();
  columns_set_.clear();
  columns_unknown_table_.clear();
  star_columns_.clear();
  order_by_columns_.clear();
  group_by_columns_.clear();
  columns_.clear();
  columns_num_ = 0;
  aggregated_columns_num_ = 0;

  results_.reset();
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
  return AddColumn(name, NO_AGGREGATION);
}

bool SqlQuery::AddColumn(const std::string& name,
                         AggregationType aggregation_type) {
  ColumnRequest column_request;
  if (!ParseTableColumn(name, &column_request.column)) {
    return false;
  }

  column_request.aggregation_type = aggregation_type;
  if (aggregation_type != NO_AGGREGATION) {
    aggregated_columns_num_++;
  }
  column_request.request_pos = columns_num_++;

  if (column_request.column.table_name.empty()) {
    columns_unknown_table_.insert(column_request);
    return true;
  }

  if (column_request.column.column_name == "*") {
    star_columns_.insert(column_request);
    return true;
  }

  columns_set_.insert(column_request);
  return true;
}

bool SqlQuery::AddOrderByColumn(const std::string& name) {
  return AddOrderByColumn(name, NO_AGGREGATION);
}

bool SqlQuery::AddOrderByColumn(const std::string& name,
                                AggregationType aggregation_type) {
  ColumnRequest new_column_request;
  if (!ParseTableColumn(name, &new_column_request.column)) {
    return false;
  }

  new_column_request.aggregation_type = aggregation_type;

  if (new_column_request.column.column_name == "*") {
    error_msg_ = "Can't use * column in ORDER BY column list";
    return false;
  }

  for (const auto& column_request : order_by_columns_) {
    if (new_column_request == column_request) {
      return true;
    }
  }

  order_by_columns_.push_back(new_column_request);
  return true;
}

bool SqlQuery::AddGroupByColumn(const std::string& name) {
  Column new_column;
  if (!ParseTableColumn(name, &new_column)) {
    return false;
  }

  if (new_column.column_name == "*") {
    error_msg_ = "Can't use * column in GROUP BY column list";
    return false;
  }

  for (const auto& column : group_by_columns_) {
    if (new_column == column) {
      return true;
    }
  }

  group_by_columns_.push_back(new_column);
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

const ColumnRequest* SqlQuery::FindColumnRequest(const Column& column) const {
  for (const auto& column_request : columns_) {
    if (column_request.column == column) {
      return &column_request;
    }
  }
  return nullptr;
}

const ColumnRequest* SqlQuery::FindColumnRequest(
    const Column& column, AggregationType aggregation_type) const {
  for (const auto& column_request : columns_) {
    std::cout << column_request.AsString(true) << std::endl;
    if (column_request.column == column &&
        column_request.aggregation_type == aggregation_type) {
      return &column_request;
    }
  }
  return nullptr;
}

bool SqlQuery::FinalizeParsing() {
  // Apply default table.
  std::string default_table = DefaultTable();
  if (default_table.empty() &&
      (!columns_unknown_table_.empty() || !order_by_columns_.empty())) {
    error_msg_ = "Default table not found.";
    return false;
  }

  for (const auto& column_request : columns_unknown_table_) {
    ColumnRequest cr_copy = column_request;
    cr_copy.column.table_name = default_table;
    if (cr_copy.column.column_name == "*") {
      star_columns_.insert(cr_copy);
    } else {
      columns_set_.insert(cr_copy);
    }
  }
  columns_unknown_table_.clear();

  for (auto& column_request : order_by_columns_) {
    if (column_request.column.table_name.empty()) {
      if (default_table.empty()) {
        error_msg_ = "Default table not found for order by column.";
        return false;
      }
      column_request.column.table_name = default_table;
    }
  }

  for (auto& column : group_by_columns_) {
    if (column.table_name.empty()) {
      if (default_table.empty()) {
        error_msg_ = "Default table not found for order by column.";
        return false;
      }
      column.table_name = default_table;
    }
  }

  // Expand * columns.
  for (const auto& column_request : star_columns_) {
    if (column_request.aggregation_type == AggregationType::COUNT) {
      continue;
    }

    const auto& table_name = column_request.column.table_name;
    auto table_m = FindTable(table_name);
    CHECK(table_m != nullptr, Strings::StrCat("Can't find table ", table_name));
    for (uint32 i = 0; i < table_m->NumFields(); i++) {
      auto field_m = table_m->FindFieldByIndex(i);
      CHECK(field_m != nullptr,
            Strings::StrCat("Can't find field ", std::to_string(i),
                            " in table ", table_name));
      ColumnRequest cr_copy = column_request;
      cr_copy.column.table_name = table_name;
      cr_copy.column.column_name = field_m->name();
      cr_copy.request_pos = column_request.request_pos;
      cr_copy.sub_request_pos = i;
      columns_set_.insert(cr_copy);
    }
  }
  star_columns_.clear();

  // Sort columns by request order.
  auto comparator = [&] (const ColumnRequest& c1,
                         const ColumnRequest& c2) {
    if (c1.request_pos != c2.request_pos) {
      return c1.request_pos < c2.request_pos;
    } else if (c1.sub_request_pos != c2.sub_request_pos) {
      return c1.sub_request_pos < c2.sub_request_pos;
    }
    return false;
  };

  for (const auto& column_request : columns_set_) {
    columns_.push_back(column_request);
  }
  columns_set_.clear();
  std::sort(columns_.begin(), columns_.end(), comparator);

  // Check all tables and columns are valid.
  for (const auto& table_name : tables_) {
    if (!TableIsValid(table_name)) {
      return false;
    }
  }

  uint32 non_aggregate_columns = 0;
  for (auto& column_request : columns_) {
    const auto& table_name = column_request.column.table_name;
    if (tables_.find(table_name) == tables_.end()) {
      error_msg_ = Strings::StrCat("Table %s ", table_name.c_str(),
                                   " not in table selection list");
      return false;
    }

    auto field_m = FindTableColumn(column_request.column);
    if (field_m == nullptr) {
      return false;
    }
    // Assign column field index and type.
    column_request.column.index = field_m->index();
    column_request.column.type = field_m->type();

    if (column_request.aggregation_type == NO_AGGREGATION) {
      non_aggregate_columns++;
    } else {
      if (!IsFieldAggregationValid(column_request.aggregation_type,
                                   field_m->type())) {
        error_msg_ = Strings::StrCat(
            "Can't use ", AggregationStr(column_request.aggregation_type),
            " on field ", column_request.column.DebugString(),
            " with type ", Schema::FieldTypeStr(column_request.column.type));
        return false;
      }
    }
  }

  // Check group column must be in select columns list, with no aggregation.
  for (const auto& column : group_by_columns_) {
    if (FindColumnRequest(column, NO_AGGREGATION) == nullptr) {
      error_msg_ = Strings::StrCat("Can't find group_by column ",
                                   column.DebugString(), " in select list");
      return false;
    }
  }

  // If group_by_columns is empty, the result will be automatically grouped
  // by all non_aggregate_columns in the select list.
  if (!group_by_columns_.empty() &&
      non_aggregate_columns != group_by_columns_.size()) {
    error_msg_ =
      "GROUP BY list doesn't match all non-aggregate columns in select list ";
    return false;
  }

  for (auto& column_request : order_by_columns_) {
    auto field_m = FindTableColumn(column_request.column);
    if (field_m == nullptr) {
      return false;
    }
    // Assign column field index and type.
    column_request.column.index = field_m->index();
    column_request.column.type = field_m->type();
  }

  // Check expression tree root returns bool.
  if (!expr_node_) {
    expr_node_.reset(new ConstValueNode(NodeValue::BoolValue(true)));
  }
  if (!expr_node_->valid()) {
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
  CreateIteratorsRecursive(expr_node_.get());
  return expr_node_->physical_plan();
}

int SqlQuery::ExecuteSelectQuery() {
  PrepareQueryPlan();

  // Static query.
  // ExecuteSelectQueryFromNode(expr_node_.get());
  // results_ = std::move(*expr_node_->mutable_results());

  // Use iterator in fly.
  results_.tuple_meta = &tuple_meta_;
  auto* iter = expr_node_->GetIterator();
  while (true) {
    auto tuple = iter->GetNextTuple();
    if (!tuple) {
      break;
    }
    results_.AddTuple(std::move(*tuple));
  }

  if (aggregated_columns_num_ > 0) {
    AggregateResults();
  }

  if (!order_by_columns_.empty()) {
    std::vector<Column> columns;
    for (const auto& column_request : order_by_columns_) {
      columns.push_back(column_request.column);
    }
    results_.SortByColumns(columns);
  }

  return results_.NumTuples();
}

int SqlQuery::ExecuteSelectQueryFromNode(ExprTreeNode* node) {
  const auto& this_plan = node->physical_plan();
  const std::string& table_name = this_plan.table_name;
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
    ExprTreeNode *pop_node = nullptr, *other_node = nullptr;
    if (this_plan.pop_node == PhysicalPlan::LEFT) {
      pop_node = node->left();
      other_node = node->right();
    } else if (this_plan.pop_node == PhysicalPlan::RIGHT) {
      pop_node = node->right();
      other_node = node->left();
    } else {
      LogFATAL("No pop node to fetch result for AND node");
    }
    ExecuteSelectQueryFromNode(pop_node);
    if (other_node->physical_plan().plan == PhysicalPlan::CONST_TRUE_SCAN) {
      // The other node returns const true. No need to verify. Pop all fetched
      // tuples up.
      *node->mutable_results() = std::move(*pop_node->mutable_results());
    } else {
      // Evaluate fetched tuples on the other node.
      node->mutable_results()->tuple_meta = &tuple_meta_;
      for (auto& tuple : pop_node->mutable_results()->tuples) {
        NodeValue result = other_node->Evaluate(tuple);
        if (result.v_bool) {
          node->mutable_results()->AddTuple(std::move(tuple));
        }
      }
    }
  } else if (op_node->OpType() == OR) {
    CHECK(this_plan.plan == PhysicalPlan::POP,
          Strings::StrCat("Expect OR node plan to be POP, but got ",
                          PhysicalPlan::PlanStr(this_plan.plan)));

    int left_re = ExecuteSelectQueryFromNode(node->left());
    int right_re = ExecuteSelectQueryFromNode(node->right());
    if (left_re <= 0 && right_re <= 0) {
      // Is it possible?
      return 0;
    }

    // TODO: This only applies to single table query. Sort and remove
    // duplication based on the primary key.
    node->mutable_results()->tuple_meta = &tuple_meta_;
    auto table_m = FindTable(table_name);
    CHECK(table_m != nullptr,
          Strings::StrCat("Couldn't find table ", table_name));

    node->mutable_results()->MergeSortResultsRemoveDup(
        *node->left()->mutable_results(), *node->right()->mutable_results(),
        table_name, table_m->PrimaryIndex());
  } else {
    LogFATAL("Unexpected OP type for OPERATOR node %s",
             OpTypeStr(op_node->OpType()).c_str());
  }

  return node->results().NumTuples();
}

int SqlQuery::Do_ExecutePhysicalQuery(ExprTreeNode* node) {
  const auto& physical_plan = node->physical_plan();
  if (physical_plan.plan == PhysicalPlan::CONST_FALSE_SKIP) {
    return 0;
  }

  const auto& table_name = physical_plan.table_name;
  if (physical_plan.plan == PhysicalPlan::SEARCH) {
    printf("search\n");
    CHECK(!physical_plan.conditions.empty(), "No condition to search");
    const auto& first_condition = physical_plan.conditions.front();
    if (first_condition.op == EQUAL) {
      DB::SearchOp search_op;
      search_op.field_indexes.push_back(first_condition.column.index);
      search_op.AddKey()->AddField(
          first_condition.value.ToSchemaField(first_condition.column.type));

      // Get table and search.
      auto* table = db_->GetTable(table_name);
      CHECK(table != nullptr, "Can't get table %s", table_name.c_str());

      // Add table record meta.
      tuple_meta_.emplace(table_name, TableRecordMeta());
      tuple_meta_[table_name].CreateDataRecordMeta(table->schema());
      node->mutable_results()->tuple_meta = &tuple_meta_;

      std::vector<std::shared_ptr<Storage::RecordBase>> records;
      table->SearchRecords(search_op, &records);
      for (const auto& record : records) {
        auto tuple = FetchedResult::Tuple();
        tuple.emplace(table_name, ResultRecord(record));
        if (node->Evaluate(tuple).v_bool) {
          node->mutable_results()->AddTuple(std::move(tuple));
        }
      }
    } else {
      DB::SearchOp range_search_op;
      range_search_op.reset();
      range_search_op.field_indexes.push_back(first_condition.column.index);
      for (const auto& condition : physical_plan.conditions) {
        if (condition.op == GE) {
          range_search_op.AddLeftKey()->AddField(
              condition.value.ToSchemaField(condition.column.type));
          range_search_op.left_open = false;
        } else if (condition.op == GT) {
          range_search_op.AddLeftKey()->AddField(
              condition.value.ToSchemaField(condition.column.type));
          range_search_op.left_open = true;
        } else if (condition.op == LT) {
          range_search_op.AddRightKey()->AddField(
              condition.value.ToSchemaField(condition.column.type));
          range_search_op.right_open = true;
        } else if (condition.op == LE) {
          range_search_op.AddRightKey()->AddField(
              condition.value.ToSchemaField(condition.column.type));
          range_search_op.right_open = false;
        } else {
          LogFATAL("Unexpected op %s", OpTypeStr(condition.op).c_str());
        }
      }

      // Get table and search.
      auto* table = db_->GetTable(table_name);
      CHECK(table != nullptr, "Can't get table %s", table_name.c_str());

      // Add table record meta.
      tuple_meta_.emplace(table_name, TableRecordMeta());
      tuple_meta_[table_name].CreateDataRecordMeta(table->schema());
      node->mutable_results()->tuple_meta = &tuple_meta_;

      std::vector<std::shared_ptr<Storage::RecordBase>> records;
      table->SearchRecords(range_search_op, &records);
      for (const auto& record : records) {
        auto tuple = FetchedResult::Tuple();
        tuple.emplace(table_name, ResultRecord(record));
        if (node->Evaluate(tuple).v_bool) {
          node->mutable_results()->AddTuple(std::move(tuple));
        }
      }
    }
  } else if (physical_plan.plan == PhysicalPlan::SCAN) {
    printf("scan\n");
    // Scan the table and evaluate on the node.
    auto* table = db_->GetTable(table_name);
    CHECK(table != nullptr, "Can't get table %s", table_name.c_str());

    // Add table record meta.
    tuple_meta_.emplace(table_name, TableRecordMeta());
    tuple_meta_[table_name].CreateDataRecordMeta(table->schema());
    node->mutable_results()->tuple_meta = &tuple_meta_;

    std::vector<std::shared_ptr<Storage::RecordBase>> records;
    table->ScanRecords(&records);
    for (const auto& record : records) {
      auto tuple = FetchedResult::Tuple();
      tuple.emplace(table_name, ResultRecord(record));
      auto match = node->Evaluate(tuple);
      if (!match.v_bool) {
        continue;
      }
      node->mutable_results()->AddTuple(std::move(tuple));
    }
  } else if (physical_plan.plan == PhysicalPlan::CONST_TRUE_SCAN) {
    printf("const true scan\n");
    // Scan the table.
    auto* table = db_->GetTable(table_name);
    CHECK(table != nullptr, "Can't get table %s", table_name.c_str());

    // Add table record meta.
    tuple_meta_.emplace(table_name, TableRecordMeta());
    tuple_meta_[table_name].CreateDataRecordMeta(table->schema());
    node->mutable_results()->tuple_meta = &tuple_meta_;

    std::vector<std::shared_ptr<Storage::RecordBase>> records;
    table->ScanRecords(&records);
    for (const auto& record : records) {
      auto tuple = FetchedResult::Tuple();
      tuple.emplace(table_name, ResultRecord(record));
      node->mutable_results()->AddTuple(std::move(tuple));
    }
  }
  return node->mutable_results()->tuples.size();
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

void SqlQuery::CreateIteratorsRecursive(ExprTreeNode* node) {
  if (node->physical_query_root()) {
    node->SetIterator(new PhysicalQueryIterator(this, node));
    return;
  }

  CHECK(node->type() == ExprTreeNode::OPERATOR,
        Strings::StrCat("Expect OPERATOR node, but got ",
                        ExprTreeNode::NodeTypeStr(node->type()).c_str()));

  OperatorNode* op_node = dynamic_cast<OperatorNode*>(node);
  if (op_node->OpType() == AND) {
    node->SetIterator(new AndNodeIterator(this, node));
  } else if (op_node->OpType() == OR) {
    node->SetIterator(new OrNodeIterator(this, node));
  } else {
    LogFATAL("Unexpected operator type %s",
             OpTypeStr(op_node->OpType()).c_str());
  }

  if (node->left()) {
    CreateIteratorsRecursive(node->left());
  }
  if (node->right()) {
    CreateIteratorsRecursive(node->right());
  }
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

    // Set table name.
    if (!left_plan->table_name.empty()) {
      this_plan->table_name = left_plan->table_name;
    }
    if (this_plan->table_name.empty() && !right_plan->table_name.empty()) {
      this_plan->table_name = right_plan->table_name;
    }

    // If AND node has a child returning a direct False, this AND node also
    // returns direct False.
    if (left_plan->plan == PhysicalPlan::CONST_FALSE_SKIP ||
        right_plan->plan == PhysicalPlan::CONST_FALSE_SKIP) {
      this_plan->plan = PhysicalPlan::CONST_FALSE_SKIP;
      this_plan->query_ratio = 0.0;
      node->set_physical_query_root(true);
    } else if (left_plan->plan == PhysicalPlan::CONST_TRUE_SCAN &&
               right_plan->plan == PhysicalPlan::CONST_TRUE_SCAN) {
      this_plan->plan = PhysicalPlan::CONST_TRUE_SCAN;
      node->set_physical_query_root(true);
      this_plan->query_ratio = 1.0;
    } else {
      // printf("left = %f\n", left_plan->query_ratio);
      // printf("right = %f\n", right_plan->query_ratio);
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
        this_plan->query_ratio = 1.0;
        node->set_physical_query_root(true);
      }
    }
  } else if (op_node->OpType() == OR) {
    CHECK(left_plan != nullptr, "OR node has no left child");
    CHECK(right_plan != nullptr, "OR node has no right child");
    CHECK(left_plan->plan != PhysicalPlan::NO_PLAN,
          "OR node has no plan on left child");
    CHECK(right_plan->plan != PhysicalPlan::NO_PLAN,
          "OR node has no plan on right child");

    // Set table name.
    if (!left_plan->table_name.empty()) {
      this_plan->table_name = left_plan->table_name;
    }
    if (this_plan->table_name.empty() && !right_plan->table_name.empty()) {
      this_plan->table_name = right_plan->table_name;
    }

    if (left_plan->plan == PhysicalPlan::CONST_FALSE_SKIP &&
        right_plan->plan == PhysicalPlan::CONST_FALSE_SKIP) {
      this_plan->plan = PhysicalPlan::CONST_FALSE_SKIP;
      this_plan->query_ratio = 0.0;
      node->set_physical_query_root(true);
    } else if (left_plan->plan == PhysicalPlan::CONST_TRUE_SCAN ||
               right_plan->plan == PhysicalPlan::CONST_TRUE_SCAN) {
      this_plan->plan = PhysicalPlan::CONST_TRUE_SCAN;
      this_plan->query_ratio = 1.0;
      node->set_physical_query_root(true);
    } else {
      this_plan->query_ratio = left_plan->query_ratio + right_plan->query_ratio;
      // printf("left = %f\n", left_plan->query_ratio);
      // printf("right = %f\n", right_plan->query_ratio);
      this_plan->query_ratio = std::min(1.0, this_plan->query_ratio);
      if (this_plan->query_ratio >= 1.0) {
        this_plan->plan = PhysicalPlan::SCAN;
        node->set_physical_query_root(true);
      } else if (left_plan->plan == PhysicalPlan::SCAN ||
                 right_plan->plan == PhysicalPlan::SCAN) {
        this_plan->plan = PhysicalPlan::SCAN;
        this_plan->query_ratio = 1.0;
        node->set_physical_query_root(true);
      } else {
        this_plan->plan = PhysicalPlan::POP;
        this_plan->pop_node = PhysicalPlan::BOTH;
      }
    }
  } else if (op_node->OpType() == NOT) {
    CHECK(left_plan != nullptr, "NOT node has no child expression");
    CHECK(left_plan->plan != PhysicalPlan::NO_PLAN,
          "NOT node has no plan on left child");

    this_plan->table_name = left_plan->table_name;

    if (left_plan->plan == PhysicalPlan::CONST_FALSE_SKIP) {
      this_plan->plan = PhysicalPlan::CONST_TRUE_SCAN;
      this_plan->query_ratio = 1.0;
      node->set_physical_query_root(true);
    } else if (left_plan->plan == PhysicalPlan::CONST_TRUE_SCAN) {
      this_plan->plan = PhysicalPlan::CONST_FALSE_SKIP;
      this_plan->query_ratio = 0;
      node->set_physical_query_root(true);
    } else {
      this_plan->plan = PhysicalPlan::SCAN;
      this_plan->query_ratio = 1.0;
      node->set_physical_query_root(true);
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

  if (physical_plan->table_name.empty()) {
    physical_plan->table_name = DefaultTable();
    CHECK(!physical_plan->table_name.empty(),
          "Don't know what table to search");
  }
  return physical_plan;
}

PhysicalPlan* SqlQuery::PreGenerateUnitPhysicalPlan(ExprTreeNode* node) {
  CHECK(node->value_type() == ValueType::BOOL,
        Strings::StrCat("Expect physical query expr returns BOOL, but got %s",
                        ValueTypeStr(node->value_type()).c_str()));

  auto* this_plan = node->mutable_physical_plan();
  if (IsConstExpression(node)) {
    NodeValue result = node->Evaluate(FetchedResult::Tuple());
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
    condition.CastValueType();
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
                          group_plan.conditions.front().column.DebugString()));

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

  const auto& table_name = conditions.begin()->column.table_name;
  auto* table_m = FindTable(table_name);
  CHECK(table_m != nullptr,
        Strings::StrCat("Couldn't find table %s",
                        conditions.begin()->column.table_name));

  physical_plan->plan = PhysicalPlan::NO_PLAN;
  physical_plan->query_ratio = 10.0;
  for (const auto& group : condition_groups) {
    // If this column has no index, we have to scan. Otherwise analyze this
    // group conditions and find the search range.
    uint32 field_index = (*group.begin())->column.index;
    auto field_m = table_m->FindFieldByIndex(field_index);
    CHECK(field_m != nullptr,
          Strings::StrCat("Couldn't find column \"",
                          (*group.begin())->column.column_name,
                          "\" in table \"", table_m->name(), "\""));
    if (!table_m->HasIndex({field_index})) {
      continue;
    }

    auto group_plan = analyze_condition_group(group, field_m);
    // Any group is direct false, the whole physical query is then skipped.
    if (group_plan.plan == PhysicalPlan::CONST_FALSE_SKIP) {
      *physical_plan = group_plan;
      break;
    }

    // Index search ratio needs to be factored.
    if (!table_m->IsPrimaryIndex({field_m->index()})) {
      group_plan.query_ratio *= kIndexSearchFactor;
      group_plan.query_ratio = std::min(1.0, group_plan.query_ratio);
      if (group_plan.query_ratio >= 1.0 &&
          group_plan.plan == PhysicalPlan::SEARCH) {
        group_plan.plan = PhysicalPlan::SCAN;
      }
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

  if (physical_plan->plan == PhysicalPlan::NO_PLAN) {
    physical_plan->plan = PhysicalPlan::SCAN;
    physical_plan->query_ratio = 1.0;
  }
  physical_plan->table_name = table_name;
}

void SqlQuery::AggregateResults() {
  std::vector<ColumnRequest*> aggregation_columns;
  std::vector<Column> non_aggregation_columns;
  std::map<std::string, uint32> extra_index;
  for (auto& column_request : columns_) {
    if (column_request.aggregation_type != NO_AGGREGATION) { 
      // Give the aggregation column extra index, appending all columns of the
      // table.
      const auto& table_name = column_request.column.table_name;
      auto* table_m = FindTable(column_request.column.table_name);
      CHECK(table_m != nullptr, "Couldn't find table %s", table_name.c_str());
      if (extra_index[table_name] == 0) {
        extra_index[table_name] = table_m->NumFields();
      }
      column_request.column.index = extra_index.at(table_name);
      if (column_request.aggregation_type != COUNT) {
        column_request.column.type = Schema::FieldType::INT;
      }

      // Update table record meta.
      auto it = results_.tuple_meta->find(table_name);
      CHECK(it != results_.tuple_meta->end(),
            "Couldn't find table record meta for table %s", table_name.c_str());
      it->second.fetched_fields.push_back(DB::TableField());
      it->second.fetched_fields.back().set_index(extra_index.at(table_name));
      it->second.fetched_fields.back().set_type(column_request.column.type);

      extra_index[table_name]++;

      aggregation_columns.push_back(&column_request);
    } else {
      non_aggregation_columns.push_back(column_request.column);
    }
  }

  FetchedResult result;
  result.tuple_meta = &tuple_meta_;
  results_.SortByColumns(non_aggregation_columns);

  // Help function to calculate AVG() columns for a tuple.
  auto cal_avg = [&] (FetchedResult::Tuple* agg_tuple, uint32 group_size) {
    if (!agg_tuple) {
      return;
    }
    for (const auto& aggregation_column : aggregation_columns) {
      if (aggregation_column->aggregation_type != AVG) {
        continue;
      }
      const auto& table_name = aggregation_column->column.table_name;
      auto it = agg_tuple->find(table_name);
      CHECK(it != agg_tuple->end(),
            "Couldn't find record of table %s from tuple",
            table_name.c_str());
      auto& aggregated_record = it->second;
      auto* aggregated_field =
          aggregated_record.MutableField(aggregation_column->column.index);
      CalculateAvg(aggregated_field, group_size);
    }
  };

  FetchedResult::Tuple* agg_tuple = nullptr;
  uint32 group_size = 0;
  for (auto& tuple : results_.tuples) {
    if (!agg_tuple ||
        (agg_tuple && FetchedResult::CompareBasedOnColumns(
                          tuple, *agg_tuple, non_aggregation_columns) != 0)) {
      // Calculate AVG() columns for last group.
      cal_avg(agg_tuple, group_size);

      // New group.
      result.AddTuple(std::move(tuple));
      agg_tuple = &(result.tuples.back());

      // Append aggregated column fields to the record.
      for (const auto& aggregation_column : aggregation_columns) {
        const auto& table_name = aggregation_column->column.table_name;
        auto it = agg_tuple->find(table_name);
        CHECK(it != agg_tuple->end(),
              "Couldn't find record of table %s from tuple",table_name.c_str());
        auto& table_record = it->second;

        if (aggregation_column->aggregation_type != COUNT) {
          auto* field_m = FindTableColumn(aggregation_column->column);
          const auto* original_field = table_record.GetField(field_m->index());
          CHECK(original_field != nullptr,
                "Couldn't get original field or aggregation column %s",
                aggregation_column->AsString(true).c_str());
          table_record.AddField(original_field->Copy());
        } else {
          table_record.AddField(new Schema::IntField(1));
        }
      }
      group_size = 1;
    } else {
      // Do aggregation.
      for (const auto& aggregation_column : aggregation_columns) {
        // Find aggregated field.
        const auto& table_name = aggregation_column->column.table_name;
        auto it = agg_tuple->find(table_name);
        CHECK(it != agg_tuple->end(),
              "Couldn't find record of table %s from tuple",table_name.c_str());
        auto& aggregated_record = it->second;
        auto* aggregated_field =
            aggregated_record.MutableField(aggregation_column->column.index);
        CHECK(aggregated_field != nullptr,
              "Couldn't get aggregated column %s",
              aggregation_column->AsString(true).c_str());

        // Find original field.
        const Schema::Field* original_field = nullptr;
        if (aggregation_column->aggregation_type != COUNT) {
          auto it2 = tuple.find(table_name);
          CHECK(it2 != tuple.end(),
                "Couldn't find record of table %s from tuple",
                table_name.c_str());
          auto& table_record = it2->second;
          auto* field_m = FindTableColumn(aggregation_column->column);
          original_field = table_record.GetField(field_m->index());
          CHECK(original_field != nullptr,
                "Couldn't get original field of aggregation column %s",
                aggregation_column->AsString(true).c_str());
        }

        AggregateField(aggregation_column->aggregation_type,
                       aggregated_field, original_field);
      }
      group_size++;
    }
  }
  // Calculate AVG() columns for the last group.
  cal_avg(agg_tuple, group_size);

  results_ = std::move(result);
}
 
void SqlQuery::PrintResults() {
  if (results_.tuples.empty()) {
    printf("Empty set\n");
    return;
  }

  // Get the print size for each column.
  for (auto& tuple : results_.tuples) {
    for (auto& column_request : columns_) {
      const auto& it = tuple.find(column_request.column.table_name);
      CHECK(it != tuple.end(), "Tuple doesn't have the record of table %s",
                               column_request.column.table_name.c_str());

      const auto& result_record = it->second;
      const auto* field = result_record.GetField(column_request.column.index);
      uint32 print_size = field->AsString().length();
      if (print_size + 2 > column_request.print_width) {
        column_request.print_width = print_size + 2;
      }
    }
  }

  bool use_table_prefix = false;
  if (tables_.size() > 1) {
    use_table_prefix = true;
  }
  for (auto& column_request : columns_) {
    column_request.print_name = column_request.AsString(use_table_prefix);
    if (column_request.print_name.length() + 2 > column_request.print_width) {
      column_request.print_width = column_request.print_name.length() + 2;
    }
  }

  // Print the header line.
  auto print_chars = [&] (char c, int32 num) {
    for (int32 i = 0; i < num; i++) {
      printf("%c", c);
    }
  };
  for (const auto& column_request : columns_) {
    printf("+");
    print_chars('-', column_request.print_width);
  }
  printf("+\n");
  for (const auto& column_request : columns_) {
    printf("| ");
    printf("%s", column_request.print_name.c_str());
    int remain_space = column_request.print_width -
                          (1 + column_request.print_name.length());
    print_chars(' ', remain_space);
  }
  printf("|\n");
  for (const auto& column_request : columns_) {
    printf("+");
    for (uint32 i = 0; i < column_request.print_width; i++) {
      printf("-");
    }
  }
  printf("+\n");

  // Do print.
  for (auto& tuple : results_.tuples) {
    for (const auto& column_request : columns_) {
      const auto& it = tuple.find(column_request.column.table_name);
      CHECK(it != tuple.end(), "Tuple doesn't have the record of table %s",
                               column_request.column.table_name.c_str());

      const auto& result_record = it->second;
      const auto* field = result_record.GetField(column_request.column.index);

      printf("| ");
      printf("%s", field->AsString().c_str());
      int remain_space = column_request.print_width -
                          (1 + field->AsString().length());
      print_chars(' ', remain_space);
    }
    printf("|\n");
  }

  // Ending.
  for (const auto& column_request : columns_) {
    printf("+");
    for (uint32 i = 0; i < column_request.print_width; i++) {
      printf("-");
    }
  }
  printf("+\n");
  printf("%d rows in set\n", (int)results_.tuples.size());
}

FetchedResult::TupleMeta* SqlQuery::mutable_tuple_meta() {
  return &tuple_meta_;
}

std::string SqlQuery::error_msg() const {
  return error_msg_;
}

void SqlQuery::set_error_msg(const std::string& error_msg) {
  error_msg_ = error_msg;
}

std::string ColumnRequest::AsString(bool use_table_prefix) const {
  std::string result = column.AsString(use_table_prefix);
  if (aggregation_type != NO_AGGREGATION) {
    result = Strings::StrCat(AggregationStr(aggregation_type),
                             "(", result, ")");
  }
  return result;
}

}  // namespace Query
