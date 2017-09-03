#include "Database/Table.h"
#include "Query/Common.h"
#include "Query/Iterator.h"
#include "Query/ExecutePlan.h"
#include "Query/NodeValue.h"
#include "Query/SqlQuery.h"

namespace Query {

// ***************************** Iterator *********************************** //
Iterator::Iterator(SqlQuery* query, ExprTreeNode* node) :
    query_(query),
    node_(node) {
}


// ************************ PhysicalQueryIterator *************************** //
PhysicalQueryIterator::PhysicalQueryIterator(
    SqlQuery* query, ExprTreeNode* node) :
  Iterator(query, node) {
}

void PhysicalQueryIterator::Init() {
  const auto& physical_plan = node_->physical_plan();
  const auto& table_name = physical_plan.table_name;

  // Get the table to search create table iterator.
  auto* table = query_->GetDB()->GetTable(table_name);
  CHECK(table != nullptr, "Can't get table %s", table_name.c_str());
  if (physical_plan.plan == PhysicalPlan::CONST_FALSE_SKIP) {
    end_ = true;
  }
  else if (physical_plan.plan == PhysicalPlan::SEARCH) {
    CHECK(!physical_plan.conditions.empty(), "No condition to search");
    const auto& first_condition = physical_plan.conditions.front();
    if (first_condition.op == EQUAL) {
      search_op_.field_indexes.push_back(first_condition.column.index);
      search_op_.AddKey()->AddField(
          first_condition.value.ToSchemaField(first_condition.column.type));
    } else {
      search_op_.reset();
      search_op_.field_indexes.push_back(first_condition.column.index);
      for (const auto& condition : physical_plan.conditions) {
        if (condition.op == GE) {
          search_op_.AddLeftKey()->AddField(
              condition.value.ToSchemaField(condition.column.type));
          search_op_.left_open = false;
        } else if (condition.op == GT) {
          search_op_.AddLeftKey()->AddField(
              condition.value.ToSchemaField(condition.column.type));
          search_op_.left_open = true;
        } else if (condition.op == LT) {
          search_op_.AddRightKey()->AddField(
              condition.value.ToSchemaField(condition.column.type));
          search_op_.right_open = true;
        } else if (condition.op == LE) {
          search_op_.AddRightKey()->AddField(
              condition.value.ToSchemaField(condition.column.type));
          search_op_.right_open = false;
        } else {
          LogFATAL("Unexpected op %s", OpTypeStr(condition.op).c_str());
        }
      }
    }

    table_iter_ = table->RecordIterator(&search_op_);
  }
  else if (physical_plan.plan == PhysicalPlan::SCAN ||
           physical_plan.plan == PhysicalPlan::CONST_TRUE_SCAN) {
    // Scan the table and evaluate on the node.
    table_iter_ = table->RecordIterator(&search_op_);
  } else {
    LogFATAL("Failed to init PhysicalQueryIterator - "
             "Invalid physical plan %s for this node",
             PhysicalPlan::PlanStr(physical_plan.plan).c_str());
    return;
  }

  // Add table record meta.
  query_->mutable_tuple_meta()->emplace(table_name, TableRecordMeta());
  (*query_->mutable_tuple_meta())[table_name].CreateDataRecordMeta(
                                                  table->schema());
  ready_ = true;
}

std::shared_ptr<FetchedResult::Tuple> PhysicalQueryIterator::GetNextTuple() {
  if (!ready_) {
    Init();
  }

  if (end_) {
    return nullptr;
  }

  const auto& physical_plan = node_->physical_plan();
  const auto& table_name = physical_plan.table_name;

  if (physical_plan.plan == PhysicalPlan::CONST_FALSE_SKIP) {
    return nullptr;
  } else if (physical_plan.plan == PhysicalPlan::SEARCH ||
             physical_plan.plan == PhysicalPlan::SCAN) {
    while (true) {
      auto record = table_iter_->GetNextRecord();
      if (!record) {
        return nullptr;
      }
      auto tuple = std::make_shared<FetchedResult::Tuple>();
      tuple->emplace(table_name, ResultRecord(record));
      if (node_->Evaluate(*tuple).v_bool) {
        FetchedResult::AddTupleMeta(tuple.get(), query_->mutable_tuple_meta());
        return tuple;
      }
    }
  } else if (physical_plan.plan == PhysicalPlan::CONST_TRUE_SCAN) {
    auto record = table_iter_->GetNextRecord();
    if (!record) {
      return nullptr;
    }
    auto tuple = std::make_shared<FetchedResult::Tuple>();
    tuple->emplace(table_name, ResultRecord(record));
    FetchedResult::AddTupleMeta(tuple.get(), query_->mutable_tuple_meta());
    return tuple;
  } else {
    LogFATAL("Can't iterate record - Invalid physical plan %s for this node",
             PhysicalPlan::PlanStr(physical_plan.plan).c_str());
  }

  return nullptr;
}

void PhysicalQueryIterator::reset() {
  if (table_iter_) {
    table_iter_->reset();
  }
  end_ = false;
}


// *************************** AndNodeIterator ****************************** //
AndNodeIterator::AndNodeIterator(
    SqlQuery* query, ExprTreeNode* node) :
  Iterator(query, node) {
}

void AndNodeIterator::Init() {
  const auto& this_plan = node_->physical_plan();
  CHECK(this_plan.plan == PhysicalPlan::POP,
        Strings::StrCat("Expect AND node plan to be POP, but got ",
                        PhysicalPlan::PlanStr(this_plan.plan)));

  if (this_plan.pop_node == PhysicalPlan::LEFT) {
    pop_node_ = node_->left();
    other_node_ = node_->right();
  } else if (this_plan.pop_node == PhysicalPlan::RIGHT) {
    pop_node_ = node_->right();
    other_node_ = node_->left();
  } else {
    LogFATAL("No pop node to fetch result for AND node");
  }

  ready_ = true;
}

std::shared_ptr<FetchedResult::Tuple> AndNodeIterator::GetNextTuple() {
  if (!ready_) {
    Init();
  }

  if (end_) {
    return nullptr;
  }

  auto* pop_iter = pop_node_->GetIterator();
  while (true) {
    auto tuple = pop_iter->GetNextTuple();
    if (!tuple) {
      end_ = true;
      return nullptr;
    }

    if (other_node_->physical_plan().plan == PhysicalPlan::CONST_TRUE_SCAN) {
      return tuple;
    } else {
      if (other_node_->Evaluate(*tuple).v_bool) {
        return tuple;
      }
    }
  }
}

void AndNodeIterator::reset() {
  if (pop_node_) {
    pop_node_->GetIterator()->reset();
  }
  end_ = false;
}


// *************************** OrNodeIterator ****************************** //
OrNodeIterator::OrNodeIterator(
    SqlQuery* query, ExprTreeNode* node) :
  Iterator(query, node) {
}

void OrNodeIterator::Init() {
  const auto& this_plan = node_->physical_plan();
  CHECK(this_plan.plan == PhysicalPlan::POP,
        Strings::StrCat("Expect AND node plan to be POP, but got ",
                        PhysicalPlan::PlanStr(this_plan.plan)));

  result_.tuple_meta = query_->mutable_tuple_meta();

  FetchedResult left_result, right_result;
  left_result.tuple_meta = query_->mutable_tuple_meta();
  right_result.tuple_meta = query_->mutable_tuple_meta();

  auto* node_iter = node_->left()->GetIterator();
  while (true) {
    auto tuple = node_iter->GetNextTuple();
    if (!tuple) {
      break;
    }
    left_result.AddTuple(std::move(*tuple));
  }

  node_iter = node_->right()->GetIterator();
  while (true) {
    auto tuple = node_iter->GetNextTuple();
    if (!tuple) {
      break;
    }
    right_result.AddTuple(std::move(*tuple));
  }

  const std::string& table_name = this_plan.table_name;
  auto* table_m = query_->FindTable(table_name);
  CHECK(table_m != nullptr,
        Strings::StrCat("Couldn't find table ", table_name));
  result_.MergeSortResultsRemoveDup(
      left_result, right_result, table_name, table_m->PrimaryIndex());

  ready_ = true;
}

std::shared_ptr<FetchedResult::Tuple> OrNodeIterator::GetNextTuple() {
  if (!ready_) {
    Init();
  }

  if (end_) {
    return nullptr;
  }

  if (tuple_index_ >= result_.tuples.size()) {
    end_ = true;
    return nullptr;
  }

  auto tuple = std::make_shared<FetchedResult::Tuple>();
  *tuple = result_.tuples.at(tuple_index_++);
  return tuple;
}

void OrNodeIterator::reset() {
  tuple_index_ = 0;
  end_ = false;
}

}  // namespace Query
