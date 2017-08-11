#ifndef QUERY_EXECUTE_PLAN_H_
#define QUERY_EXECUTE_PLAN_H_

#include "Query/Common.h"
#include "Query/NodeValue.h"

namespace Query {

struct QueryCondition {
  Column column;
  OperatorType op;
  NodeValue value;
  bool is_const = false;
  bool const_result = false;  // only used when is_const = true

  std::string AsString() const {
    return Strings::StrCat(column.DebugString(), " ", OpTypeStr(op), " ",
                           value.AsString());
  }

  // INT64 can be compared with DOUBLE and CHAR. We need to unify the value type
  // with the column type.
  void CastValueType();
};

struct PhysicalPlan {
  enum Plan {
    NO_PLAN,
    CONST_FALSE_SKIP,
    CONST_TRUE_SCAN,  // This should scan
    SCAN,
    SEARCH,
    POP,  // pop result from children nodes.
  };

  enum ExecuteNode {
    NON,
    BOTH,
    LEFT,
    RIGHT,
  };

  bool ShouldScan() const { return plan == SCAN || plan == CONST_TRUE_SCAN; }
  static std::string PlanStr(Plan plan);

  void reset();

  Plan plan = NO_PLAN;
  double query_ratio = 1.0;
  ExecuteNode pop_node = NON;  // Only used when plan is POP.
  std::string table_name;

  std::vector<QueryCondition> conditions;
};

}  // namespace Query

#endif // QUERY_EXECUTE_PLAN_H_
