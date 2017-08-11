#include <math.h>

#include "Query/ExecutePlan.h"

namespace Query {

// *************************** QueryCondition ******************************* //
void QueryCondition::CastValueType() {
  auto type = FromSchemaType(column.type);
  if (type == INT64) {
    if (value.type == DOUBLE) {
      // Careful, don't cast directly. Needs to check the op type.
      if (op == EQUAL) {
        if (floor(value.v_double) != value.v_double) {
          is_const = true;
          const_result = false;
        } else {
          value.v_int64 = static_cast<int64>(value.v_double);
        }
      } else if (op == NONEQUAL) {
        if (floor(value.v_double) != value.v_double) {
          is_const = true;
          const_result = true;
        } else {
          value.v_int64 = static_cast<int64>(value.v_double);
        }
      } else if (op == LT) {
        if (floor(value.v_double) == value.v_double) {
          value.v_int64 = static_cast<int64>(value.v_double) - 1;
        } else {
          value.v_int64 = static_cast<int64>(value.v_double);
        }
        op = LE;
      } else if (op == LE) {
        value.v_int64 = static_cast<int64>(value.v_double);
      } else if (op == GT) {
        value.v_int64 = static_cast<int64>(value.v_double) + 1;
        op = GE;
      } else if (op == GE) {
        if (floor(value.v_double) == value.v_double) {
          value.v_int64 = static_cast<int64>(value.v_double);
        } else {
          value.v_int64 = static_cast<int64>(value.v_double) + 1;
        }
      } 
    } else if (value.type == CHAR) {
      value.v_int64 = static_cast<int64>(value.v_char);
    } else if (value.type == INT64) {
      // Optmize for < and >, prefer using <= and >= in B+ tree search.
      if (op == LT) {
        value.v_int64--;
        op = LE;
      } else if (op == GT) {
        value.v_int64++;
        op = GE;
      }
    } else {
      LogFATAL("Unexpected value conversion from %s to INT64",
               ValueTypeStr(value.type).c_str());
    }
    value.type = INT64;
  } else if (type == DOUBLE) {
    if (value.type == INT64) {
      value.v_double = static_cast<double>(value.v_int64);
    } else if (value.type != DOUBLE) {
      LogFATAL("Unexpected value conversion from %s to DOUBLE",
               ValueTypeStr(value.type).c_str());
    }
    value.type = DOUBLE;
  } else if (type == CHAR) {
    if (value.type == INT64) {
      value.v_char = static_cast<char>(value.v_int64);
    } else if (value.type != CHAR) {
      LogFATAL("Unexpected value conversion from %s to DOUBLE",
               ValueTypeStr(value.type).c_str());
    }
    value.type = CHAR;
  }
}


// ***************************** PhysicalPlan ******************************* //
std::string PhysicalPlan::PlanStr(Plan plan) {
  switch (plan) {
    case NO_PLAN: return "NO_PLAN";
    case CONST_FALSE_SKIP: return "CONST_FALSE_SKIP";
    case CONST_TRUE_SCAN: return "CONST_TRUE_SCAN";
    case SCAN: return "SCAN";
    case SEARCH: return "SEARCH";
    case POP: return "POP";
    default: return "UNKNOWN_PLAN_TYPE";
  }
  return "";
}

void PhysicalPlan::reset() {
  plan = NO_PLAN;
  query_ratio = 0;
  pop_node = NON;  // Only used when plan is POP.
  conditions.clear();
}

}  // namespace Query
