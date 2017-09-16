#ifndef QUERY_COMMON_H_
#define QUERY_COMMON_H_

#include <string>

#include "Strings/Utils.h"

#include "Schema/SchemaType.h"
#include "Storage/Record.h"

namespace Query {

enum OperatorType {
  UNKNOWN_OPERATOR,
  ADD,  // +
  SUB,  // -
  MUL,  // *
  DIV,  // /
  MOD,  // %
  EQUAL,     // =
  NONEQUAL,  // !=
  GE,  // >=
  GT,  // >
  LT,  // <
  LE,  // <=
  AND,
  OR,
  NOT,
};

std::string OpTypeStr(OperatorType op_type);
OperatorType StrToOp(const std::string& str);
bool IsNumerateOp(OperatorType op_type);
bool IsCompareOp(OperatorType op_type);
bool IsLogicalOp(OperatorType op_type);
OperatorType FlipOp(OperatorType op_type);


// Column
struct Column {
  Column() = default;
  Column(const std::string& table_name_, const std::string& column_name_):
      table_name(table_name_), column_name(column_name_) {}

  std::string table_name;
  std::string column_name;
  int index = -1;
  Schema::FieldType type = Schema::FieldType::UNKNOWN_TYPE;
  uint32 size = 0; // For CharArray type

  std::string AsString(bool use_table_prefix) const;
  std::string DebugString() const;

  bool operator==(const Column& other) const {
    return table_name == other.table_name && column_name == other.column_name;
  }

  bool operator!=(const Column& other) const {
    return !(*this == other);
  }

  bool operator<(const Column& other) const {
    if (table_name != other.table_name) {
      return table_name < other.table_name;
    } else if (column_name != other.column_name) {
      return column_name < other.column_name;
    }
    return false;
  }
};

}  // namespace Query

#endif  // QUERY_COMMON_H_
