#ifndef DATABASE_CATALOG_MANAGER_
#define DATABASE_CATALOG_MANAGER_

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "Base/MacroUtils.h"
#include "Database/Catalog_pb.h"

// This file is a C++ view of Catalog proto.
namespace DB {

class TableInfoManager;
class TableFieldManager;

class CatalogManager {
 public:
  explicit CatalogManager(DatabaseCatalog* catalog);
  bool Init();

  const DatabaseCatalog& catalog() const { return *catalog_; }
  DatabaseCatalog* mutable_catalog()  { return catalog_; }

  // DB name.
  std::string DBName() const;

  TableInfoManager* FindTableByName(const std::string& table_name);

 private:
  DatabaseCatalog* catalog_;

  std::map<std::string, std::shared_ptr<TableInfoManager>> tables_;

  FORBID_COPY_AND_ASSIGN(CatalogManager);
};

class TableInfoManager {
 public:
  explicit TableInfoManager(TableInfo* table);
  bool Init();

  const TableInfo& table_info() const { return *table_info_; }
  TableInfo* mtable_table_info() { return table_info_; }

  // Table name.
  std::string name() const;

  TableFieldManager* FindFieldByName(const std::string field_name);
  TableFieldManager* FindFieldByIndex(uint32 index);

  std::vector<int32> primary_key_indexes() const;

 private:
  TableInfo* table_info_;

  std::map<std::string, std::shared_ptr<TableFieldManager>> fields_;
  std::map<int32, TableFieldManager*> fields_by_index_;
};

class TableFieldManager {
 public:
  explicit TableFieldManager(TableField* field);
  bool Init();

  const TableField& field() const { return *field_; }
  TableField* mutable_field() { return field_; }

  std::string name() const;
  int32 index() const;
  TableField::Type type() const;
  int32 size() const;

 private:
  TableField* field_;
};

}  // namespace DB

#endif  // DATABASE_CATALOG_MANAGER_
