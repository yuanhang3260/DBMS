#include "Base/Log.h"
#include "Base/Ptr.h"

#include "Database/CatalogManager.h"

namespace DB {

// ************************* CatalogManager ********************************* //
CatalogManager::CatalogManager(DatabaseCatalog* catalog) : catalog_(catalog) {}

bool CatalogManager::Init() {
  // Table name --> TableInfo map
  for (auto& table : catalog_->mutable_tables()) {
    if (tables_.find(table.name()) != tables_.end()) {
      LogERROR("Duplicated table %s", table.name().c_str());
      return false;
    }

    auto table_manager = std::make_shared<TableInfoManager>(&table);
    if (!table_manager->Init()) {
      return false;
    }
    tables_.emplace(table.name(), table_manager);
  }
  return true;
}

std::string CatalogManager::DBName() const {
  return catalog_->name();
}

TableInfoManager*
CatalogManager::FindTableByName(const std::string& table_name) {
  auto it = tables_.find(table_name);
  if (it == tables_.end()) {
    return nullptr;
  }
  return it->second.get();
}

// ************************* TableInfoManager ******************************* //
TableInfoManager::TableInfoManager(TableInfo* table) : table_info_(table) {}

bool TableInfoManager::Init() {
  for (auto& field : table_info_->mutable_fields()) {
    if (fields_.find(field.name()) != fields_.end()) {
      LogERROR("Duplicated field: %s", field.name().c_str());
      return false;
    }

    if (fields_by_index_.find(field.index()) != fields_by_index_.end()) {
      LogERROR("Duplicated field index %d", field.index());
      return false;
    }

    auto field_manager = std::make_shared<TableFieldManager>(&field);
    if (!field_manager->Init()) {
      return false;
    }
    fields_.emplace(field.name(), field_manager);
    fields_by_index_.emplace(field.index(), field_manager.get());
  }

  return true;
}

std::string TableInfoManager::name() const {
  return table_info_->name();
}

TableFieldManager*
TableInfoManager::FindFieldByName(const std::string field_name) {
  auto it = fields_.find(field_name);
  if (it == fields_.end()) {
    return nullptr;
  }
  return it->second.get();
}

TableFieldManager* TableInfoManager::FindFieldByIndex(uint32 index) {
  auto it = fields_by_index_.find(index);
  if (it == fields_by_index_.end()) {
    return nullptr;
  }
  return it->second;
}

std::vector<int32> TableInfoManager::primary_key_indexes() const {
  std::vector<int32> key_indexes;
  for (const auto& index : table_info_->primary_key_indexes()) {
    key_indexes.push_back(index);
  }
  return key_indexes;
}


// ************************* TableFieldManager ****************************** //
TableFieldManager::TableFieldManager(TableField* field) : field_(field) {}

bool TableFieldManager::Init() { return true;}

std::string TableFieldManager::name() const {
  return field_->name();
}

int32 TableFieldManager::index() const {
  return field_->index();
}

TableField::Type TableFieldManager::type() const {
  return field_->type();
}

int32 TableFieldManager::size() const {
  return field_->size();
}

}  // namespace DB