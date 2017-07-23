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

FieldInfoManager*
CatalogManager::FindTableFieldByName(const Query::Column& column) {
  auto table_m = FindTableByName(column.table_name);
  if (table_m == nullptr) {
    return nullptr;
  }
  return table_m->FindFieldByName(column.column_name);
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

    auto field_manager = std::make_shared<FieldInfoManager>(&field);
    if (!field_manager->Init()) {
      return false;
    }
    fields_.emplace(field.name(), field_manager);
    fields_by_index_.emplace(field.index(), field_manager.get());
  }

  // Determine the indexes of INDEX_DATA file. If primary key is specified in 
  // schema, use primary keys. If not, use index 0.
  for (const auto& index : table_info_->primary_index().index_fields()) {
    idata_index_.push_back(index);
  }
  if (idata_index_.empty()) {
    idata_index_.push_back(0);
  }

  return true;
}

std::string TableInfoManager::name() const {
  return table_info_->name();
}

FieldInfoManager*
TableInfoManager::FindFieldByName(const std::string field_name) {
  auto it = fields_.find(field_name);
  if (it == fields_.end()) {
    return nullptr;
  }
  return it->second.get();
}

FieldInfoManager* TableInfoManager::FindFieldByIndex(uint32 index) {
  auto it = fields_by_index_.find(index);
  if (it == fields_by_index_.end()) {
    return nullptr;
  }
  return it->second;
}

std::vector<int32> TableInfoManager::PrimaryIndex() const {
  return idata_index_;
}

bool TableInfoManager::IsPrimaryIndex(const std::vector<int32>& index) const {
  if (index.size() != idata_index_.size()) {
    return false;
  }

  for (uint32 i = 0; i < index.size(); i++) {
    if (index.at(i) != idata_index_.at(i)) {
      return false;
    }
  }
  return true;
}

std::vector<int32> TableInfoManager::MakeIndex(const DB::Index& index) {
  std::vector<int32> re;
  for (const auto& index_field : index.index_fields()) {
    re.push_back(index_field);
  }
  return re;
}

bool TableInfoManager::HasIndex(const std::vector<int32>& index) const {
  for (const auto& table_index : table_info_->indexes()) {
    if (table_index.index_fields().size() != index.size()) {
      continue;
    }
    bool match = true;
    for (uint32 i = 0; i < index.size(); i++) {
      if (table_index.index_fields(i) != index.at(i)) {
        match = false;
        break;
      }
    }
    if (match) {
      return true;
    }
  }
  return false;
}

// ************************* FieldInfoManager ****************************** //
FieldInfoManager::FieldInfoManager(TableField* field) : field_(field) {}

bool FieldInfoManager::Init() { return true;}

std::string FieldInfoManager::name() const {
  return field_->name();
}

int32 FieldInfoManager::index() const {
  return field_->index();
}

TableField::Type FieldInfoManager::type() const {
  return field_->type();
}

int32 FieldInfoManager::size() const {
  return field_->size();
}

}  // namespace DB
