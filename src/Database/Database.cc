#include <sys/stat.h>
#include <string.h>

#include "Base/BaseTypes.h"
#include "Base/Log.h"
#include "Base/Path.h"
#include "IO/FileSystemUtils.h"
#include "Strings/Utils.h"

#include "Database/Database.h"

namespace DB {

std::unique_ptr<Database> Database::CreateDatabase(const std::string& name) {
  // TODO: Instead of pass in db name, pass in and parse db schema definition,
  // and create catalog file in //data/${db_name} directory.
  return std::unique_ptr<Database>(new Database(name));
}

Database::Database(const std::string& name) : name_(name) {
  printf("Loading catalog...\n");
  CHECK(LoadCatalog(), "Failed to load catalog for database %s", name_.c_str());

  catalog_m_.reset(new CatalogManager(&catalog_));
  CHECK(catalog_m_->Init(), "Failed to init CatalogManager");

  // Create Tables.
  for (const auto& table : catalog_.tables()) {
    auto table_m = catalog_m_->FindTableByName(table.name());
    CHECK(table_m != nullptr, "Could't find table %s", table.name().c_str());
    tables_.emplace(table.name(),
                    std::unique_ptr<Table>(
                        new Table(catalog_.name(), table.name(), table_m)));
  }
}

bool Database::LoadCatalog() {
  std::string catalog_filename =
      Path::JoinPath(Storage::kDataDirectory, name_, "catalog.pb");

  struct stat stat_buf;
  int re = stat(catalog_filename.c_str(), &stat_buf);
  if (re < 0) {
    LogERROR("Failed to stat catalog file %s", catalog_filename.c_str());
    return false;
  }

  int size = stat_buf.st_size;
  FILE* file = fopen(catalog_filename.c_str(), "r");
  if (!file) {
    LogERROR("Failed to open catalog file %s", catalog_filename.c_str());
    return false;
  }
  // Read catalog file.
  char buf[size];
  re = fread(buf, 1, size, file);
  if (re != size) {
    LogERROR("Read catalog file %s error, expect %d bytes, actual %d",
             catalog_filename.c_str(), size, re);
    return false;
  }
  fclose(file);
  // Parse catalog proto data.
  catalog_.DeSerialize(buf, size);
  return true;
}

bool Database::SetCatalog(const DatabaseCatalog& catalog) {
  if (catalog_m_) {
    return false;
  }

  catalog_ = catalog;
  catalog_m_.reset(new CatalogManager(&catalog_));
  if (!catalog_m_->Init()) {
    return false;
  }
  return true;
}

Table* Database::GetTable(const std::string& table_name) {
  auto it = tables_.find(table_name);
  if (it == tables_.end()) {
    return nullptr;
  }
  return (it->second).get();
}


}  // namsespace DB
