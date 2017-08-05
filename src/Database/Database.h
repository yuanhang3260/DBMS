#ifndef DATABSE_DATABSE_
#define DATABSE_DATABSE_

#include <vector>
#include <map>
#include <memory>

#include "Database/CatalogManager.h"
#include "Database/Catalog_pb.h"
#include "Database/Table.h"

namespace DB {

class Database {
 public:
  Database() = default;
  Database(const std::string& name);

  // TODO: Pass in db schema definition instead of db name.
  static std::unique_ptr<Database> CreateDatabase(const std::string& name);

  const DatabaseCatalog& catalog() const { return catalog_; }
  bool SetCatalog(const DatabaseCatalog& catalog);

  CatalogManager* mutable_catalog_manager() { return catalog_m_.get(); }

  Table* GetTable(const std::string& table_name);

 private:
  bool LoadCatalog();

  std::string name_;
  DatabaseCatalog catalog_;
  std::unique_ptr<CatalogManager> catalog_m_;

  std::map<std::string, std::unique_ptr<Table>> tables_;

  FORBID_COPY_AND_ASSIGN(Database);
};

}  // namsespace DB


#endif  // DATABSE_DATABSE_
