#ifndef DATABSE_DATABSE_
#define DATABSE_DATABSE_

#include <vector>
#include <map>
#include <memory>

#include "Database/Catalog_pb.h"
#include "Database/Table.h"

namespace DB {

class Database {
 public:
  // TODO: Pass in db schema definition instead of db name.
  static std::unique_ptr<Database> CreateDatabase(const std::string& name);

  const DatabaseCatalog catalog() const { return catalog_; }

 private:
  Database(const std::string& name);
  bool LoadCatalog();

  std::string name_;
  DatabaseCatalog catalog_;

  std::map<std::string, std::unique_ptr<Table>> tables_;

  FORBID_COPY_AND_ASSIGN(Database);
};

}  // namsespace DB


#endif  // DATABSE_DATABSE_
