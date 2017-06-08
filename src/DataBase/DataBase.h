#ifndef DATABSE_DATABSE_
#define DATABSE_DATABSE_

#include <vector>
#include <map>
#include <memory>

#include "DataBase/Catalog_pb.h"
#include "DataBase/Table.h"

namespace DB {

class DataBase {
 public:
  // TODO: Pass in db schema definition instead of db name.
  static std::unique_ptr<DataBase> CreateDataBase(const std::string& name);

 private:
  DataBase(const std::string& name);
  bool LoadCatalog();

  std::string name_;
  DatabaseCatalog catalog_;

  std::map<std::string, std::unique_ptr<Table>> tables_;

  FORBID_COPY_AND_ASSIGN(DataBase);
};

}  // namsespace DB


#endif  // DATABSE_DATABSE_
