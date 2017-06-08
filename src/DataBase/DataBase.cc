#include <sys/stat.h>
#include <string.h>

#include "Base/BaseTypes.h"
#include "Base/Log.h"
#include "Strings/Utils.h"

#include "DataBase/DataBase.h"

namespace DB {

std::unique_ptr<DataBase> DataBase::CreateDataBase(const std::string& name) {
  // TODO: Instead of pass in db name, pass in and parse db schema definition,
  // and create catalog file in //data/${db_name} directory.
  return std::unique_ptr<DataBase>(new DataBase(name));
}

DataBase::DataBase(const std::string& name) : name_(name) {
  SANITY_CHECK(LoadCatalog(),
               "Failed to load catalog for database %s", name_.c_str());

  // TODO: Create Tables.
}

bool DataBase::LoadCatalog() {
  std::string catalog_filename =
      Strings::StrCat(Storage::kDataDirectory, name_, "/", "catalog.pb");

  struct stat stat_buf;
  int re = stat(catalog_filename.c_str(), &stat_buf);
  if (re < 0) {
    LogERROR("Failed to stat schema file %s", catalog_filename.c_str());
    return false;
  }

  int size = stat_buf.st_size;
  FILE* file = fopen(catalog_filename.c_str(), "r");
  if (!file) {
    LogERROR("Failed to open schema file %s", catalog_filename.c_str());
    return false;
  }
  // Read catalog file.
  char buf[size];
  re = fread(buf, 1, size, file);
  if (re != size) {
    LogERROR("Read schema file %s error, expect %d bytes, actual %d",
             catalog_filename.c_str(), size, re);
    return false;
  }
  fclose(file);
  // Parse catalog proto data.
  catalog_.DeSerialize(buf, size);
  return true;
}



}  // namsespace DB
