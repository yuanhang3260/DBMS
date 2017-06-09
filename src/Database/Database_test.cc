#include "Base/Log.h"
#include "Base/Path.h"
#include "Base/Utils.h"
#include "IO/FileSystemUtils.h"
#include "Strings/Utils.h"
#include "UnitTest/UnitTest.h"

#include "Database/Database.h"

namespace DB {

namespace {
const char* const kDBName = "test_db";
const char* const kTableName = "testTable";
}

class DatabaseTest: public UnitTest {
 private:
  DatabaseCatalog catalog_;
  std::unique_ptr<Database> db_;

 public:
  void InitCatalog() {
    catalog_.set_name(kDBName);
    auto table = catalog_.add_tables();

    // Add a table schema.
    table->set_name(kTableName);
    // Add string type
    DB::TableField* field = table->add_fields();
    field->set_name("name");
    field->set_index(0);
    field->set_type(DB::TableField::STRING);
    // Add int type
    field = table->add_fields();
    field->set_name("age");
    field->set_index(1);
    field->set_type(DB::TableField::INTEGER);
  }

  bool CreateCatalogFile() {
    // Serialize the catalog message and write to file.
    ::proto::SerializedMessage* sdmsg = catalog_.Serialize();
    const char* obj_data = sdmsg->GetBytes();

    std::string catalog_filename =
        Path::JoinPath(Storage::kDataDirectory, kDBName, "catalog.pb");

    std::string db_dir = Path::JoinPath(Storage::kDataDirectory, kDBName);

    AssertTrue(FileSystem::CreateDir(db_dir));
    AssertTrue(FileSystem::CreateFile(catalog_filename));

    FILE* file = fopen(catalog_filename.c_str(), "w+");
    if (!file) {
      LogERROR("Failed to open schema file %s", catalog_filename.c_str());
      return false;
    }
    // Read schema file.
    int re = fwrite(obj_data, 1, sdmsg->size(), file);
    if (re != (int)sdmsg->size()) {
      LogERROR("Read schema file %s error, expect %d bytes, actual %d",
               catalog_filename.c_str(), sdmsg->size(), re);
      return false;
    }
    fclose(file);
    return true;
  }

  void setup() override {
    InitCatalog();
    CreateCatalogFile();
  }

  // Create database and load catalog file. Verify it is correct.
  void Test_CatalogIsValid() {
    db_ = Database::CreateDatabase(kDBName);
    AssertTrue(catalog_.Equals(db_->catalog()));
  }

};

}  // namespace DB


int main(int argc, char** argv) {
  DB::DatabaseTest test;
  test.setup();

  test.Test_CatalogIsValid();

  test.teardown();
  std::cout << "\033[2;32mAll Passed ^_^\033[0m" << std::endl;
  return 0;
}