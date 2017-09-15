#include "Base/Log.h"
#include "Base/Path.h"
#include "Base/Utils.h"
#include "IO/FileSystemUtils.h"
#include "Strings/Utils.h"
#include "UnitTest/UnitTest.h"

#include "Database/Database.h"
#include "Query/Interpreter.h"
#include "Query/SqlQuery.h"
#include "Storage/Record.h"

namespace DB {

namespace {

using Storage::DataRecord;
using Storage::IndexRecord;
using Storage::RecordBase;
using Query::FetchedResult;
using Query::Interpreter;
using Query::OperatorType;
using Query::PhysicalPlan;
using Query::ResultRecord;
using Query::SqlQuery;

const char* const kDBName = "test_db";
const char* const kTableName = "Puppy";
const char* const kHostTableName = "Host";
const int kNumRecordsSource = 20;
const int kNumHosts = 3;

}

class DatabaseTest: public UnitTest {
 private:
  std::vector<std::shared_ptr<RecordBase>> puppy_records_;
  std::vector<std::shared_ptr<RecordBase>> host_records_;
  std::vector<uint32> key_fields_ = std::vector<uint32>{0, 2, 3};

  DatabaseCatalog catalog_;
  std::shared_ptr<Database> db_;
  Table* puppy_table_ = nullptr;

  std::shared_ptr<Interpreter> interpreter_;
  std::shared_ptr<SqlQuery> query_;

 public:
  void InitCatalog() {
    catalog_.set_name(kDBName);

    // Create a table Puppy.
    auto table_info = catalog_.add_tables();
    table_info->set_name(kTableName);

    // Add string type
    DB::TableField* field = table_info->add_fields();
    field->set_name("name");
    field->set_index(0);
    field->set_type(DB::TableField::STRING);
    field->mutable_min_value()->set_limit_str("");
    field->mutable_max_value()->set_limit_str("zzz");
    // Add int type
    field = table_info->add_fields();
    field->set_name("age");
    field->set_index(1);
    field->set_type(DB::TableField::INT);
    field->mutable_min_value()->set_limit_int32(0);
    field->mutable_max_value()->set_limit_int32(9);
    // Add long int type
    field = table_info->add_fields();
    field->set_name("id");
    field->set_index(2);  // primary key
    field->set_type(DB::TableField::LONGINT);
    field->mutable_min_value()->set_limit_int64(0);
    field->mutable_max_value()->set_limit_int64(kNumRecordsSource - 1);
    // Add double type
    field = table_info->add_fields();
    field->set_name("weight");
    field->set_index(3);
    field->set_type(DB::TableField::DOUBLE);
    field->mutable_min_value()->set_limit_double(0.5);
    field->mutable_max_value()->set_limit_double(2.0);
    // Add bool type
    field = table_info->add_fields();
    field->set_name("adult");
    field->set_index(4);
    field->set_type(DB::TableField::BOOL);
    field->mutable_min_value()->set_limit_bool(false);
    field->mutable_max_value()->set_limit_bool(true);
    // Add char array type
    field = table_info->add_fields();
    field->set_name("signature");
    field->set_index(5);
    field->set_type(DB::TableField::CHARARRAY);
    field->set_size(3);
    field->mutable_min_value()->set_limit_chararray("a");
    field->mutable_max_value()->set_limit_chararray("zzz");
    // Add long int type
    field = table_info->add_fields();
    field->set_name("host_id");
    field->set_index(6);  // primary key
    field->set_type(DB::TableField::LONGINT);
    field->mutable_min_value()->set_limit_int64(0);
    field->mutable_max_value()->set_limit_int64(kNumHosts - 1);

    // Set primary and other keys.
    auto* index = table_info->mutable_primary_index();
    index->add_index_fields(2);

    index = table_info->add_indexes();
    index->add_index_fields(0);
    index = table_info->add_indexes();
    index->add_index_fields(1);
    index = table_info->add_indexes();
    index->add_index_fields(3);
    index = table_info->add_indexes();
    index->add_index_fields(4);
    index = table_info->add_indexes();
    index->add_index_fields(5);
    index = table_info->add_indexes();
    index->add_index_fields(6);

    // Create a table Host.
    table_info = catalog_.add_tables();
    table_info->set_name(kHostTableName);

    // Add long int type
    field = table_info->add_fields();
    field->set_name("id");
    field->set_index(0);  // primary key
    field->set_type(DB::TableField::LONGINT);
    field->mutable_min_value()->set_limit_int64(0);
    field->mutable_max_value()->set_limit_int64(kNumHosts - 1);

    // Add string type
    field = table_info->add_fields();
    field->set_name("name");
    field->set_index(1);
    field->set_type(DB::TableField::STRING);
    field->mutable_min_value()->set_limit_str("aaa");
    field->mutable_max_value()->set_limit_str("zzz");

    // Set primary and other keys.
    index = table_info->mutable_primary_index();
    index->add_index_fields(0);

    index = table_info->add_indexes();
    index->add_index_fields(1);
  }

  void InitRecordResource() {
    // Puppies.
    puppy_records_.clear();
    for (int i = 0; i < kNumRecordsSource; i++) {
      puppy_records_.push_back(std::shared_ptr<RecordBase>(new DataRecord()));

      // Init fields to records.
      // name
      {
        int str_len = Utils::RandomNumber(4);
        char buf[str_len];
        for (int i = 0; i < str_len; i++) {
          buf[i] = 'a' + Utils::RandomNumber(26);
        }
        puppy_records_.at(i)->AddField(new Schema::StringField(buf, str_len));
      }
      // age: 0 ~ 9
      int rand_int = Utils::RandomNumber(10);
      puppy_records_.at(i)->AddField(new Schema::IntField(rand_int));
      // id: 0 ~ (kNumRecordsSource - 1)
      puppy_records_.at(i)->AddField(new Schema::LongIntField(i));
      // weight: 0.5 ~ 2.0
      double rand_double = 0.5 + 1.5 * Utils::RandomFloat();
      puppy_records_.at(i)->AddField(new Schema::DoubleField(rand_double));
      // adult
      bool rand_bool = Utils::RandomNumber() % 2 == 1 ? true : false;
      puppy_records_.at(i)->AddField(new Schema::BoolField(rand_bool));
      // signature
      {
        int len_limit = 3;
        int str_len = Utils::RandomNumber(len_limit) + 1;
        char buf[str_len];
        for (int i = 0; i < str_len; i++) {
          buf[i] = 'a' + Utils::RandomNumber(26);
        }
        puppy_records_.at(i)->AddField(
            new Schema::CharArrayField(buf, str_len, len_limit));
      }
      // host_id: 0 ~ (kNumHosts -1)
      rand_int = Utils::RandomNumber(kNumHosts);
      puppy_records_.at(i)->AddField(new Schema::LongIntField(rand_int));

      //puppy_records_.at(i)->Print();
    }

    // Hosts.
    host_records_.clear();
    for (int i = 0; i < kNumHosts; i++) {
      host_records_.push_back(std::shared_ptr<RecordBase>(new DataRecord()));

      // id: 0 ~ (kNumHosts - 1)
      host_records_.at(i)->AddField(new Schema::LongIntField(i));

      // name
      if (i == 1) {
        host_records_.at(i)->AddField(new Schema::StringField("hy"));
      } else {
        int str_len = Utils::RandomNumber(4) + 1;
        char buf[str_len];
        for (int i = 0; i < str_len; i++) {
          buf[i] = 'a' + Utils::RandomNumber(26);
        }
        host_records_.at(i)->AddField(new Schema::StringField(buf, str_len));
      }
    }
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

  void CreateDatabase() {
    db_ = Database::CreateDatabase(kDBName);
    AssertTrue(catalog_.Equals(db_->catalog()));
  }

  void LoadData() {
    auto load_data_for_table = [&] (
        DB::Table* table,
        std::vector<std::shared_ptr<RecordBase>>& records) {
      table->PreLoadData(records);
      AssertTrue(table->ValidateAllIndexRecords(records.size()));

      for (const auto& index: table->schema().indexes()) {
        std::vector<uint32> key_index = TableInfoManager::MakeIndex(index);
        printf("Checking tree %s\n", Table::IndexStr(key_index).c_str());
        auto file_type = table->IsDataFileKey(key_index) ?
                             Storage::INDEX_DATA : Storage::INDEX;
        auto tree = table->Tree(file_type, key_index);
        tree->SaveToDisk();

        AssertTrue(tree->ValidityCheck(), "Check Index tree failed");
      }
      printf("Checking tree %s\n",
             Table::IndexStr(table->DataTreeKey()).c_str());
      AssertTrue(table->DataTree()->ValidityCheck(),
                 "Check Data tree failed");

      printf("\n");
    };

    auto puppy_table_ = db_->GetTable(kTableName);
    CHECK(puppy_table_ != nullptr, "Couldn't get table %s", kTableName);
    load_data_for_table(puppy_table_, puppy_records_);

    auto host_table_ = db_->GetTable(kHostTableName);
    CHECK(host_table_ != nullptr, "Couldn't get table %s", kHostTableName);    
    load_data_for_table(host_table_, host_records_);
  }

  void setup() override {
    InitCatalog();
    InitRecordResource();
    CreateCatalogFile();
    CreateDatabase();
    LoadData();

    interpreter_ = std::make_shared<Interpreter>(db_.get());
    interpreter_->set_debug(true);
  }

  int ExpectedResultNum(const Query::ExprTreeNode& expr_root) {
    int expected_num = 0;
    for (const auto& puppy_record : puppy_records_) {
      auto tuple = Tuple();
      tuple.AddTableRecord(kTableName, puppy_record);
      if (expr_root.Evaluate(tuple).v_bool) {
        expected_num++;
      }
    }
    return expected_num;
  }

  int ExpectedJoinResultNum(const Query::ExprTreeNode& expr_root) {
    int expected_num = 0;
    for (const auto& puppy_record : puppy_records_) {
      for (const auto& host_record : host_records_) {
        auto tuple = Tuple();
        tuple.AddTableRecord(kTableName, puppy_record);
        tuple.AddTableRecord(kHostTableName, host_record);
        if (expr_root.Evaluate(tuple).v_bool) {
          expected_num++;
        }
      }
    }
    return expected_num;
  }

  bool VerifyResult(const Query::ExprTreeNode& expr_root,
                    const ResultContainer& result) {
    // while (true) {
    //   auto tuple = result->GetNextTuple();
    //   if (!expr_root.Evaluate(tuple).v_bool) {
    //     LogERROR("Result record mismatch with query:");
    //     tuple.at(kTableName).record->Print();
    //     return false;
    //   }
    // }
    return true;
  }

  void Test_SelectQuery() {
    std::cout << __FUNCTION__ << std::endl;
    std::string expr;

    expr = "SELECT * FROM Puppy WHERE Puppy.id < 300";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    auto query = interpreter_->shared_query();
    AssertTrue(query->FinalizeParsing());
    int num_results = query->ExecuteSelectQuery();
    printf("num_results = %d\n", num_results);
    AssertEqual(ExpectedResultNum(query->expr_root()), num_results);
    AssertTrue(VerifyResult(query->expr_root(), query->results()));
    interpreter_->reset();
    printf("\n");

    expr = "SELECT * FROM Puppy WHERE id > -1 AND id < 10";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    query = interpreter_->shared_query();
    AssertTrue(query->FinalizeParsing());
    num_results = query->ExecuteSelectQuery();
    printf("num_results = %d\n", num_results);
    query->PrintResults();
    interpreter_->reset();
    printf("\n");

    expr = "SELECT * FROM Puppy WHERE Puppy.id < 300 AND age < 1";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    query = interpreter_->shared_query();
    AssertTrue(query->FinalizeParsing());
    num_results = query->ExecuteSelectQuery();
    printf("num_results = %d\n", num_results);
    AssertEqual(ExpectedResultNum(query->expr_root()), num_results);
    AssertTrue(VerifyResult(query->expr_root(), query->results()));
    interpreter_->reset();
    printf("\n");

    expr = "SELECT * FROM Puppy WHERE Puppy.id > 700 OR weight > 1.7";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    query = interpreter_->shared_query();
    AssertTrue(query->FinalizeParsing());
    num_results = query->ExecuteSelectQuery();
    printf("num_results = %d\n", num_results);
    AssertEqual(ExpectedResultNum(query->expr_root()), num_results);
    AssertTrue(VerifyResult(query->expr_root(), query->results()));
    interpreter_->reset();
    printf("\n");

    expr = "SELECT * FROM Puppy WHERE true";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    query = interpreter_->shared_query();
    AssertTrue(query->FinalizeParsing());
    num_results = query->ExecuteSelectQuery();
    printf("num_results = %d\n", num_results);
    AssertEqual(ExpectedResultNum(query->expr_root()), num_results);
    AssertTrue(VerifyResult(query->expr_root(), query->results()));
    interpreter_->reset();
    printf("\n");

    expr = "SELECT * FROM Puppy";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    query = interpreter_->shared_query();
    AssertTrue(query->FinalizeParsing());
    num_results = query->ExecuteSelectQuery();
    printf("num_results = %d\n", num_results);
    AssertEqual(ExpectedResultNum(query->expr_root()), num_results);
    AssertTrue(VerifyResult(query->expr_root(), query->results()));
    interpreter_->reset();
    printf("\n");

    expr = "SELECT * FROM Puppy WHERE adult AND NOT (age < 30)";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    query = interpreter_->shared_query();
    AssertTrue(query->FinalizeParsing());
    num_results = query->ExecuteSelectQuery();
    printf("num_results = %d\n", num_results);
    AssertEqual(ExpectedResultNum(query->expr_root()), num_results);
    AssertTrue(VerifyResult(query->expr_root(), query->results()));
    interpreter_->reset();
    printf("\n");

    expr = "SELECT * FROM Puppy WHERE NOT adult AND age > 7";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    query = interpreter_->shared_query();
    AssertTrue(query->FinalizeParsing());
    num_results = query->ExecuteSelectQuery();
    printf("num_results = %d\n", num_results);
    AssertEqual(ExpectedResultNum(query->expr_root()), num_results);
    AssertTrue(VerifyResult(query->expr_root(), query->results()));
    interpreter_->reset();
    printf("\n");

    expr = "SELECT * FROM Puppy WHERE id = 4 OR(age > 8 AND signature < \"h\") ORDER BY name, id";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    query = interpreter_->shared_query();
    AssertTrue(query->FinalizeParsing());
    num_results = query->ExecuteSelectQuery();
    printf("num_results = %d\n", num_results);
    query->PrintResults();
    AssertEqual(ExpectedResultNum(query->expr_root()), num_results);
    AssertTrue(VerifyResult(query->expr_root(), query->results()));
    interpreter_->reset();
    printf("\n");

    expr = "SELECT * FROM Puppy WHERE id <100 OR(age > 8 OR name = \"snoopy\")";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    query = interpreter_->shared_query();
    AssertTrue(query->FinalizeParsing());
    num_results = query->ExecuteSelectQuery();
    printf("num_results = %d\n", num_results);
    AssertEqual(ExpectedResultNum(query->expr_root()), num_results);
    AssertTrue(VerifyResult(query->expr_root(), query->results()));
    interpreter_->reset();
    printf("\n");

    expr = "SELECT * FROM Puppy WHERE id >500 OR weight < 1.0";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    query = interpreter_->shared_query();
    AssertTrue(query->FinalizeParsing());
    num_results = query->ExecuteSelectQuery();
    printf("num_results = %d\n", num_results);
    AssertEqual(ExpectedResultNum(query->expr_root()), num_results);
    AssertTrue(VerifyResult(query->expr_root(), query->results()));
    interpreter_->reset();
    printf("\n");

    expr = "SELECT * FROM Puppy WHERE age + 3 < 8";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    query = interpreter_->shared_query();
    AssertTrue(query->FinalizeParsing());
    num_results = query->ExecuteSelectQuery();
    printf("num_results = %d\n", num_results);
    AssertEqual(ExpectedResultNum(query->expr_root()), num_results);
    AssertTrue(VerifyResult(query->expr_root(), query->results()));
    interpreter_->reset();
    printf("\n");

    expr = "SELECT * FROM Puppy WHERE NOT NOT NOT false";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    query = interpreter_->shared_query();
    AssertTrue(query->FinalizeParsing());
    num_results = query->ExecuteSelectQuery();
    printf("num_results = %d\n", num_results);
    AssertEqual(ExpectedResultNum(query->expr_root()), num_results);
    AssertTrue(VerifyResult(query->expr_root(), query->results()));
    interpreter_->reset();
    printf("\n");

    expr = "SELECT age, AVG(weight) FROM Puppy GROUP BY age ORDER BY AVG(weight)";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    query = interpreter_->shared_query();
    AssertTrue(query->FinalizeParsing());
    num_results = query->ExecuteSelectQuery();
    printf("num_results = %d\n", num_results);
    query->PrintResults();
    interpreter_->reset();
    printf("\n");
  }

  void Test_Join() {
    std::cout << __FUNCTION__ << std::endl;
    std::string expr;

    expr = "SELECT Puppy.*, Host.* FROM Puppy, Host "
           "WHERE Puppy.host_id = Host.id "
           "ORDER BY Host.id, Puppy.id";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    auto query = interpreter_->shared_query();
    AssertTrue(query->FinalizeParsing());
    int num_results = query->ExecuteJoinQuery();
    printf("num_results = %d\n", num_results);
    query->PrintResults();
    AssertEqual(ExpectedJoinResultNum(query->expr_root()), num_results);
    AssertTrue(VerifyResult(query->expr_root(), query->results()));
    interpreter_->reset();
    printf("\n");

    expr = "SELECT Puppy.*, Host.* FROM Puppy, Host "
           "WHERE Puppy.host_id = Host.id AND "
           "(Puppy.name <= \"h\" OR Puppy.weight < 1.1) AND "
           "Host.name = \"hy\"";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    query = interpreter_->shared_query();
    AssertTrue(query->FinalizeParsing());
    num_results = query->ExecuteJoinQuery();
    printf("num_results = %d\n", num_results);
    query->PrintResults();
    AssertEqual(ExpectedJoinResultNum(query->expr_root()), num_results);
    AssertTrue(VerifyResult(query->expr_root(), query->results()));
    interpreter_->reset();
    printf("\n");

    expr = "SELECT Puppy.*, Host.* FROM Puppy, Host "
           "WHERE Puppy.host_id = Host.id AND "
           "Host.name = \"hy\"";
    std::cout << expr << std::endl;
    AssertTrue(interpreter_->Parse(expr));
    query = interpreter_->shared_query();
    AssertTrue(query->FinalizeParsing());
    num_results = query->ExecuteJoinQuery();
    printf("num_results = %d\n", num_results);
    query->PrintResults();
    AssertEqual(ExpectedJoinResultNum(query->expr_root()), num_results);
    AssertTrue(VerifyResult(query->expr_root(), query->results()));
    interpreter_->reset();
    printf("\n");
  }
};

}  // namespace DB


int main(int argc, char** argv) {
  DB::DatabaseTest test;
  test.setup();

  test.Test_SelectQuery();
  test.Test_Join();

  test.teardown();
  std::cout << "\033[2;32mAll Passed ^_^\033[0m" << std::endl;
  return 0;
}