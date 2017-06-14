#include "Base/Log.h"
#include "Base/MacroUtils.h"
#include "Base/Ptr.h"
#include "IO/FileSystemUtils.h"
#include "UnitTest/UnitTest.h"

#include "Storage/MergeSort.h"

namespace Storage {

namespace {
const char* const kDBName = "test_db";
const char* const kTableName = "testTable";
const int kNumRecordsSource = 1000;
}  // namespace 

class MergeSortTest: public UnitTest {
 private:
  std::vector<std::shared_ptr<DataRecord>> record_resource_;
  DB::TableSchema schema_;
  std::vector<int> key_indexes_ = std::vector<int>{1, 0};
  std::unique_ptr<MergeSorter> sorter_;

 public:
  void InitSchema() {
    // Create a table schema_.
    schema_.set_name(kTableName);
    // Add string type
    DB::TableField* field = schema_.add_fields();
    field->set_name("name");
    field->set_index(0);
    field->set_type(DB::TableField::STRING);
    // Add int type
    field = schema_.add_fields();
    field->set_name("age");
    field->set_index(1);
    field->set_type(DB::TableField::INTEGER);
    // Add char array type
    field = schema_.add_fields();
    field->set_name("signature");
    field->set_index(5);
    field->set_type(DB::TableField::CHARARR);
    field->set_size(20);
  }

  void InitRecordResource() {
    record_resource_.clear();
    for (int i = 0; i < kNumRecordsSource; i++) {
      record_resource_.push_back(std::make_shared<DataRecord>());

      // Init fields to records.
      // name
      {
        int str_len = Utils::RandomNumber(10);
        char buf[str_len];
        for (int i = 0; i < str_len; i++) {
          buf[i] = 'a' + Utils::RandomNumber(26);
        }
        record_resource_[i]->AddField(new Schema::StringField(buf, str_len));
      }
      // age
      int rand_int = Utils::RandomNumber(20);
      record_resource_[i]->AddField(new Schema::IntField(rand_int));
      // signature
      {
        int len_limit = 20;
        int str_len = Utils::RandomNumber(len_limit) + 1;
        char buf[str_len];
        for (int i = 0; i < str_len; i++) {
          buf[i] = 'a' + Utils::RandomNumber(26);
        }
        record_resource_[i]->AddField(
            new Schema::CharArrayField(buf, str_len, len_limit));
      }
    }
  }

  void InitDataDir() {
    SANITY_CHECK(FileSystem::CreateDir(Storage::DBDataDir(kDBName)),
                 "Failed to create test db data dir.");
  }

  void setup() {
    InitDataDir();
    InitSchema();
    InitRecordResource();

    // Create merge sorter.
    MergeSortOptions opts(kDBName, 1 /* txn_id*/, &schema_,
                          key_indexes_, INDEX_DATA);
    sorter_ = ptr::MakeUnique<MergeSorter>(opts);
    AssertTrue(sorter_->Init());
  }

  void Test_WriteRead() {
    std::string ms_file_name = sorter_->TempfilePath(0, 0);
    FileSystem::Remove(ms_file_name);
    MergeSortTempfileManager ms_file_manager(&sorter_->options(), ms_file_name);
    AssertTrue(ms_file_manager.Init());

    // Write records to file.
    for (uint32 i = 0; i < record_resource_.size(); i++) {
      AssertTrue(ms_file_manager.WriteRecord(*record_resource_.at(i)));
    }
    ms_file_manager.FinishFile();

    // Read records from file and compare with original records.
    AssertTrue(ms_file_manager.InitForReading());
    uint32 total_records = 0;
    while (true) {
      auto next_record = ms_file_manager.NextRecord();
      if (!next_record) {
        break;
      }
      AssertTrue(*next_record == *record_resource_.at(total_records));
      total_records++;
    }
    AssertEqual(record_resource_.size(), total_records);
  }
};

}  // namespace Storage

int main() {
  Storage::MergeSortTest test;
  test.setup();

  test.Test_WriteRead();

  test.teardown();

  std::cout << "\033[2;32mPassed ^_^\033[0m" << std::endl;
  return 0;
}
