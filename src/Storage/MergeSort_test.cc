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
const int kNumRecordsSource = 10000;
}  // namespace

class MergeSortTest: public UnitTest {
 private:
  std::vector<std::shared_ptr<RecordBase>> record_resource_;
  DB::TableInfo schema_;
  std::vector<int> key_fields_ = std::vector<int>{0, 2, 3};
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
    // Add double type
    field = schema_.add_fields();
    field->set_name("weight");
    field->set_index(1);
    field->set_type(DB::TableField::DOUBLE);
    // Add int type
    field = schema_.add_fields();
    field->set_name("age");
    field->set_index(2);
    field->set_type(DB::TableField::INTEGER);
    // Add char array type
    field = schema_.add_fields();
    field->set_name("signature");
    field->set_index(3);
    field->set_type(DB::TableField::CHARARR);
    field->set_size(20);
  }

  void InitRecordResource() {
    record_resource_.clear();
    for (int i = 0; i < kNumRecordsSource; i++) {
      auto record = std::make_shared<IndexRecord>();
      record->set_rid(RecordID(0, 0));
      record_resource_.push_back(record);

      // Init fields to records.
      // name
      {
        int str_len = Utils::RandomNumber(10);
        char buf[str_len];
        for (int i = 0; i < str_len; i++) {
          buf[i] = 'a' + Utils::RandomNumber(26);
        }
        record->AddField(new Schema::StringField(buf, str_len));
      }
      // age
      int rand_int = Utils::RandomNumber(20);
      record->AddField(new Schema::IntField(rand_int));
      // signature
      {
        int len_limit = 20;
        int str_len = Utils::RandomNumber(len_limit) + 1;
        char buf[str_len];
        for (int i = 0; i < str_len; i++) {
          buf[i] = 'a' + Utils::RandomNumber(26);
        }
        record->AddField(
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
    MergeSortOptions opts(kDBName, 1 /* txn_id*/, &schema_, key_fields_,
                          {0,2} /* sort by (name, age) */, INDEX, 5);
    //opts.desc = true;
    sorter_ = ptr::MakeUnique<MergeSorter>(opts);
    AssertTrue(sorter_->Init());
  }

  void Test_WriteRead() {
    std::string ms_file_name = sorter_->TempfilePath(0, 0);
    MergeSortTempfileManager ms_file_manager(&sorter_->options(), ms_file_name);
    AssertTrue(ms_file_manager.InitForWriting());

    // Write records to file.
    for (uint32 i = 0; i < record_resource_.size(); i++) {
      AssertTrue(ms_file_manager.WriteRecord(*record_resource_.at(i)));
    }
    ms_file_manager.FinishWriting();

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

  void Test_Sort() {
    std::string result_file = sorter_->Sort(record_resource_);
    AssertTrue(!result_file.empty());

    MergeSortTempfileManager ms_file_manager(&sorter_->options(), result_file);
    AssertTrue(ms_file_manager.InitForReading());

    auto comparator = [&] (std::shared_ptr<RecordBase> r1,
                           std::shared_ptr<RecordBase> r2) {
      return sorter_->SortComparator(r1, r2);
    };
    // // Sort the original record list.
    // std::stable_sort(record_resource_.begin(), record_resource_.end(),
    //                  comparator);

    uint32 total_records = 0;
    std::shared_ptr<RecordBase> last_record;
    while (true) {
      auto next_record = ms_file_manager.NextRecord();
      if (!next_record) {
        break;
      }
      // next_record->Print();

      // Compare with expected record in sorted list.
      if (last_record) {
        AssertFalse(comparator(next_record, last_record));
        last_record = next_record;
      }

      total_records++;
    }
    AssertEqual(record_resource_.size(), total_records);
  }
};

}  // namespace Storage

int main() {
  Storage::MergeSortTest test;
  test.setup();

  //test.Test_WriteRead();
  test.Test_Sort();

  test.teardown();

  std::cout << "\033[2;32mPassed ^_^\033[0m" << std::endl;
  return 0;
}
