#include <vector>
#include <map>

#include "Base/Log.h"
#include "Base/Utils.h"
#include "IO/FileSystemUtils.h"
#include "Strings/Utils.h"
#include "UnitTest/UnitTest.h"

#include "Database/Table.h"
#include "Storage/PageRecord_Common.h"

namespace DB {

namespace {

using Storage::RecordBase;
using Storage::DataRecord;
using Storage::IndexRecord;
using Storage::RecordID;
using Storage::FileType;
using Storage::BplusTree;

const char* const kDBName = "test_db";
const char* const kTableName = "Puppy";
const int kNumRecordsSource = 100;
}

class TableTest: public UnitTest {
 private:
  std::vector<int> key_indexes_;
  TableInfo schema_;
  std::shared_ptr<TableInfoManager> table_m;
  std::shared_ptr<Table> table_;
  std::vector<std::shared_ptr<RecordBase>> puppy_records_;

 public:
  void InitSchema() {
    // Create a table schema_.
    schema_.set_name(kTableName);

    // Add string type
    TableField* field = schema_.add_fields();
    field->set_name("name");
    field->set_index(0);
    field->set_type(TableField::STRING);
    // Add int type
    field = schema_.add_fields();
    field->set_name("age");
    field->set_index(1);
    field->set_type(TableField::INT);
    // Add long int type
    field = schema_.add_fields();
    field->set_name("id");
    field->set_index(2);  // primary key
    field->set_type(TableField::LONGINT);
    // Add double type
    field = schema_.add_fields();
    field->set_name("weight");
    field->set_index(3);
    field->set_type(TableField::DOUBLE);
    // Add bool type
    field = schema_.add_fields();
    field->set_name("adult");
    field->set_index(4);
    field->set_type(TableField::BOOL);
    // Add char array type
    field = schema_.add_fields();
    field->set_name("signature");
    field->set_index(5);
    field->set_type(TableField::CHARARRAY);
    field->set_size(20);

    auto* index = schema_.mutable_primary_index();
    index->add_index_fields(2);
    // Create key_indexes.
    for (auto i: schema_.primary_index().index_fields()) {
      key_indexes_.push_back(i);
    }

    index = schema_.add_indexes();
    index->add_index_fields(0);
    index = schema_.add_indexes();
    index->add_index_fields(1);
    index = schema_.add_indexes();
    index->add_index_fields(3);
    index = schema_.add_indexes();
    index->add_index_fields(4);
    index = schema_.add_indexes();
    index->add_index_fields(5);

    table_m.reset(new TableInfoManager(&schema_));
    AssertTrue(table_m->Init());
  }

  void InitRecordResource() {
    puppy_records_.clear();
    for (int i = 0; i < kNumRecordsSource; i++) {
      puppy_records_.push_back(std::shared_ptr<RecordBase>(new DataRecord()));

      // Init fields to records.
      // name
      {
        int str_len = Utils::RandomNumber(5);
        char buf[str_len];
        for (int i = 0; i < str_len; i++) {
          buf[i] = 'a' + Utils::RandomNumber(26);
        }
        puppy_records_.at(i)->AddField(new Schema::StringField(buf, str_len));
      }
      // age: 0 ~ 6
      int rand_int = Utils::RandomNumber(7);
      puppy_records_.at(i)->AddField(new Schema::IntField(rand_int));
      // id: 0 ~ (kNumRecordsSource - 1)
      puppy_records_.at(i)->AddField(new Schema::LongIntField(i));
      // weight: 1.0 ~ 2.0
      double rand_double = 1.0 + 1.0 * Utils::RandomFloat();
      puppy_records_.at(i)->AddField(new Schema::DoubleField(rand_double));
      // adult
      bool rand_bool = Utils::RandomNumber() % 2 == 1 ? true : false;
      puppy_records_.at(i)->AddField(new Schema::BoolField(rand_bool));
      // signature
      {
        int len_limit = 20;
        int str_len = Utils::RandomNumber(len_limit) + 1;
        char buf[str_len];
        for (int i = 0; i < str_len; i++) {
          buf[i] = 'a' + Utils::RandomNumber(26);
        }
        puppy_records_.at(i)->AddField(
            new Schema::CharArrayField(buf, str_len, len_limit));
      }
    }
  }

  bool CreateDBDirectory() {
    std::string db_dir = Strings::StrCat(Storage::kDataDirectory, kDBName);
    return FileSystem::CreateDir(db_dir);
  }

  void setup() override {
    InitSchema();
    CreateDBDirectory();
    InitRecordResource();

    table_.reset(new Table(kDBName, kTableName, table_m.get()));
    LoadData();
  }

  void LoadData() {
    table_->PreLoadData(puppy_records_);
    AssertTrue(table_->ValidateAllIndexRecords(puppy_records_.size()));

    for (const auto& index: table_m->table_info().indexes()) {
      std::vector<int> key_index = TableInfoManager::MakeIndex(index);
      printf("Checking tree %s\n", Table::IndexStr(key_index).c_str());
      auto file_type = table_->IsDataFileKey(key_index) ?
                           Storage::INDEX_DATA : Storage::INDEX;
      auto tree = table_->Tree(file_type, key_index);
      tree->SaveToDisk();

      AssertTrue(tree->ValidityCheck(), "Check Index tree failed");
    }
    printf("Checking tree %s\n",Table::IndexStr(table_->DataTreeKey()).c_str());
    AssertTrue(table_->DataTree()->ValidityCheck(), "Check Data tree failed");
  }

  void PrintRecords(
      const std::vector<std::shared_ptr<Storage::RecordBase>>& records) {
    for (const auto& record : records) {
      record->Print();
    }
  }

  uint32 NumExpectedMatches(const SearchOp& op) {
    int expected_matches = 0;
    for (const auto& record : puppy_records_) {
      if (RecordBase::CompareRecordWithKey(*op.key, *record,
                                           op.field_indexes) == 0) {
        expected_matches++;
      }
    }
    return expected_matches;
  }

  uint32 NumExpectedMatches(const RangeSearchOp& op) {
    int expected_matches = 0;
    for (const auto& record : puppy_records_) {
      bool left_match = false, right_match = false;
      if (op.left_key) {
        int re = RecordBase::CompareRecordWithKey(*op.left_key, *record,
                                                  op.field_indexes);
        if ((re < 0 && op.left_open) || (re <= 0 && !op.left_open)) {
          left_match = true;
        }
      } else {
        left_match = true;
      }

      if (op.right_key) {
        int re = RecordBase::CompareRecordWithKey(*op.right_key, *record,
                                                  op.field_indexes);
        if ((re > 0 && op.right_open) || (re >= 0 && !op.right_open)) {
          right_match = true;
        }
      } else {
        right_match = true;
      }

      if (left_match && right_match) {
        expected_matches++;
      }
    }
    return expected_matches;
  }

  void Test_ScanRecords() {
    std::vector<std::shared_ptr<Storage::RecordBase>> result;
    AssertEqual(kNumRecordsSource, table_->ScanRecords(&result));

    result.clear();
    AssertEqual(kNumRecordsSource, table_->ScanRecords(&result, {3}));
  }

  void Test_SearchRecords() {
    std::vector<std::shared_ptr<Storage::RecordBase>> result;
    SearchOp op;

    // Search by id.
    for (uint32 id = 0; id < puppy_records_.size(); id++) {
      result.clear();
      op.reset();
      op.field_indexes.push_back(2);
      op.key->AddField(new Schema::LongIntField(id));
      table_->SearchRecords(op, &result);
      AssertEqual(NumExpectedMatches(op), result.size());
      AssertEqual(1, result.size());
    }

    // Search by age.
    result.clear();
    op.reset();
    op.field_indexes.push_back(1);
    op.key->AddField(new Schema::IntField(5));

    table_->SearchRecords(op, &result);
    AssertEqual(NumExpectedMatches(op), result.size());

    // Search by adult
    result.clear();
    op.reset();
    op.field_indexes.push_back(4);
    op.key->AddField(new Schema::BoolField(true));

    table_->SearchRecords(op, &result);
    AssertEqual(NumExpectedMatches(op), result.size());
  }

  void Test_RangeSearchRecords() {
    std::vector<std::shared_ptr<Storage::RecordBase>> result;
    RangeSearchOp op;

    // Search by: id in [10, 20)
    op.reset();
    result.clear();
    op.field_indexes.push_back(2);
    op.left_key->AddField(new Schema::LongIntField(kNumRecordsSource/3));
    op.left_open = false;
    op.right_key->AddField(new Schema::LongIntField(kNumRecordsSource/3 * 2));
    op.right_open = false;

    table_->RangeSearchRecords(op, &result);
    AssertEqual(NumExpectedMatches(op), result.size());
    printf("Matched records = %d\n", result.size());

    // Search by: name >= "m"
    op.reset();
    result.clear();
    op.field_indexes.push_back(0);
    op.left_key->AddField(new Schema::StringField("m"));
    op.left_open = false;
    op.right_key.reset();
    table_->RangeSearchRecords(op, &result);
    AssertEqual(NumExpectedMatches(op), result.size());
    printf("Matched records = %d\n", result.size());

    // Search by: weight < 1.6
    op.reset();
    result.clear();
    op.field_indexes.push_back(3);
    op.left_key.reset();
    op.right_key->AddField(new Schema::DoubleField(1.6));
    op.right_open = true;
    table_->RangeSearchRecords(op, &result);
    AssertEqual(NumExpectedMatches(op), result.size());
    printf("Matched records = %d\n", result.size());

    // Search by: 1 < age <= 1
    op.reset();
    result.clear();
    op.field_indexes.push_back(1);
    op.left_key->AddField(new Schema::IntField(1));
    op.left_open = true;
    op.right_key->AddField(new Schema::IntField(1));
    op.right_open = false;
    table_->RangeSearchRecords(op, &result);
    AssertEqual(NumExpectedMatches(op), result.size());
    AssertEqual(0, result.size());
    printf("Matched records = %d\n", result.size());
  }
};

}  // namespace Schema

int main() {
  DB::TableTest test;
  test.setup();

  //test.Test_SearchRecords();
  //test.Test_ScanRecords();
  test.Test_RangeSearchRecords();

  test.teardown();

  std::cout << "\033[2;32mPassed ^_^\033[0m" << std::endl;
  return 0;
}