#include "Base/Log.h"
#include "Base/MacroUtils.h"
#include "UnitTest/UnitTest.h"

#include "Database/Catalog_pb.h"
#include "Query/NodeValue.h"
#include "Query/Result.h"

namespace Query {

namespace {

using Storage::RecordBase;
using Storage::RecordID;
using Storage::DataRecord;
using Storage::IndexRecord;

const char* const kTableName = "Puppy";
const char* const kDBName = "testDB";
const int kNumRecordsSource = 10;

}  // namespace

class CommonTest: public UnitTest {
 private:
  std::vector<std::shared_ptr<RecordBase>> data_records_;
  std::vector<std::shared_ptr<RecordBase>> index_record_;
  std::vector<uint32> key_fields_ = std::vector<uint32>{1, 2};
  std::map<std::string, TableRecordMeta> tuple_meta_;

  DB::DatabaseCatalog catalog_;
 
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
    // Add int type
    field = table_info->add_fields();
    field->set_name("age");
    field->set_index(1);
    field->set_type(DB::TableField::INT);
    // Add long int type
    field = table_info->add_fields();
    field->set_name("id");
    field->set_index(2);  // primary key
    field->set_type(DB::TableField::LONGINT);
    // Add double type
    field = table_info->add_fields();
    field->set_name("weight");
    field->set_index(3);
    field->set_type(DB::TableField::DOUBLE);
    // Add bool type
    field = table_info->add_fields();
    field->set_name("adult");
    field->set_index(4);
    field->set_type(DB::TableField::BOOL);
    // Add char array type
    field = table_info->add_fields();
    field->set_name("signature");
    field->set_index(5);
    field->set_type(DB::TableField::CHARARRAY);
    field->set_size(20);
  }

  void InitRecordResource(std::vector<std::shared_ptr<RecordBase>>* records) {
    records->clear();
    for (int i = 0; i < kNumRecordsSource; i++) {
      records->push_back(std::shared_ptr<RecordBase>(new DataRecord()));

      // Init fields to records.
      // name
      {
        int str_len = Utils::RandomNumber(5);
        char buf[str_len];
        for (int i = 0; i < str_len; i++) {
          buf[i] = 'a' + Utils::RandomNumber(26);
        }
        records->back()->AddField(new Schema::StringField(buf, str_len));
      }
      // age: 0 ~ 6
      int rand_int = Utils::RandomNumber(7);
      records->back()->AddField(new Schema::IntField(rand_int));
      // id: 0 ~ (kNumRecordsSource - 1)
      records->back()->AddField(new Schema::LongIntField(i));
      // weight: 1.0 ~ 2.0
      double rand_double = 1.0 + 1.0 * Utils::RandomFloat();
      records->back()->AddField(new Schema::DoubleField(rand_double));
      // adult
      bool rand_bool = Utils::RandomNumber() % 2 == 1 ? true : false;
      records->back()->AddField(new Schema::BoolField(rand_bool));
      // signature
      {
        int len_limit = 20;
        int str_len = Utils::RandomNumber(len_limit) + 1;
        char buf[str_len];
        for (int i = 0; i < str_len; i++) {
          buf[i] = 'a' + Utils::RandomNumber(26);
        }
        records->back()->AddField(
            new Schema::CharArrayField(buf, str_len, len_limit));
      }
    }
  }

  void setup() {
    InitCatalog();
    InitRecordResource(&data_records_);

    tuple_meta_.emplace(kTableName, TableRecordMeta());
    tuple_meta_.at(kTableName).CreateDataRecordMeta(catalog_.tables(0));
  }

  void Test_SortByColumn() {
    std::cout << __FUNCTION__ << std::endl;

    ResultContainer result;
    result.SetTupleMeta(&tuple_meta_);
    for (const auto& record : data_records_) {
      auto tuple = Tuple();
      AssertTrue(tuple.AddTableRecord(kTableName, record));
      result.AddTuple(tuple);
    }
    AssertTrue(result.FinalizeAdding());

    result.SortByColumns(kTableName, key_fields_);

    std::vector<Column> sort_columns;
    for (uint32 index : key_fields_) {
      sort_columns.emplace_back(kTableName, "");
      sort_columns.back().index = index;
    }

    auto iterator = result.GetIterator();
    uint32 counter = 0;
    std::shared_ptr<Tuple> prev_tuple;
    while (true) {
      auto tuple = iterator.GetNextTuple();
      if (!tuple) {
        break;
      }
      counter++;
      if (prev_tuple) {
        AssertTrue(
          Tuple::CompareBasedOnColumns(*prev_tuple, *tuple, sort_columns) <= 0);
      }
      prev_tuple = tuple;
      //tuple->GetTableRecord(kTableName)->record->Print();
    }
    AssertEqual(kNumRecordsSource, counter);
    printf("\n");
  }

  void Test_MergeSortResultsRemoveDup() {
    std::cout << __FUNCTION__ << std::endl;

    std::vector<std::shared_ptr<RecordBase>> data_records_1;
    InitRecordResource(&data_records_1);
    ResultContainer result1;
    result1.SetTupleMeta(&tuple_meta_);
    for (const auto& record : data_records_1) {
      auto tuple = Tuple();
      AssertTrue(tuple.AddTableRecord(kTableName, record));
      result1.AddTuple(tuple);
    }
    AssertTrue(result1.FinalizeAdding());

    std::vector<std::shared_ptr<RecordBase>> data_records_2;
    InitRecordResource(&data_records_2);
    ResultContainer result2;
    result2.SetTupleMeta(&tuple_meta_);
    for (const auto& record : data_records_2) {
      auto tuple = Tuple();
      AssertTrue(tuple.AddTableRecord(kTableName, record));
      result2.AddTuple(tuple);
    }
    AssertTrue(result2.FinalizeAdding());

    ResultContainer result;
    result.SetTupleMeta(&tuple_meta_);
    result.MergeSortResultsRemoveDup(result1, result2, kTableName, {1});
    AssertGreaterEqual(7, result.NumTuples());

    auto iterator = result.GetIterator();
    while (true) {
      auto tuple = iterator.GetNextTuple();
      if (!tuple) {
        break;
      }
      tuple->GetTableRecord(kTableName)->record->Print();
    }
    printf("\n");
  }
};

}  // namespace Query


int main() {
  Query::CommonTest test;
  test.setup();

  test.Test_SortByColumn();
  test.Test_MergeSortResultsRemoveDup();

  test.teardown();

  std::cout << "\033[2;32mPassed ^_^\033[0m" << std::endl;
  return 0;
}
