#include "Base/Log.h"
#include "Base/MacroUtils.h"
#include "UnitTest/UnitTest.h"

#include "Query/Common.h"

namespace Query {

namespace {

using Storage::RecordBase;
using Storage::RecordID;
using Storage::DataRecord;
using Storage::IndexRecord;

const char* const kTableName = "Puppy";
const char* const kDBName = "testDB";
const int kNumRecordsSource = 100;

}  // namespace

class CommonTest: public UnitTest {
 private:
  std::vector<std::shared_ptr<RecordBase>> data_records_;
  std::vector<std::shared_ptr<RecordBase>> index_record_;
  std::vector<uint32> key_fields_ = std::vector<uint32>{1, 2};
 
 public:
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
    InitRecordResource(&data_records_);
  }

  void Test_SortByColumn() {
    FetchedResult result;
    result.tuple_meta.emplace(kTableName, TableRecordMeta()); 
    for (const auto& record : data_records_) {
      auto tuple = FetchedResult::Tuple();
      tuple.emplace(kTableName, ResultRecord(record));
      result.AddTuple(std::move(tuple));
    }

    result.SortByColumns(kTableName, key_fields_);

    for (const auto& tuple : result.tuples) {
      tuple.at(kTableName).record->Print();
    }
  }

  void Test_MergeSortResultsRemoveDup() {
    std::vector<std::shared_ptr<RecordBase>> data_records_1;
    InitRecordResource(&data_records_1);
    FetchedResult result1;
    result1.tuple_meta.emplace(kTableName, TableRecordMeta());
    for (const auto& record : data_records_1) {
      auto tuple = FetchedResult::Tuple();
      tuple.emplace(kTableName, ResultRecord(record));
      result1.AddTuple(std::move(tuple));
    }

    std::vector<std::shared_ptr<RecordBase>> data_records_2;
    InitRecordResource(&data_records_2);
    FetchedResult result2;
    result2.tuple_meta.emplace(kTableName, TableRecordMeta());
    for (const auto& record : data_records_2) {
      auto tuple = FetchedResult::Tuple();
      tuple.emplace(kTableName, ResultRecord(record));
      result2.AddTuple(std::move(tuple));
    }

    FetchedResult result;
    result.MergeSortResultsRemoveDup(result1, result2, kTableName, {1});
    AssertGreaterEqual(7, result.tuples.size());

    // for (const auto& tuple : result.tuples) {
    //   tuple.at(kTableName).record->Print();
    // }
  }
};

}  // namespace Query


int main() {
  Query::CommonTest test;
  test.setup();

  //test.Test_SortByColumn();
  test.Test_MergeSortResultsRemoveDup();

  test.teardown();

  std::cout << "\033[2;32mPassed ^_^\033[0m" << std::endl;
  return 0;
}
