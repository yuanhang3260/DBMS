#include "Base/Log.h"
#include "Base/MacroUtils.h"
#include "Base/Path.h"
#include "Base/Ptr.h"
#include "Base/Utils.h"
#include "IO/FileSystemUtils.h"
#include "UnitTest/UnitTest.h"

#include "Storage/FlatTupleFile.h"
#include "Utility/Uuid.h"

namespace Storage {

namespace {
const char* const kDBName = "test_db";
const char* const kPuppyTableName = "Puppy";
const char* const kHostTableName = "Host";
const uint32 kNumRecordsSource = 10000;
}  // namespace

class FlatTupleFileTest: public UnitTest {
 private:
  std::vector<std::shared_ptr<RecordBase>> index_records_;
  std::vector<std::shared_ptr<RecordBase>> data_records_;
  DB::DatabaseCatalog catalog_;

  Query::TupleMeta tuple_meta_;
  std::unique_ptr<FlatTupleFile> ft_file_;

 public:
  void InitSchema() {
    // Create table Puppy.
    auto table_info = catalog_.add_tables();
    table_info->set_name(kPuppyTableName);

    // Add string type
    DB::TableField* field = table_info->add_fields();
    field->set_name("name");
    field->set_index(0);
    field->set_type(DB::TableField::STRING);
    // Add double type
    field = table_info->add_fields();
    field->set_name("weight");
    field->set_index(1);
    field->set_type(DB::TableField::DOUBLE);
    // Add int type
    field = table_info->add_fields();
    field->set_name("age");
    field->set_index(2);
    field->set_type(DB::TableField::INT);
    // Add char array type
    field = table_info->add_fields();
    field->set_name("signature");
    field->set_index(3);
    field->set_type(DB::TableField::CHARARRAY);
    field->set_size(20);

    tuple_meta_[kPuppyTableName].CreateDataRecordMeta(*table_info);

    // Create table Host.
    table_info = catalog_.add_tables();
    table_info->set_name(kHostTableName);

    // Add string type
    field = table_info->add_fields();
    field->set_name("name");
    field->set_index(0);
    field->set_type(DB::TableField::STRING);
    // Add long int type
    field = table_info->add_fields();
    field->set_name("id");
    field->set_index(1);  // primary key
    field->set_type(DB::TableField::LONGINT);

    tuple_meta_[kHostTableName].CreateIndexRecordMeta(*table_info, {0});
  }

  void InitIndexRecords() {
    index_records_.clear();
    for (uint32 i = 0; i < kNumRecordsSource; i++) {
      auto record = std::make_shared<IndexRecord>();
      record->set_rid(RecordID(0, 0));
      index_records_.push_back(record);

      // id.
      //record->AddField(new Schema::LongIntField(i));

      // name.
      int str_len = Utils::RandomNumber(10);
      char buf[str_len];
      for (int i = 0; i < str_len; i++) {
        buf[i] = 'a' + Utils::RandomNumber(26);
      }
      record->AddField(new Schema::StringField(buf, str_len));
    }
  }

  void InitDataRecords() {
    data_records_.clear();
    for (uint32 i = 0; i < kNumRecordsSource; i++) {
      auto record = std::make_shared<DataRecord>();
      data_records_.push_back(record);

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
      // weight
      double rand_double = 0.5 + 1.5 * Utils::RandomFloat();
      record->AddField(new Schema::DoubleField(rand_double));
      // age
      int rand_int = Utils::RandomNumber(9);
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
    InitDataRecords();
    InitIndexRecords();

    FlatTupleFileOptions opts(tuple_meta_, {kPuppyTableName});
    opts.db_name = kDBName;
    opts.txn_id = UUID::TimeStampID();

    ft_file_.reset(new FlatTupleFile(opts));
  }

  void Test_WriteRead() {
    // Write tuples to file.
    for (const auto& record : data_records_) {
      Query::Tuple tuple;
      tuple.AddTableRecord(kPuppyTableName, record);
      AssertTrue(ft_file_->WriteTuple(tuple));
    }
    AssertTrue(ft_file_->FinishWriting());

    // Read records from file and compare with original records.
    uint32 total_tuples = 0;
    auto iterator = ft_file_->GetIterator();
    while (true) {
      auto tuple = iterator.NextTuple();
      if (!tuple) {
        break;
      }
      AssertTrue(*(tuple->GetTableRecord(kPuppyTableName)->record) ==
                 *data_records_.at(total_tuples));
      total_tuples++;
    }
    AssertEqual(data_records_.size(), total_tuples);
  }

  void Test_WriteRead_MultiTableTuples() {
    FlatTupleFileOptions opts(tuple_meta_, {kPuppyTableName, kHostTableName});
    opts.db_name = kDBName;
    opts.txn_id = UUID::TimeStampID();
    ft_file_.reset(new FlatTupleFile(opts));

    // Write tuples to file.
    for (uint32 i = 0; i < kNumRecordsSource; i++) {
      Query::Tuple tuple;
      tuple.AddTableRecord(kPuppyTableName, data_records_.at(i));
      tuple.AddTableRecord(kHostTableName, index_records_.at(i));
      AssertTrue(ft_file_->WriteTuple(tuple));
    }
    AssertTrue(ft_file_->FinishWriting());

    // Read records from file and compare with original records.
    uint32 total_tuples = 0;
    auto iterator = ft_file_->GetIterator();
    while (true) {
      auto tuple = iterator.NextTuple();
      if (!tuple) {
        break;
      }
      AssertTrue(*((tuple->GetTableRecord(kPuppyTableName)->record)) ==
                 *data_records_.at(total_tuples));
      AssertTrue(*((tuple->GetTableRecord(kHostTableName)->record)) ==
                 *index_records_.at(total_tuples));
      total_tuples++;
    }
    AssertEqual(data_records_.size(), total_tuples);
  }

  void Test_Sort() {
    FlatTupleFileOptions opts(tuple_meta_, {kPuppyTableName, kHostTableName});
    opts.db_name = kDBName;
    opts.txn_id = UUID::TimeStampID();
    opts.num_buf_pages = 5;
    ft_file_.reset(new FlatTupleFile(opts));

    // Write tuples to file.
    for (uint32 i = 0; i < kNumRecordsSource; i++) {
      Query::Tuple tuple;
      tuple.AddTableRecord(kPuppyTableName, data_records_.at(i));
      tuple.AddTableRecord(kHostTableName, index_records_.at(i));
      AssertTrue(ft_file_->WriteTuple(tuple));
    }
    AssertTrue(ft_file_->FinishWriting());

    Query::Column column1(kHostTableName, "name");
    column1.index = 0;
    Query::Column column2(kPuppyTableName, "age");
    column2.index = 2;
    AssertTrue(ft_file_->Sort({column1, column2}));

    uint32 total_tuples = 0;
    std::shared_ptr<Query::Tuple> prev_tuple;
    auto iterator = ft_file_->GetIterator();
    while (true) {
      auto tuple = iterator.NextTuple();
      if (!tuple) {
        break;
      }
      total_tuples++;
      if (prev_tuple) {
        AssertTrue(Query::Tuple::CompareBasedOnColumns(
                      *prev_tuple, *tuple, {column1, column2}) <= 0);
      }
      prev_tuple = tuple;
      // tuple->at(kHostTableName).record->Print();
      // tuple->at(kPuppyTableName).record->Print();
      // printf("\n");
    }
    AssertEqual(data_records_.size(), total_tuples);
  }

  void Test_SnapshotRestore() {
    std::cout << __FUNCTION__ << std::endl;

    // Write tuples to file.
    FlatTupleFileOptions opts(tuple_meta_, {kPuppyTableName, kHostTableName});
    opts.db_name = kDBName;
    opts.txn_id = UUID::TimeStampID();
    ft_file_.reset(new FlatTupleFile(opts));

    // Write tuples to file.
    for (uint32 i = 0; i < kNumRecordsSource; i++) {
      Query::Tuple tuple;
      tuple.AddTableRecord(kPuppyTableName, data_records_.at(i));
      tuple.AddTableRecord(kHostTableName, index_records_.at(i));
      //tuple.Print();
      AssertTrue(ft_file_->WriteTuple(tuple));
    }
    AssertTrue(ft_file_->FinishWriting());
    //printf("\n");

    // Begin random reading.
    for (uint32 times = 0; times < 50; times++) {
      // Pick up a random tuple.
      int32 begin_tuple_index = Utils::RandomNumber(kNumRecordsSource);
      FlatTupleFile::Iterator iterator = ft_file_->GetIterator();
      FlatTupleFile::Iterator iterator_1;
      std::shared_ptr<Query::Tuple> tuple_1;
      for (int32 i = 0; i <= begin_tuple_index; i++) {
        iterator_1 = iterator;
        tuple_1 = iterator.NextTuple();
        AssertTrue(tuple_1.get());
      }

      // Go to another random tuple.
      int32 end_tuple_index = begin_tuple_index +
          Utils::RandomNumber(kNumRecordsSource - begin_tuple_index);
      //printf("%d, %d\n", begin_tuple_index, end_tuple_index);
      FlatTupleFile::Iterator iterator_2 = iterator_1;
      std::shared_ptr<Query::Tuple> tuple_2 = tuple_1;
      for (int32 i = begin_tuple_index; i < end_tuple_index; i++) {
        iterator_2 = iterator;
        tuple_2 = iterator.NextTuple();
        AssertTrue(tuple_2.get());
      }

      // Restore snapshots, repeating 3 times.
      for (int j = 0; j < 3; j++) {
        // Restore to first tuple position and re-read.
        iterator = iterator_1;
        auto new_tuple_1 = iterator.NextTuple();
        AssertTrue(new_tuple_1.get());
        AssertTrue(*(tuple_1->GetTableRecord(kPuppyTableName)->record) ==
                   *(new_tuple_1->GetTableRecord(kPuppyTableName)->record));
        AssertTrue(*(tuple_1->GetTableRecord(kHostTableName)->record) ==
                   *(new_tuple_1->GetTableRecord(kHostTableName)->record));

        // Restore to second tuple position and re-read.
        iterator = iterator_2;
        auto new_tuple_2 = iterator.NextTuple();
        AssertTrue(new_tuple_2.get());
        AssertTrue(*(tuple_2->GetTableRecord(kPuppyTableName)->record) ==
                   *(new_tuple_2->GetTableRecord(kPuppyTableName)->record));
        AssertTrue(*(tuple_2->GetTableRecord(kHostTableName)->record) ==
                   *(new_tuple_2->GetTableRecord(kHostTableName)->record));

        // Go back to first tuple. This time iterate one by one until reaching
        // the second tuple.
        iterator = iterator_1;
        new_tuple_1 = iterator.NextTuple();
        AssertTrue(new_tuple_1.get());
        for (int k = 0; k < (end_tuple_index - begin_tuple_index); k++) {
          new_tuple_2 = iterator.NextTuple();
        }
        AssertTrue(new_tuple_2.get());
        AssertTrue(*(tuple_2->GetTableRecord(kPuppyTableName)->record) ==
                   *(new_tuple_2->GetTableRecord(kPuppyTableName)->record));
        AssertTrue(*(tuple_2->GetTableRecord(kHostTableName)->record) ==
                   *(new_tuple_2->GetTableRecord(kHostTableName)->record));
      }
    }
  }
};

}  // namespace Storage

int main() {
  Storage::FlatTupleFileTest test;
  test.setup();

  test.Test_WriteRead();
  test.Test_WriteRead_MultiTableTuples();
  test.Test_Sort();
  test.Test_SnapshotRestore();

  test.teardown();

  std::cout << "\033[2;32mPassed ^_^\033[0m" << std::endl;
  return 0;
}
