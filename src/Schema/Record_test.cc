#include <map>
#include <stdexcept>

#include "UnitTest/UnitTest.h"
#include "Base/Utils.h"
#include "Base/Log.h"
#include "Storage/Common.h"
#include "Record.h"
#include "DataTypes.h"

namespace Schema {

class RecordTest: public UnitTest {
 private:
  std::map<int, std::shared_ptr<DataRecord>> record_resource;
  std::map<int, std::shared_ptr<IndexRecord>> indexrecord_resource;
  std::map<int, std::shared_ptr<TreeNodeRecord>> treenoderecord_resource;
  const int kNumRecordsSource = 1000;
  TableSchema schema;
  std::vector<int> key_indexes = std::vector<int>{1, 0};

 public:
  enum RecordType {
    DATARECORD,
    INDEXRECORD,
    TREENODERECORD,
  };

  void setup() {
    InitSchema();
    InitRecordResource();
  }

  void InitSchema() {
    // Create a table schema.
    schema.set_name("testTable");
    // Add string type
    Schema::TableField* field = schema.add_fields();
    field->set_name("name");
    field->set_index(0);
    field->set_type(Schema::TableField::STRING);
    // Add int type
    field = schema.add_fields();
    field->set_name("age");
    field->set_index(1);
    field->set_type(Schema::TableField::INTEGER);
    // Add long int type
    field = schema.add_fields();
    field->set_name("money");
    field->set_index(2);
    field->set_type(Schema::TableField::LLONG);
    // Add double type
    field = schema.add_fields();
    field->set_name("weight");
    field->set_index(3);
    field->set_type(Schema::TableField::DOUBLE);
    // Add bool type
    field = schema.add_fields();
    field->set_name("adult");
    field->set_index(4);
    field->set_type(Schema::TableField::BOOL);
    // Add char array type
    field = schema.add_fields();
    field->set_name("signature");
    field->set_index(5);
    field->set_type(Schema::TableField::CHARARR);
    field->set_size(20);
  }

  void InitRecordResource() {
    record_resource.clear();
    for (int i = 0; i < kNumRecordsSource; i++) {
      record_resource.emplace(i, std::make_shared<DataRecord>());

      // Init fields to records.
      // name
      {
        int str_len = Utils::RandomNumber(10);
        char buf[str_len];
        for (int i = 0; i < str_len; i++) {
          buf[i] = 'a' + Utils::RandomNumber(26);
        }
        record_resource.at(i)->AddField(new StringType(buf, str_len));
      }
      // age
      int rand_int = Utils::RandomNumber(20);
      record_resource.at(i)->AddField(new IntType(rand_int));
      // money (we use this field as key for record resource map).
      record_resource.at(i)->AddField(new LongIntType(i));
      // weight
      double rand_double = 1.0 * Utils::RandomNumber() / Utils::RandomNumber();
      record_resource.at(i)->AddField(new DoubleType(rand_double));
      // adult
      bool rand_bool = Utils::RandomNumber() % 2 == 1 ? true : false;
      record_resource.at(i)->AddField(new BoolType(rand_bool));
      // signature
      {
        int len_limit = 20;
        int str_len = Utils::RandomNumber(len_limit) + 1;
        char buf[str_len];
        for (int i = 0; i < str_len; i++) {
          buf[i] = 'a' + Utils::RandomNumber(26);
        }
        record_resource.at(i)->AddField(
            new CharArrayType(buf, str_len, len_limit));
      }
    }
  }

  void GenerateBplusTreePageRecord(RecordType type) {
    if (record_resource.empty()) {
      return;
    }
    if (type == INDEXRECORD) {
      for (int i = 0; i < kNumRecordsSource; i++) {
        IndexRecord* record = new IndexRecord();
        record->set_rid(RecordID(Utils::RandomNumber(1000),
                                 Utils::RandomNumber(1000)));
        record_resource.at(i)->ExtractKey(record, key_indexes);
        indexrecord_resource.emplace(i, std::shared_ptr<IndexRecord>(record));
      }
    }
    if (type == TREENODERECORD) {
      for (int i = 0; i < kNumRecordsSource; i++) {
        TreeNodeRecord* record = new TreeNodeRecord();
        record->set_page_id(Utils::RandomNumber(1000));
        record_resource.at(i)->ExtractKey(record, key_indexes);
        treenoderecord_resource.emplace(
            i, std::shared_ptr<TreeNodeRecord>(record));
      }
    }
  }

  void Test_Record_Operators() {
    DataRecord key1;
    key1.AddField(new IntType(5));
    key1.AddField(new LongIntType(1111111111111));
    key1.AddField(new DoubleType(3.5));
    key1.AddField(new BoolType(false));
    key1.AddField(new StringType("abc"));
    key1.AddField(new CharArrayType("acd", 3, 10));
    
    DataRecord key2;
    key2.AddField(new IntType(5));
    key2.AddField(new LongIntType(1111111111111));
    key2.AddField(new DoubleType(3.5));
    key2.AddField(new BoolType(false));
    key2.AddField(new StringType("abc"));
    key2.AddField(new CharArrayType("acd", 3, 10));

    AssertTrue(key1 == key2);
    auto& fields = key2.fields();

    // change int key field.
    (reinterpret_cast<IntType*>(fields[0].get()))->set_value(10);
    AssertTrue(key1 < key2, "int");
    (reinterpret_cast<IntType*>(fields[0].get()))->set_value(5);

    // change longint key field.
    (reinterpret_cast<LongIntType*>(fields[1].get()))->set_value(1111111111112);
    AssertTrue(key1 < key2, "long int");
    (reinterpret_cast<LongIntType*>(fields[1].get()))->set_value(1111111111111);

    // change double key field.
    (reinterpret_cast<DoubleType*>(fields[2].get()))->set_value(3.2);
    AssertTrue(key1 > key2, "double");
    (reinterpret_cast<DoubleType*>(fields[2].get()))->set_value(3.5);

    // change bool key field.
    (reinterpret_cast<BoolType*>(fields[3].get()))->set_value(true);
    AssertTrue(key1 < key2, "bool");
    (reinterpret_cast<BoolType*>(fields[3].get()))->set_value(false);

    // change string key field.
    (reinterpret_cast<StringType*>(fields[4].get()))->set_value("aabc");
    AssertTrue(key1 > key2, "string");
    (reinterpret_cast<StringType*>(fields[4].get()))->set_value("abc");

    // change CharArray key field.
    (reinterpret_cast<CharArrayType*>(fields[4].get()))->SetData("abcd", 3);
    AssertTrue(key1 == key2, "CharArray");
    (reinterpret_cast<CharArrayType*>(fields[4].get()))->SetData("abc", 3);

    fields.pop_back();
    AssertTrue(key1 > key2, "length diff");

    key1.fields().pop_back();
    AssertTrue(key1 == key2, "length same");

    key1.AddField(new IntType(5));
    key2.AddField(new LongIntType(5));
    AssertTrue(key1 < key2, "type diff");

    AssertEqual(6, key1.NumFields());
    AssertEqual(6, key2.NumFields());
  }

  void Test_Record_LoadDump() {
    std::cout << __FUNCTION__ << std::endl;
    // Dump
    DataRecord key1;
    key1.AddField(new IntType(5));  // 4
    key1.AddField(new StringType("abc"));  // 4
    key1.AddField(new LongIntType(1111111111111));  // 8
    key1.AddField(new StringType(""));  // 1
    key1.AddField(new DoubleType(3.5));  // 8
    key1.AddField(new CharArrayType("wxyz", 4, 4));  // 4
    key1.AddField(new BoolType(false));  // 1
    key1.AddField(new CharArrayType("####", 0, 5));  // 5

    byte* buf = new byte[128];
    AssertEqual(35, key1.DumpToMem(buf), "Dump size error");

    // Load
    DataRecord key2;
    key2.AddField(new IntType());
    key2.AddField(new StringType());
    key2.AddField(new LongIntType());
    key2.AddField(new StringType());
    key2.AddField(new DoubleType());
    key2.AddField(new CharArrayType(4));
    key2.AddField(new BoolType());
    key2.AddField(new CharArrayType(5));
    AssertEqual(35, key2.LoadFromMem(buf), "Load size error");
    AssertTrue(key1 == key2, "key1 != key2");

    DataRecord key3;
    key3.AddField(new IntType());
    key3.AddField(new StringType());
    key3.AddField(new LongIntType());
    key3.AddField(new StringType());
    key3.AddField(new DoubleType());
    key3.AddField(new CharArrayType(11));
    key3.AddField(new BoolType());
    key3.AddField(new CharArrayType(7));
    AssertTrue(key3 < key1,  "key3 >= key1");

    DataRecord key4;
    AssertTrue(key4 < key1, "key4 >= key1");

    delete[] buf;
  }

  void Test_SortRecords() {
    std::vector<std::shared_ptr<Schema::RecordBase>> records;
    for (int i = 0; i < 10; i++) {
      records.push_back(std::make_shared<Schema::IndexRecord>());
      records[i]->AddField(new Schema::IntType(10 - i / 5));
      records[i]->AddField(new Schema::LongIntType(1111111111111));
      records[i]->AddField(new Schema::DoubleType(3.5));
      records[i]->AddField(new Schema::BoolType(false));
      if (i % 2 == 0) {
        records[i]->AddField(new Schema::StringType("axy"));  
      }
      else {
        records[i]->AddField(new Schema::StringType("abc"));
      }
      records[i]->AddField(new Schema::CharArrayType("acd", 3, 10));
      records[i]->Print();
    }

    PageRecordsManager::SortRecords(records, std::vector<int>{4, 0});
    std::cout << "After sorting:" << std::endl;
    for (int i = 0; i < 10; i++) {
      records[i]->Print();
    }
  }

  void Test_PageRecords() {
    // Create a record page.
    DataBaseFiles::RecordPage page(1);
    page.InitInMemoryPage();
    PageRecordsManager prmanager(&page, &schema, key_indexes,
                                 DataBaseFiles::INDEX_DATA,
                                 DataBaseFiles::TREE_LEAVE);

    for (int iteration = 0; iteration < 10000; iteration++) {
      // Insert records to it.
      auto list = Utils::RandomListFromRange(0, kNumRecordsSource - 1);
      int i = 0;
      while (1) {
        int re = prmanager.InsertRecordToPage(
                               record_resource.at(list[i]).get());
        if (!re) {
          // LogINFO("Inserted %d records", i);
          // LogINFO("Space used %d bytes",
          //         DataBaseFiles::kPageSize - page.FreeSize());
          break;
        }
        i++;
      }

      // Let's delete some of them.
      const auto& slot_directory = page.Meta()->slot_directory();
      auto delete_list = Utils::RandomListFromRange(0, slot_directory.size()-1,
                                                    slot_directory.size() / 3);
      for (int delete_slot: delete_list) {
        page.DeleteRecord(delete_slot);
      }
      LogINFO("Randomly deleted %d records", delete_list.size());

      // Use PageRecordsManager to load all remaining records.
      AssertTrue(prmanager.LoadRecordsFromPage(), "Load page record failed");
      AssertTrue(prmanager.CheckSort(), "CheckSort failed");

      const auto& all_records = prmanager.plrecords();
      for (const auto& plrecord: all_records) {
        // Verify content of records.
        int index = reinterpret_cast<LongIntType*>(
                        plrecord.record()->fields()[2].get())
                            ->value();
        AssertTrue(*(plrecord.record()) ==
                   *reinterpret_cast<RecordBase*>(
                        record_resource.at(index).get()));
        //plrecord.Print();
      }
    }
  }

  void Test_ExtractKey() {
    GenerateBplusTreePageRecord(INDEXRECORD);
    GenerateBplusTreePageRecord(TREENODERECORD);
    // for (const auto& entry: indexrecord_resource) {
    //   entry.second->Print();
    // }
    // for (const auto& entry: treenoderecord_resource) {
    //   entry.second->Print();
    // }
  }
};

}  // namespace Schema

int main() {
  Schema::RecordTest test;
  test.setup();
  test.Test_Record_Operators();
  test.Test_Record_LoadDump();
  test.Test_SortRecords();
  test.Test_PageRecords();
  test.Test_ExtractKey();
  test.teardown();

  std::cout << "\033[2;32mPassed ^_^\033[0m" << std::endl;
  return 0;
}