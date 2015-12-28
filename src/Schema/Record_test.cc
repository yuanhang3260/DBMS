#include <stdexcept>

#include "UnitTest/UnitTest.h"
#include "Base/Utils.h"
#include "Base/Log.h"
#include "Record.h"
#include "DataTypes.h"

namespace Schema {

class RecordTest: public UnitTest {
 public:
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
    // Dump
    DataRecord key1;
    key1.AddField(new IntType(5));  // 4
    key1.AddField(new StringType("abc"));  // 4
    key1.AddField(new LongIntType(1111111111111));  // 8
    key1.AddField(new StringType(""));  // 1
    key1.AddField(new DoubleType(3.5));  // 8
    key1.AddField(new CharArrayType("wxyz", 4, 4));  // 5
    key1.AddField(new BoolType(false));  // 1
    key1.AddField(new CharArrayType("####", 0, 5));  // 1

    byte* buf = new byte[128];
    AssertEqual(31, key1.DumpToMem(buf), "Dump size error");

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
    AssertEqual(31, key2.LoadFromMem(buf), "Load size error");
    AssertTrue(key1 == key2);

    DataRecord key3;
    key3.AddField(new IntType());
    key3.AddField(new StringType());
    key3.AddField(new LongIntType());
    key3.AddField(new StringType());
    key3.AddField(new DoubleType());
    key3.AddField(new CharArrayType(11));
    key3.AddField(new BoolType());
    key3.AddField(new CharArrayType(7));
    AssertTrue(key3 < key1);

    DataRecord key4;
    AssertTrue(key4 < key1);

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
};

}  // namespace Schema

int main() {
  Schema::RecordTest test;
  test.setup();
  test.Test_Record_Operators();
  test.Test_Record_LoadDump();
  test.Test_SortRecords();
  test.teardown();

  std::cout << "\033[2;32mPassed ^_^\033[0m" << std::endl;
  return 0;
}