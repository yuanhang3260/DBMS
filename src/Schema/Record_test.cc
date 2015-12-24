#include <stdexcept>

#include "UnitTest/UnitTest.h"
#include "Base/Utils.h"
#include "Record.h"

namespace Schema {

class RecordTest: public UnitTest {
 public:
  void Test_Record_Operators() {
    Record key1;
    key1.AddField(new IntType(5));
    key1.AddField(new LongIntType(1111111111111));
    key1.AddField(new DoubleType(3.5));
    key1.AddField(new BoolType(false));
    key1.AddField(new StringType("abc"));
    key1.AddField(new CharArrayType("acd", 3, 10));
    
    Record key2;
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
    Record key1;
    key1.AddField(new IntType(5));  // 4
    key1.AddField(new StringType("abc"));  // 4
    key1.AddField(new LongIntType(1111111111111));  // 8
    key1.AddField(new StringType(""));  // 1
    key1.AddField(new DoubleType(3.5));  // 8
    key1.AddField(new CharArrayType("wxyz", 4, 10));  // 5
    key1.AddField(new BoolType(false));  // 1
    key1.AddField(new CharArrayType("####", 0, 5));  // 1

    byte* buf = new byte[128];
    AssertEqual(32, key1.DumpToMem(buf), "Dump size error");

    // Load
    Record key2;
    key2.AddField(new IntType());
    key2.AddField(new StringType());
    key2.AddField(new LongIntType());
    key2.AddField(new StringType());
    key2.AddField(new DoubleType());
    key2.AddField(new CharArrayType(11));
    key2.AddField(new BoolType());
    key2.AddField(new CharArrayType(7));
    AssertEqual(32, key2.LoadFromMem(buf), "Load size error");
    AssertTrue(key1 == key2);

    Record key3;
    key3.AddField(new IntType());
    key3.AddField(new StringType());
    key3.AddField(new LongIntType());
    key3.AddField(new StringType());
    key3.AddField(new DoubleType());
    key3.AddField(new CharArrayType(11));
    key3.AddField(new BoolType());
    key3.AddField(new CharArrayType(7));
    AssertTrue(key3 < key1);

    Record key4;
    AssertTrue(key4 < key1);

    delete[] buf;
  }
};

}  // namespace Schema

int main() {
  Schema::RecordTest test;
  test.setup();
  test.Test_Record_Operators();
  test.Test_Record_LoadDump();
  test.teardown();

  std::cout << "\033[2;32mPassed ^_^\033[0m" << std::endl;
  return 0;
}