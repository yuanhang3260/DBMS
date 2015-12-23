#include <vector>
#include <algorithm>

#include "UnitTest/UnitTest.h"
#include "Base/Utils.h"
#include "DataTypes.h"


namespace Schema {

class DataTypesTest: public UnitTest {
 public:
  void Test_IntType() {
    std::cout << __FUNCTION__ << std::endl;
    // int type
    int value = Utils::RandomNumber();
    IntType int_field(value);
    AssertEqual(value, int_field.value());

    int value2 = Utils::RandomNumber();
    IntType int_field2(value2);
    AssertEqual(value2, int_field2.value());    

    AssertEqual(value < value2, int_field < int_field2);
    AssertTrue(int_field <= int_field);
    AssertTrue(int_field >= int_field);
    AssertEqual(int_field == int_field2, value == value2);
    AssertEqual(int_field != int_field2, value != value2);
    std::vector<IntType> v;
    for (int i = 0; i < 100; i++) {
      v.push_back(IntType(Utils::RandomNumber()));
    }
    std::sort(v.begin(), v.end());
    for (int i = 0; i < 99; i ++) {
      AssertTrue(v[i] <= v[i + 1]);
    }
  }

  void Test_LongIntType() {
    std::cout << __FUNCTION__ << std::endl;
    // int type
    int64 value = Utils::RandomNumber();
    LongIntType longint_field(value);
    AssertEqual(value, longint_field.value());

    int64 value2 = Utils::RandomNumber();
    LongIntType longint_field2(value2);
    AssertEqual(value2, longint_field2.value());    

    AssertEqual(value < value2, longint_field < longint_field2);
    AssertTrue(longint_field <= longint_field);
    AssertTrue(longint_field >= longint_field);
    AssertEqual(longint_field == longint_field2, value == value2);
    AssertEqual(longint_field != longint_field2, value != value2);
    std::vector<LongIntType> v;
    for (int i = 0; i < 100; i++) {
      v.push_back(LongIntType(Utils::RandomNumber()));
    }
    std::sort(v.begin(), v.end());
    for (int i = 0; i < 99; i ++) {
      AssertTrue(v[i] <= v[i + 1]);
    }
  }

  void Test_DoubleType() {
    std::cout << __FUNCTION__ << std::endl;
    // int type
    double value = Utils::RandomNumber() * 1.0 / Utils::RandomNumber();
    DoubleType double_field(value);
    AssertEqual(value, double_field.value());

    double value2 = Utils::RandomNumber() * 1.0 / Utils::RandomNumber();
    DoubleType double_field2(value2);
    AssertEqual(value2, double_field2.value());    

    AssertEqual(value < value2, double_field < double_field2);
    AssertTrue(double_field <= double_field);
    AssertTrue(double_field >= double_field);
    AssertEqual(double_field == double_field2, value == value2);
    AssertEqual(double_field != double_field2, value != value2);
    std::vector<DoubleType> v;
    for (int i = 0; i < 100; i++) {
      v.push_back(
          DoubleType(Utils::RandomNumber() * 1.0 / Utils::RandomNumber()));
    }
    std::sort(v.begin(), v.end());
    for (int i = 0; i < 99; i ++) {
      AssertTrue(v[i] <= v[i + 1]);
    }
  }

  void Test_StringType() {
    std::cout << __FUNCTION__ << std::endl;
    AssertTrue(StringType("ab") < StringType("ac"));
    AssertTrue(StringType("ab") < StringType("abc"));
    AssertTrue(StringType("") < StringType("ab"));
    AssertTrue(StringType("abd") < StringType("bb"));
    AssertTrue(StringType("abd") == StringType("abd"));

    AssertTrue(StringType("xy") >= StringType("x"));
    AssertTrue(StringType("xy") >= StringType("xba"));
    AssertTrue(StringType("x") >= StringType(""));
    AssertTrue(StringType("xy") >= StringType("xx"));
    AssertTrue(StringType("xyz") == StringType("xyz"));    
  }

  void Test_CharArrayType() {
    std::cout << __FUNCTION__ << std::endl;
    AssertTrue(CharArrayType("ab", 5) < CharArrayType("ac", 5));
    AssertTrue(CharArrayType("ab", 5) < CharArrayType("abc", 5));
    AssertTrue(CharArrayType("", 5) < CharArrayType("ab", 5));
    AssertTrue(CharArrayType("abd", 5) < CharArrayType("bb", 5));
    AssertFalse(CharArrayType("ab", 5) < CharArrayType("ab", 5));
    AssertTrue(CharArrayType("ab", 5) <= CharArrayType("ab", 5));
    AssertTrue(CharArrayType("abd", 5) == CharArrayType("abd", 5));

    AssertTrue(CharArrayType("xy", 5) >= CharArrayType("x", 5));
    AssertTrue(CharArrayType("xy", 5) >= CharArrayType("xba", 5));
    AssertTrue(CharArrayType("x", 5) >= CharArrayType("", 5));
    AssertTrue(CharArrayType("xy", 5) >= CharArrayType("xx", 5));
    AssertFalse(CharArrayType("xy", 5) > CharArrayType("xy", 5));
    AssertTrue(CharArrayType("xy", 5) >= CharArrayType("xy", 5));
    AssertTrue(CharArrayType("xyz", 5) == CharArrayType("xyz", 5));

    // Test SetData()
    CharArrayType chararray_field("abc", 5);
    AssertFalse(chararray_field.SetData("abcdefg", 6));
    AssertTrue(strncmp(chararray_field.value(), "abc", 3) == 0);
  }
};

}  // namespace Schema

int main() {
  Schema::DataTypesTest test;
  test.setup();
  test.Test_IntType();
  test.Test_LongIntType();
  test.Test_DoubleType();
  test.Test_StringType();
  test.Test_CharArrayType();
  test.teardown();

  std::cout << "\033[2;32mPassed ^_^\033[0m" << std::endl;
  return 0;
}
