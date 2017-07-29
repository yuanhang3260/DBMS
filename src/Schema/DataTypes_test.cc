#include <vector>
#include <algorithm>

#include "UnitTest/UnitTest.h"
#include "Base/Utils.h"
#include "DataTypes.h"


namespace Schema {

class DataTypesTest: public UnitTest {
 public:
  void Test_IntField() {
    std::cout << __FUNCTION__ << std::endl;
    // int type
    int value = Utils::RandomNumber();
    IntField int_field(value);
    AssertEqual(value, int_field.value());

    int value2 = Utils::RandomNumber();
    IntField int_field2(value2);
    AssertEqual(value2, int_field2.value());    

    AssertEqual(value < value2, int_field < int_field2);
    AssertTrue(int_field <= int_field);
    AssertTrue(int_field >= int_field);
    AssertEqual(int_field == int_field2, value == value2);
    AssertEqual(int_field != int_field2, value != value2);
    std::vector<IntField> v;
    for (int i = 0; i < 100; i++) {
      v.push_back(IntField(Utils::RandomNumber()));
    }
    std::sort(v.begin(), v.end());
    for (int i = 0; i < 99; i ++) {
      AssertTrue(v[i] <= v[i + 1]);
    }
  }

  void Test_LongIntField() {
    std::cout << __FUNCTION__ << std::endl;
    // int type
    int64 value = Utils::RandomNumber();
    LongIntField longint_field(value);
    AssertEqual(value, longint_field.value());

    int64 value2 = Utils::RandomNumber();
    LongIntField longint_field2(value2);
    AssertEqual(value2, longint_field2.value());    

    AssertEqual(value < value2, longint_field < longint_field2);
    AssertTrue(longint_field <= longint_field);
    AssertTrue(longint_field >= longint_field);
    AssertEqual(longint_field == longint_field2, value == value2);
    AssertEqual(longint_field != longint_field2, value != value2);
    std::vector<LongIntField> v;
    for (int i = 0; i < 100; i++) {
      v.push_back(LongIntField(Utils::RandomNumber()));
    }
    std::sort(v.begin(), v.end());
    for (int i = 0; i < 99; i ++) {
      AssertTrue(v[i] <= v[i + 1]);
    }
  }

  void Test_DoubleField() {
    std::cout << __FUNCTION__ << std::endl;
    // int type
    double value = Utils::RandomNumber() * 1.0 / Utils::RandomNumber();
    DoubleField double_field(value);
    AssertEqual(value, double_field.value());

    double value2 = Utils::RandomNumber() * 1.0 / Utils::RandomNumber();
    DoubleField double_field2(value2);
    AssertEqual(value2, double_field2.value());    

    AssertEqual(value < value2, double_field < double_field2);
    AssertTrue(double_field <= double_field);
    AssertTrue(double_field >= double_field);
    AssertEqual(double_field == double_field2, value == value2);
    AssertEqual(double_field != double_field2, value != value2);
    std::vector<DoubleField> v;
    for (int i = 0; i < 100; i++) {
      v.push_back(
          DoubleField(Utils::RandomNumber() * 1.0 / Utils::RandomNumber()));
    }
    std::sort(v.begin(), v.end());
    for (int i = 0; i < 99; i ++) {
      AssertTrue(v[i] <= v[i + 1]);
    }
  }

  void Test_CharField() {
    std::cout << __FUNCTION__ << std::endl;
    AssertTrue(CharField('a') < CharField('b'));
    AssertTrue(CharField('a') <= CharField('a'));
    AssertTrue(CharField('c') > CharField('b'));
    AssertTrue(CharField('c') >= CharField('b'));
    AssertTrue(CharField('c') != CharField('d'));
    AssertTrue(CharField('c') == CharField('c'));
  }

  void Test_StringField() {
    std::cout << __FUNCTION__ << std::endl;
    AssertTrue(StringField("ab") < StringField("ac"));
    AssertTrue(StringField("ab") < StringField("abc"));
    AssertTrue(StringField("") < StringField("ab"));
    AssertTrue(StringField("abd") < StringField("bb"));
    AssertTrue(StringField("abd") == StringField("abd"));

    AssertTrue(StringField("xy") >= StringField("x"));
    AssertTrue(StringField("xy") >= StringField("xba"));
    AssertTrue(StringField("x") >= StringField(""));
    AssertTrue(StringField("xy") >= StringField("xx"));
    AssertTrue(StringField("xyz") == StringField("xyz"));    
  }

  void Test_CharArrayField() {
    std::cout << __FUNCTION__ << std::endl;
    AssertTrue(CharArrayField("ab", 5) < CharArrayField("ac", 5));
    AssertTrue(CharArrayField("ab", 5) < CharArrayField("abc", 5));
    AssertTrue(CharArrayField("", 5) < CharArrayField("ab", 5));
    AssertTrue(CharArrayField("abd", 5) < CharArrayField("bb", 5));
    AssertFalse(CharArrayField("ab", 5) < CharArrayField("ab", 5));
    AssertTrue(CharArrayField("ab", 5) <= CharArrayField("ab", 5));
    AssertTrue(CharArrayField("abd", 5) == CharArrayField("abd", 5));

    AssertTrue(CharArrayField("xy", 5) >= CharArrayField("x", 5));
    AssertTrue(CharArrayField("xy", 5) >= CharArrayField("xba", 5));
    AssertTrue(CharArrayField("x", 5) >= CharArrayField("", 5));
    AssertTrue(CharArrayField("xy", 5) >= CharArrayField("xx", 5));
    AssertFalse(CharArrayField("xy", 5) > CharArrayField("xy", 5));
    AssertTrue(CharArrayField("xy", 5) >= CharArrayField("xy", 5));
    AssertTrue(CharArrayField("xyz", 5) == CharArrayField("xyz", 5));

    // Test SetData()
    CharArrayField chararray_field("abc", 5);
    AssertFalse(chararray_field.SetData("abcdefg", 6));
    AssertTrue(strncmp(chararray_field.value(), "abc", 3) == 0);
  }

  void Test_EvaluateValueRatio() {
    // Int
    ValueRange<int> int_range;
    int_range.min = 1;
    int_range.max = 5;
    int_range.set_single_value(3);
    AssertFloatEqual(0.2, IntField::EvaluateValueRatio(int_range));

    int_range.single_value.reset();

    int_range.set_left_value(2);
    int_range.set_right_value(3);
    AssertFloatEqual(0.4, IntField::EvaluateValueRatio(int_range));

    int_range.set_left_value(-1);
    int_range.set_right_value(3);
    AssertFloatEqual(0.6, IntField::EvaluateValueRatio(int_range));

    int_range.set_left_value(2);
    int_range.set_right_value(8);
    AssertFloatEqual(0.8, IntField::EvaluateValueRatio(int_range));

    int_range.set_right_value(1);
    int_range.left_value.reset();
    AssertFloatEqual(0.2, IntField::EvaluateValueRatio(int_range));

    int_range.set_left_value(2);
    int_range.right_value.reset();
    AssertFloatEqual(0.8, IntField::EvaluateValueRatio(int_range));

    int_range.set_left_value(3);
    int_range.set_right_value(1);
    AssertFloatEqual(0, IntField::EvaluateValueRatio(int_range));

    int_range.set_left_value(-3);
    int_range.set_right_value(-1);
    AssertFloatEqual(0, IntField::EvaluateValueRatio(int_range));

    int_range.set_left_value(6);
    int_range.set_right_value(9);
    AssertFloatEqual(0, IntField::EvaluateValueRatio(int_range));

    // String
    ValueRange<std::string> str_range;
    str_range.min = "abcdefghi";
    str_range.max = "abcxyzw";
    str_range.set_single_value("abd");
    AssertFloatEqual(0, StringField::EvaluateValueRatio(str_range));

    str_range.set_single_value("abcmn");
    AssertFloatEqual(1.0 / 5410981784,
                     StringField::EvaluateValueRatio(str_range));

    str_range.single_value.reset();

    str_range.min = "xya";
    str_range.max = "xyf";
    str_range.set_left_value("xyb");
    str_range.set_right_value("xyc");
    AssertFloatEqual(268435457.0 / 1342177281,  //  ~= 0.2
                     StringField::EvaluateValueRatio(str_range));
  }
};

}  // namespace Schema

int main() {
  Schema::DataTypesTest test;
  test.setup();
  // test.Test_IntField();
  // test.Test_LongIntField();
  // test.Test_DoubleField();
  // test.Test_CharField();
  // test.Test_StringField();
  // test.Test_CharArrayField();
  test.Test_EvaluateValueRatio();
  test.teardown();

  std::cout << "\033[2;32mPassed ^_^\033[0m" << std::endl;
  return 0;
}
