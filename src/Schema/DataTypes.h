#ifndef SCHEMA_DATA_TYPES_
#define SCHEMA_DATA_TYPES_

#include <string.h>
#include <string>
#include <climits>
#include <cfloat>

#include "Base/MacroUtils.h"
#include "Base/BaseTypes.h"
#include "Base/Utils.h"
#include "Schema/SchemaType.h"

namespace Schema {

template <class T>
struct ValueRange {
  T min;
  T max;
  std::shared_ptr<T> single_value;
  std::shared_ptr<T> left_value;
  bool left_open = false;
  std::shared_ptr<T> right_value;
  bool right_open = false;

  void set_single_value(const T& v) { single_value.reset(new T(v)); }
  void set_left_value(const T& v) { left_value.reset(new T(v)); }
  void set_right_value(const T& v) { right_value.reset(new T(v)); }
};

class IntField: public Field {
 public:
  IntField() = default;
  IntField(int value) : value_(value) {}
  ~IntField() {}

  DEFINE_ACCESSOR(value, int);
  FieldType type() const override { return FieldType::INT; }
  int length() const override { return 4; }

  // Comparable
  bool operator<(const IntField& other) const {
    return value_ < other.value();
  }

  bool operator<=(const IntField& other) const {
    return value_ <= other.value();
  }

  bool operator>(const IntField& other) const {
    return value_ > other.value();
  }

  bool operator>=(const IntField& other) const {
    return value_ >= other.value();
  }

  bool operator==(const IntField& other) const {
    return value_ == other.value();
  }

  bool operator!=(const IntField& other) const {
    return value_ != other.value();
  }

  IntField& operator+=(const IntField& other) {
    value_ += other.value_;
    return *this;
  }

  std::string AsString() const override { return std::to_string(value_); }

  // Dump to memory
  int DumpToMem(byte* buf) const override {
    if (!buf) {
      return -1;
    }
    memcpy(buf, &value_, sizeof(value_));
    return sizeof(value_);
  }

  int LoadFromMem(const byte* buf) override {
    if (!buf) {
      return -1;
    }
    memcpy(&value_, buf, sizeof(int));
    return sizeof(int);
  }

  void reset() override { value_ = INT_MIN; }

  static double EvaluateValueRatio(ValueRange<int>& range);

  Field* Copy() const override { return new IntField(value_); }

  void Inc(int inc) { value_ += inc; }

 private:
  int value_ = LONG_MIN;
};


class LongIntField: public Field {
 public:
  LongIntField() = default;
  LongIntField(int64 value) : value_(value) {}
  ~LongIntField() {}

  DEFINE_ACCESSOR(value, int64);
  FieldType type() const override { return FieldType::LONGINT; }
  int length() const override { return 8; }

  // Comparable
  bool operator<(const LongIntField& other) const {
    return value_ < other.value();
  }

  bool operator<=(const LongIntField& other) const {
    return value_ <= other.value();
  }

  bool operator>(const LongIntField& other) const {
    return value_ > other.value();
  }

  bool operator>=(const LongIntField& other) const {
    return value_ >= other.value();
  }

  bool operator==(const LongIntField& other) const {
    return value_ == other.value();
  }

  bool operator!=(const LongIntField& other) const {
    return value_ != other.value();
  }

  LongIntField& operator+=(const LongIntField& other) {
    value_ += other.value_;
    return *this;
  }

  std::string AsString() const override { return std::to_string(value_); }

  // Dump to memory
  int DumpToMem(byte* buf) const override {
    if (!buf) {
      return -1;
    }
    memcpy(buf, &value_, sizeof(value_));
    return sizeof(value_);
  }

  int LoadFromMem(const byte* buf) override {
    if (!buf) {
      return -1;
    }
    memcpy(&value_, buf, sizeof(int64));
    return sizeof(int64);
  }

  void reset() override { value_ = LLONG_MIN; }

  static double EvaluateValueRatio(ValueRange<int64>& range);

  Field* Copy() const override { return new LongIntField(value_); }

  void Inc(int inc) { value_ += inc; }

 private:
  int64 value_ = LLONG_MIN;
};


class DoubleField: public Field {
 public:
  DoubleField() = default;
  DoubleField(double value) : value_(value) {}
  ~DoubleField() {}

  DEFINE_ACCESSOR(value, double);
  FieldType type() const override { return FieldType::DOUBLE; }
  int length() const override { return 8; }

  // Comparable
  bool operator<(const DoubleField& other) const {
    return value_ < other.value();
  }

  bool operator<=(const DoubleField& other) const {
    return value_ <= other.value();
  }

  bool operator>(const DoubleField& other) const {
    return value_ > other.value();
  }

  bool operator>=(const DoubleField& other) const {
    return value_ >= other.value();
  }

  bool operator==(const DoubleField& other) const {
    return value_ == other.value();
  }

  bool operator!=(const DoubleField& other) const {
    return value_ != other.value();
  }

  DoubleField& operator+=(const DoubleField& other) {
    value_ += other.value_;
    return *this;
  }

  std::string AsString() const override;

  // Dump to memory
  int DumpToMem(byte* buf) const override {
    if (!buf) {
      return -1;
    }
    memcpy(buf, &value_, sizeof(value_));
    return sizeof(value_);
  }

  int LoadFromMem(const byte* buf) override {
    if (!buf) {
      return -1;
    }
    memcpy(&value_, buf, sizeof(double));
    return sizeof(double);
  }

  void reset() override { value_ = -DBL_MAX; }

  static double EvaluateValueRatio(ValueRange<double>& range);

  Field* Copy() const override { return new DoubleField(value_); }

 private:
  double value_ = -DBL_MAX;
};


class BoolField: public Field {
 public:
  BoolField() = default;
  BoolField(bool value) : value_(value) {}
  ~BoolField() {}

  DEFINE_ACCESSOR(value, bool);
  FieldType type() const override { return FieldType::BOOL; }
  int length() const override { return 1; }

  // Comparable
  bool operator<(const BoolField& other) const {
    return value_ < other.value();
  }

  bool operator<=(const BoolField& other) const {
    return value_ <= other.value();
  }

  bool operator>(const BoolField& other) const {
    return value_ > other.value();
  }

  bool operator>=(const BoolField& other) const {
    return value_ >= other.value();
  }

  bool operator==(const BoolField& other) const {
    return value_ == other.value();
  }

  bool operator!=(const BoolField& other) const {
    return value_ != other.value();
  }

  std::string AsString() const override { return value_? "True" : "False"; }

  // Dump to memory
  int DumpToMem(byte* buf) const override {
    if (!buf) {
      return -1;
    }
    memcpy(buf, &value_, sizeof(value_));
    return sizeof(value_);
  }

  int LoadFromMem(const byte* buf) override {
    if (!buf) {
      return -1;
    }
    memcpy(&value_, buf, sizeof(bool));
    return sizeof(bool);
  }

  void reset() override { value_ = false; }

  static double EvaluateValueRatio(ValueRange<bool>& range);

  Field* Copy() const override { return new BoolField(value_); }

 private:
  bool value_ = false;
};


class CharField: public Field {
 public:
  CharField() = default;
  CharField(char c) : value_(c) {}
  ~CharField() {}

  DEFINE_ACCESSOR(value, char);
  FieldType type() const override { return FieldType::CHAR; }
  int length() const override { return 1; }

  // Comparable
  bool operator<(const CharField& other) const {
    return value_ < other.value();
  }

  bool operator<=(const CharField& other) const {
    return value_ <= other.value();
  }

  bool operator>(const CharField& other) const {
    return value_ > other.value();
  }

  bool operator>=(const CharField& other) const {
    return value_ >= other.value();
  }

  bool operator==(const CharField& other) const {
    return value_ == other.value();
  }

  bool operator!=(const CharField& other) const {
    return value_ != other.value();
  }

  // Dump to memory
  int DumpToMem(byte* buf) const override {
    if (!buf) {
      return -1;
    }
    memcpy(buf, &value_, sizeof(value_));
    return sizeof(value_);
  }

  int LoadFromMem(const byte* buf) override {
    if (!buf) {
      return -1;
    }
    memcpy(&value_, buf, sizeof(value_));
    return sizeof(value_);
  }

  std::string AsString() const override { return std::string(1, value_); }

  void reset() override { value_ = 0; }

  static double EvaluateValueRatio(ValueRange<char>& range);

  Field* Copy() const override { return new CharField(value_); }

 private:
  char value_ = 0;
};


class StringField: public Field {
 public:
  StringField() = default;
  StringField(const std::string& str) : value_(str) {}
  StringField(const char* buf, int size) : value_(buf, size) {}
  ~StringField() {}

  DEFINE_ACCESSOR(value, std::string);
  FieldType type() const override { return FieldType::STRING; }
  int length() const override { return value_.length() + 1; }

  // Comparable
  bool operator<(const StringField& other) const;
  bool operator<=(const StringField& other) const;
  bool operator>(const StringField& other) const;
  bool operator>=(const StringField& other) const;
  bool operator==(const StringField& other) const;
  bool operator!=(const StringField& other) const;

  // Dump to memory
  int DumpToMem(byte* buf) const override;
  int LoadFromMem(const byte* buf) override;

  std::string AsString() const override { return value_; }

  void reset() override { value_.clear(); }

  static double EvaluateValueRatio(ValueRange<std::string>& range);

  Field* Copy() const override { return new StringField(value_); }

 private:
  std::string value_;
};


class CharArrayField: public Field {
 public:
  CharArrayField() = default;
  CharArrayField(int lenlimit);
  CharArrayField(const std::string& str, int lenlimit);
  CharArrayField(const std::string& str);
  CharArrayField(const char* src, int length, int lenlimit);
  ~CharArrayField();

  bool SetData(const char* src, int length);

  FieldType type() const override { return FieldType::CHARARRAY; }
  int length() const override {
    return length_limit_;
  }
  int valid_length() const {
    return length_;
  }
  const char* value() const { return value_; }

  // Comparable
  bool operator<(const CharArrayField& other) const;
  bool operator<=(const CharArrayField& other) const;
  bool operator>(const CharArrayField& other) const;
  bool operator>=(const CharArrayField& other) const;
  bool operator==(const CharArrayField& other) const;
  bool operator!=(const CharArrayField& other) const;

  std::string AsString() const override { return std::string(value_, length_); }

  // Dump to memory
  int DumpToMem(byte* buf) const override;
  int LoadFromMem(const byte* buf) override;

  void reset() override;

  static double EvaluateValueRatio(ValueRange<std::string>& range);

  Field* Copy() const override;

 private:
  char* value_ = nullptr;
  int length_ = 0;  // Length of the char array. Ending '\0' excluded.
  int length_limit_;
};


}  // namespace Schema

#endif  /* SCHEMA_DATA_TYPES_ */
