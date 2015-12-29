#ifndef SCHEMA_DATA_TYPES_
#define SCHEMA_DATA_TYPES_

#include <string.h>
#include <string>
#include <climits>
#include <cfloat>

#include "Base/MacroUtils.h"
#include "Base/BaseTypes.h"
#include "Base/Utils.h"
#include "SchemaType.h"

namespace Schema {

class IntType: public SchemaFieldType {
 public:
  IntType() = default;
  IntType(int value) : value_(value) {}

  DEFINE_ACCESSOR(value, int);
  FieldType type() const override { return INT; }
  int length() const override { return 4; }

  // Comparable
  bool operator<(const IntType& other) const {
    return value_ < other.value();
  }

  bool operator<=(const IntType& other) const {
    return value_ <= other.value();
  }

  bool operator>(const IntType& other) const {
    return value_ > other.value();
  }

  bool operator>=(const IntType& other) const {
    return value_ >= other.value();
  }

  bool operator==(const IntType& other) const {
    return value_ == other.value();
  }

  bool operator!=(const IntType& other) const {
    return value_ != other.value();
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

 private:
  int value_ = LONG_MIN;
};


class LongIntType: public SchemaFieldType {
 public:
  LongIntType() = default;
  LongIntType(int64 value) : value_(value) {}

  DEFINE_ACCESSOR(value, int64);
  FieldType type() const override { return LONGINT; }
  int length() const override { return 8; }

  // Comparable
  bool operator<(const LongIntType& other) const {
    return value_ < other.value();
  }

  bool operator<=(const LongIntType& other) const {
    return value_ <= other.value();
  }

  bool operator>(const LongIntType& other) const {
    return value_ > other.value();
  }

  bool operator>=(const LongIntType& other) const {
    return value_ >= other.value();
  }

  bool operator==(const LongIntType& other) const {
    return value_ == other.value();
  }

  bool operator!=(const LongIntType& other) const {
    return value_ != other.value();
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

 private:
  int64 value_ = LLONG_MIN;
};


class DoubleType: public SchemaFieldType {
 public:
  DoubleType() = default;
  DoubleType(double value) : value_(value) {}

  DEFINE_ACCESSOR(value, double);
  FieldType type() const override { return DOUBLE; }
  int length() const override { return 8; }

  // Comparable
  bool operator<(const DoubleType& other) const {
    return value_ < other.value();
  }

  bool operator<=(const DoubleType& other) const {
    return value_ <= other.value();
  }

  bool operator>(const DoubleType& other) const {
    return value_ > other.value();
  }

  bool operator>=(const DoubleType& other) const {
    return value_ >= other.value();
  }

  bool operator==(const DoubleType& other) const {
    return value_ == other.value();
  }

  bool operator!=(const DoubleType& other) const {
    return value_ != other.value();
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
    memcpy(&value_, buf, sizeof(double));
    return sizeof(double);
  }

 private:
  double value_ = -DBL_MAX;
};


class BoolType: public SchemaFieldType {
 public:
  BoolType() = default;
  BoolType(bool value) : value_(value) {}

  DEFINE_ACCESSOR(value, bool);
  FieldType type() const override { return BOOL; }
  int length() const override { return 1; }

  // Comparable
  bool operator<(const BoolType& other) const {
    return value_ < other.value();
  }

  bool operator<=(const BoolType& other) const {
    return value_ <= other.value();
  }

  bool operator>(const BoolType& other) const {
    return value_ > other.value();
  }

  bool operator>=(const BoolType& other) const {
    return value_ >= other.value();
  }

  bool operator==(const BoolType& other) const {
    return value_ == other.value();
  }

  bool operator!=(const BoolType& other) const {
    return value_ != other.value();
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
    memcpy(&value_, buf, sizeof(bool));
    return sizeof(bool);
  }

 private:
  bool value_ = false;
};


class StringType: public SchemaFieldType {
 public:
  StringType() = default;
  StringType(std::string str) : value_(str) {}
  StringType(const char* buf, int size) : value_(buf, size) {}

  DEFINE_ACCESSOR(value, std::string);
  FieldType type() const override { return STRING; }
  int length() const override { return value_.length() + 1; }

  // Comparable
  bool operator<(const StringType& other) const;
  bool operator<=(const StringType& other) const;
  bool operator>(const StringType& other) const;
  bool operator>=(const StringType& other) const;
  bool operator==(const StringType& other) const;
  bool operator!=(const StringType& other) const;

  // Dump to memory
  int DumpToMem(byte* buf) const override;
  int LoadFromMem(const byte* buf) override;

  std::string AsString() const override { return value_; }

 private:
  std::string value_;
};


class CharArrayType: public SchemaFieldType {
 public:
  CharArrayType() = default;
  CharArrayType(int lenlimit);
  CharArrayType(std::string str, int lenlimit);
  CharArrayType(const char* src, int length, int lenlimit);
  ~CharArrayType();

  bool SetData(const char* src, int length);

  FieldType type() const override { return CHARARRAY; }
  int length() const override {
    return length_limit_;
  }
  const char* value() { return value_; }

  // Comparable
  bool operator<(const CharArrayType& other) const;
  bool operator<=(const CharArrayType& other) const;
  bool operator>(const CharArrayType& other) const;
  bool operator>=(const CharArrayType& other) const;
  bool operator==(const CharArrayType& other) const;
  bool operator!=(const CharArrayType& other) const;

  std::string AsString() const override { return std::string(value_, length_); }

  // Dump to memory
  int DumpToMem(byte* buf) const override;
  int LoadFromMem(const byte* buf) override;

 private:
  char* value_ = nullptr;
  int length_ = 0;  // Length of the char array. Ending '\0' excluded.
  int length_limit_;
};


}  // namespace Schema

#endif  /* SCHEMA_DATA_TYPES_ */
