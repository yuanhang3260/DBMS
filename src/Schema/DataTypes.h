#ifndef SCHEMA_DATA_TYPES_
#define SCHEMA_DATA_TYPES_

#include <string.h>
#include <string>

#include "Base/MacroUtils.h"
#include "Base/BaseTypes.h"
#include "Base/Utils.h"
#include "SchemaType.h"

namespace Schema {

class IntType: public SchemaField {
 public:
  IntType() = default;
  IntType(int value) : value_(value) {}

  DEFINE_ACCESSOR(value, int);
  FieldType type() override { return INT; }
  int length() override { return 4; }

  // Comparable
  bool operator<(const IntType& other) const {
    return value_ < other.value();
  }

  // Dump to memory
  int DumpToMem(byte* buf) const override {
    if (!buf) {
      return -1;
    }
    memcpy(buf, &value_, sizeof(value_));
    return sizeof(value_);
  }

 private:
  int value_ = 0;
};


class LongIntType: public SchemaField {
 public:
  LongIntType() = default;
  LongIntType(int64 value) : value_(value) {}

  DEFINE_ACCESSOR(value, int64);
  FieldType type() override { return LONGINT; }
  int length() override { return 8; }

  // Comparable
  bool operator<(const LongIntType& other) const {
    return value_ < other.value();
  }

  // Dump to memory
  int DumpToMem(byte* buf) const override {
    if (!buf) {
      return -1;
    }
    memcpy(buf, &value_, sizeof(value_));
    return sizeof(value_);
  }

 private:
  int64 value_ = 0;
};


class DoubleType: public SchemaField {
 public:
  DoubleType() = default;
  DoubleType(double value) : value_(value) {}

  DEFINE_ACCESSOR(value, double);
  FieldType type() override { return DOUBLE; }
  int length() override { return 8; }

  // Comparable
  bool operator<(const DoubleType& other) const {
    return value_ < other.value();
  }

  // Dump to memory
  int DumpToMem(byte* buf) const override {
    if (!buf) {
      return -1;
    }
    memcpy(buf, &value_, sizeof(value_));
    return sizeof(value_);
  }

 private:
  double value_ = 0.0;
};


class BoolType: public SchemaField {
 public:
  BoolType() = default;
  BoolType(bool value) : value_(value) {}

  DEFINE_ACCESSOR(value, bool);
  FieldType type() override { return BOOL; }
  int length() override { return 4; }

  // Comparable
  bool operator<(const BoolType& other) const {
    return value_ < other.value();
  }

  // Dump to memory
  int DumpToMem(byte* buf) const override {
    if (!buf) {
      return -1;
    }
    memcpy(buf, &value_, sizeof(value_));
    return sizeof(value_);
  }

 private:
  bool value_ = false;
};


class StringType: public SchemaField {
 public:
  StringType() = default;
  StringType(std::string str) : value_(str) {}

  DEFINE_ACCESSOR(value, std::string);
  FieldType type() override { return STRING; }
  int length() override { return value_.length(); }

  // Comparable
  bool operator<(const StringType& other) const;

  // Dump to memory
  int DumpToMem(byte* buf) const override;

 private:
  std::string value_;
};


class CharArrayType: public SchemaField {
 public:
  CharArrayType() = default;
  CharArrayType(std::string str, int lenlimit);
  CharArrayType(const char* src, int length, int lenlimit);
  ~CharArrayType();

  bool SetData(const char* src, int length);

  FieldType type() override { return CHARARRAY; }
  int length() override { return length_; }
  const char* value() { return value_; }

  // Comparable
  bool operator<(const CharArrayType& other) const;

  // Dump to memory
  int DumpToMem(byte* buf) const override;

 private:
  char* value_ = nullptr;
  int length_ = 0;
  int length_limit_;
};


}  // namespace Schema

#endif  /* SCHEMA_DATA_TYPES_ */
