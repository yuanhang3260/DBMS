#include "DataTypes.h"

namespace Schema {

// Comparable
bool StringType::operator<(const StringType& other) const {
  int len = Utils::Min(value_.length(), other.value_.length());
  for (int i = 0; i < len; i++) {
    if (value_[i] < other.value_[i]) {
      return true;
    }
    else if (value_[i] < other.value_[i]) {
      return false;
    }
  }
  return value_.length() < other.value_.length();
}

// Dump to memory
int StringType::DumpToMem(byte* buf) const {
  if (!buf) {
    return -1;
  }
  memcpy(buf, value_.c_str(), value_.length());
  return value_.length();
}

CharArrayType::CharArrayType(std::string str, int lenlimit) :
    length_limit_(lenlimit) {
  if (!SetData(str.c_str(), str.length())) {
    throw std::runtime_error(
        "[Init CharArrayType Failed] - invalid lenlimit < src length");
  }
}

CharArrayType::CharArrayType(const char* src, int length, int lenlimit) :
    length_(length),
    length_limit_(lenlimit) {
  if (!SetData(src, length)) {
    throw std::runtime_error(
        "[Init CharArrayType Failed] - invalid lenlimit < src length");
  }
}

bool CharArrayType::SetData(const char* src, int length) {
  if (!value_) {
    value_ = new char[length_limit_];
  }
  if (length > length_limit_) {
    return false;
  }
  memcpy(value_, src, length);
  length_ = length;
  return true;
}

CharArrayType::~CharArrayType() {
  if (value_) {
    delete[] value_;
  }
}

// Comparable
bool CharArrayType::operator<(const CharArrayType& other) const {
  int len = Utils::Min(length_, other.length_);
  for (int i = 0; i < len; i++) {
    if (value_[i] < other.value_[i]) {
      return true;
    }
    else if (value_[i] < other.value_[i]) {
      return false;
    }
  }
  return length_ < other.length_;
}

// Dump to memory
int CharArrayType::DumpToMem(byte* buf) const {
  if (!buf) {
    return -1;
  }
  memcpy(buf, value_, length_);
  return length_;
}

}  // namepsace Schema