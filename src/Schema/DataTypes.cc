#include "Base/Log.h"
#include "DataTypes.h"

namespace Schema {

// Comparable
bool StringType::operator<(const StringType& other) const {
  return strcmp(value_.c_str(), other.value_.c_str()) < 0;
}

bool StringType::operator>(const StringType& other) const {
  return strcmp(value_.c_str(), other.value_.c_str()) > 0;
}

bool StringType::operator<=(const StringType& other) const {
  return !(*this > other);
}

bool StringType::operator>=(const StringType& other) const {
  return !(*this < other);
}

bool StringType::operator==(const StringType& other) const {
  return strcmp(value_.c_str(), other.value_.c_str()) == 0;
}

bool StringType::operator!=(const StringType& other) const {
  return !(*this == other);
}

// Dump to memory
int StringType::DumpToMem(byte* buf) const {
  if (!buf) {
    return -1;
  }
  memcpy(buf, value_.c_str(), value_.length());
  buf[value_.length()] = '\0';
  return value_.length() + 1;
}

// Dump to memory
int StringType::LoadFromMem(const byte* buf) {
  if (!buf) {
    return -1;
  }
  value_ = std::string(reinterpret_cast<const char*>(buf));
  return value_.length() + 1;
}

CharArrayType::CharArrayType(int lenlimit) :
    length_limit_(lenlimit) {
  value_ = new char[length_limit_];
  memset(value_, 0, length_limit_);
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
  if (length > length_limit_) {
    LogERROR("Can't SetData() for CharArrayType - length %d > length_limit %d",
             length, length_limit_);
    return false;
  }

  if (!value_) {
    value_ = new char[length_limit_];
  }
  memset(value_, 0, length_limit_);
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
  int re = strncmp(value_, other.value_, len);
  if (re < 0) {
    return true;
  }
  if (re > 0) {
    return false;
  }
  return length_ < other.length_;
}

bool CharArrayType::operator>(const CharArrayType& other) const {
  int len = Utils::Min(length_, other.length_);
  int re = strncmp(value_, other.value_, len);
  if (re > 0) {
    return true;
  }
  if (re < 0) {
    return false;
  }
  return length_ > other.length_;
}

bool CharArrayType::operator<=(const CharArrayType& other) const {
  return !(*this > other);
}

bool CharArrayType::operator>=(const CharArrayType& other) const {
  return !(*this < other);
}

bool CharArrayType::operator==(const CharArrayType& other) const {
  if (length_ != other.length_) {
    return false;
  }
  return strncmp(value_, other.value_, length_) == 0;
}

bool CharArrayType::operator!=(const CharArrayType& other) const {
  return !(*this == other);
}

// Dump to memory
int CharArrayType::DumpToMem(byte* buf) const {
  if (!buf) {
    return -1;
  }
  memcpy(buf, value_, length_limit_);
  return length_limit_;
}

// Dump to memory
int CharArrayType::LoadFromMem(const byte* buf) {
  if (!buf) {
    return -1;
  }
  memset(value_, 0, length_limit_);
  memcpy(value_, buf, length_limit_);
  
  int i = 0;
  for (; i < length_limit_; i++) {
    if (value_[i] == 0) {
      break;
    }
  }
  length_ = i;
  return length_limit_;
}

void CharArrayType::reset() {
  if (!value_) {
    return;
  }

  memset(value_, 0, length_limit_);
  length_ = 0;
}

}  // namepsace Schema
