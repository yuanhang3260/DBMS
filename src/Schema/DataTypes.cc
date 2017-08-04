#include "Base/Log.h"
#include "Strings/Utils.h"

#include "Schema/DataTypes.h"

namespace Schema {

namespace {

template<typename T>
double EvaluateIntegerValueRatio(ValueRange<T>& range) {
  // std::cout << "(min, max) = (" << range.min << ", " << range.max << ")\n";
  // if (range.single_value) {
  //   std::cout << "single_value: " << *range.single_value << std::endl;
  // }
  // if (range.left_value) {
  //   std::cout << "left_value: " << *range.left_value << std::endl;
  // }
  // if (range.right_value) {
  //   std::cout << "right_value: " << *range.right_value << std::endl;
  // }

  if (range.min > range.max) {
    return -1;
  }

  if (range.single_value) {
    if (*range.single_value == range.min && range.min == range.max) {
      return 2;
    } else if (*range.single_value >= range.min &&
               *range.single_value <= range.max) {
      return 1.0 / (range.max - range.min + 1);
    } else {
      return -1;
    }
  } else {
    if (!range.left_value || *range.left_value < range.min) {
      range.set_left_value(range.min);
      range.left_open = false;
    }
    if (!range.right_value || *range.right_value > range.max) {
      range.set_right_value(range.max);
      range.right_open = false;
    }

    if (*range.left_value > *range.right_value) {
      return -1;
    }
    if (*range.left_value == *range.right_value &&
        (range.left_open || range.right_open)) {
      return -1;
    }

    if (*range.left_value == range.min && *range.right_value == range.max) {
      return 2;
    }

    int boundary_num = 0;
    if (range.left_open && range.right_open) {
      boundary_num = -1;
    } else if (!range.left_open && !range.right_open) {
      boundary_num = 1;
    }
    return 1.0 * (*range.right_value - *range.left_value + boundary_num) /
                 (range.max - range.min + 1);
  }
}

double EvaluateDoubleValueRatio(ValueRange<double>& range) {
  // std::cout << "(min, max) = (" << range.min << ", " << range.max << ")\n";
  // if (range.single_value) {
  //   std::cout << "single_value: " << *range.single_value << std::endl;
  // }
  // if (range.left_value) {
  //   std::cout << "left_value: " << *range.left_value << std::endl;
  // }
  // if (range.right_value) {
  //   std::cout << "right_value: " << *range.right_value << std::endl;
  // }

  if (range.min > range.max) {
    return -1;
  }

  if (range.single_value) {
    if (*range.single_value == range.min && range.min == range.max) {
      return 2;
    } else {
      return 0;
    }
  } else {
    if (!range.left_value || *range.left_value < range.min) {
      range.set_left_value(range.min);
      range.left_open = false;
    }
    if (!range.right_value || *range.right_value > range.max) {
      range.set_right_value(range.max);
      range.right_open = false;
    }

    if (*range.left_value > *range.right_value) {
      return -1;
    }
    if (*range.left_value == *range.right_value &&
        (range.left_open || range.right_open)) {
      return -1;
    }
    if (*range.left_value == range.min && *range.right_value == range.max) {
      return 2;
    }

    return 1.0 * (*range.right_value - *range.left_value) /
                 (range.max - range.min);
  }
}

double EvaluateStringValueRatio(ValueRange<std::string>& range) {
  // std::cout << "(min, max) = (" << range.min << ", " << range.max << ")\n";
  // if (range.single_value) {
  //   std::cout << "single_value: " << *range.single_value << std::endl;
  // }
  // if (range.left_value) {
  //   std::cout << "left_value: " << *range.left_value << std::endl;
  // }
  // if (range.right_value) {
  //   std::cout << "right_value: " << *range.right_value << std::endl;
  // }

  if (range.min > range.max) {
    return 0;
  }

  // Map a string into an int64 space, each char is 128-base weighted.
  auto numerise_str = [&] (const std::string& str, uint32 skip_prefix) {
    int64 sum = 0;
    int64 weight = 128*128*128*128;
    for (uint32 i = skip_prefix; i < skip_prefix + 5; i++) {
      char c;
      if (i >= str.length()) {
        c = 0;
      } else {
        c = str.at(i);
      }
      sum += c * weight;
      weight /= 128;
    }
    return sum;
  };

  auto longest_common_preifx =
    [&] (const std::vector<const std::string*>& strs) {
      uint32 prefix_len = INT_MAX;
      for (uint32 i = 0; i < strs.size() - 1; i++) {
        uint32 prefix = Strings::LongestCommonPrefix(*strs.at(i),
                                                     *strs.at(i + 1));
        prefix_len = std::min(prefix_len, prefix);
      }
      return prefix_len;
    };

  if (range.single_value) {
    if (*range.single_value == range.min && range.min == range.max) {
      return 2;
    } else if (*range.single_value >= range.min &&
               *range.single_value <= range.max) {
      uint32 prefix = longest_common_preifx({range.single_value.get(),
                                             &range.min,
                                             &range.max});
      // std::cout << numerise_str(range.max, prefix) << " - "
      //           << numerise_str(range.min, prefix) << std::endl;
      return 1.0 / (numerise_str(range.max, prefix) -
                    numerise_str(range.min, prefix) + 1);
    } else {
      return -1;
    }
  } else {
    if (!range.left_value || *range.left_value < range.min) {
      range.set_left_value(range.min);
      range.left_open = false;
    }
    if (!range.right_value || *range.right_value > range.max) {
      range.set_right_value(range.max);
      range.right_open = false;
    }

    if (*range.left_value > *range.right_value) {
      return -1;
    }
    if (*range.left_value == *range.right_value &&
        (range.left_open || range.right_open)) {
      return -1;
    }
    if (*range.left_value == range.min && *range.right_value == range.max) {
      return 2;
    }

    uint32 prefix = longest_common_preifx({range.left_value.get(),
                                           range.right_value.get(),
                                           &range.min,
                                           &range.max});
    // std::cout << (numerise_str(*range.right_value, prefix) -
    //               numerise_str(*range.left_value, prefix) + 1) << ", "
    //           << (numerise_str(range.max, prefix) -
    //               numerise_str(range.min, prefix) + 1)
    //           << std::endl;
    int boundary_num = 0;
    if (range.left_open && range.right_open) {
      boundary_num = -1;
    } else if (!range.left_open && !range.right_open) {
      boundary_num = 1;
    }
    return 1.0 *
            (numerise_str(*range.right_value, prefix) -
             numerise_str(*range.left_value, prefix) + boundary_num) /
            (numerise_str(range.max, prefix) -
             numerise_str(range.min, prefix) + 1);
  }
}

}  // namespace

// ******************************** Int ************************************* //
double IntField::EvaluateValueRatio(ValueRange<int>& range) {
  return EvaluateIntegerValueRatio<int>(range);
}

// ****************************** Long Int ********************************** //
double LongIntField::EvaluateValueRatio(ValueRange<int64>& range) {
  return EvaluateIntegerValueRatio<int64>(range);
}

// ******************************* Double *********************************** //
double DoubleField::EvaluateValueRatio(ValueRange<double>& range) {
  return EvaluateDoubleValueRatio(range);
}

// ******************************** Bool ************************************ //
double BoolField::EvaluateValueRatio(ValueRange<bool>& range) {
  return 0.5;
}

// ******************************** Char ************************************ //
double CharField::EvaluateValueRatio(ValueRange<char>& range) {
  return EvaluateIntegerValueRatio<char>(range);
}

// ******************************* String *********************************** //
// Comparable
bool StringField::operator<(const StringField& other) const {
  return value_ < other.value_;
}

bool StringField::operator>(const StringField& other) const {
  return value_ > other.value_;
}

bool StringField::operator<=(const StringField& other) const {
  return !(*this > other);
}

bool StringField::operator>=(const StringField& other) const {
  return !(*this < other);
}

bool StringField::operator==(const StringField& other) const {
  return value_ == other.value_;
}

bool StringField::operator!=(const StringField& other) const {
  return !(*this == other);
}

// Dump to memory
int StringField::DumpToMem(byte* buf) const {
  if (!buf) {
    return -1;
  }
  memcpy(buf, value_.c_str(), value_.length());
  buf[value_.length()] = '\0';
  return value_.length() + 1;
}

// Dump to memory
int StringField::LoadFromMem(const byte* buf) {
  if (!buf) {
    return -1;
  }
  value_ = std::string(reinterpret_cast<const char*>(buf));
  return value_.length() + 1;
}

double StringField::EvaluateValueRatio(ValueRange<std::string>& range) {
  return EvaluateStringValueRatio(range);
}

// ****************************** CharArray ********************************* //
CharArrayField::CharArrayField(int lenlimit) :
    length_limit_(lenlimit) {
  value_ = new char[length_limit_];
  memset(value_, 0, length_limit_);
}

CharArrayField::CharArrayField(const std::string& str, int lenlimit) :
    length_limit_(lenlimit) {
  if (!SetData(str.c_str(), str.length())) {
    throw std::runtime_error(
        "[Init CharArrayField Failed] - invalid lenlimit < src length");
  }
}

CharArrayField::CharArrayField(const char* src, int length, int lenlimit) :
    length_(length),
    length_limit_(lenlimit) {
  if (!SetData(src, length)) {
    throw std::runtime_error(
        "[Init CharArrayField Failed] - invalid lenlimit < src length");
  }
}

bool CharArrayField::SetData(const char* src, int length) {
  if (length > length_limit_) {
    LogERROR("Can't SetData() for CharArrayField - length %d > length_limit %d",
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

CharArrayField::~CharArrayField() {
  if (value_) {
    delete[] value_;
  }
}

// Comparable
bool CharArrayField::operator<(const CharArrayField& other) const {
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

bool CharArrayField::operator>(const CharArrayField& other) const {
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

bool CharArrayField::operator<=(const CharArrayField& other) const {
  return !(*this > other);
}

bool CharArrayField::operator>=(const CharArrayField& other) const {
  return !(*this < other);
}

bool CharArrayField::operator==(const CharArrayField& other) const {
  if (length_ != other.length_) {
    return false;
  }
  return strncmp(value_, other.value_, length_) == 0;
}

bool CharArrayField::operator!=(const CharArrayField& other) const {
  return !(*this == other);
}

// Dump to memory
int CharArrayField::DumpToMem(byte* buf) const {
  if (!buf) {
    return -1;
  }
  memcpy(buf, value_, length_limit_);
  return length_limit_;
}

// Dump to memory
int CharArrayField::LoadFromMem(const byte* buf) {
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

void CharArrayField::reset() {
  if (!value_) {
    return;
  }

  memset(value_, 0, length_limit_);
  length_ = 0;
}

double CharArrayField::EvaluateValueRatio(ValueRange<std::string>& range) {
  return EvaluateStringValueRatio(range);
}

}  // namepsace Schema
