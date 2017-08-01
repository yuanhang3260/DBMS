#include <memory>
#include <mutex>
#include <map>

#include "Proto/Descriptor.h"
#include "Proto/DescriptorsBuilder.h"
#include "Proto/MessageReflection.h"
#include "Proto/MessageFactory.h"

#include "Catalog_pb.h"

namespace {

const ::proto::MessageDescriptor* ValueLimit_descriptor_ = nullptr;
const ::proto::MessageReflection* ValueLimit_reflection_ = nullptr;
const ::proto::MessageDescriptor* TableField_descriptor_ = nullptr;
const ::proto::MessageReflection* TableField_reflection_ = nullptr;
const ::proto::MessageDescriptor* Index_descriptor_ = nullptr;
const ::proto::MessageReflection* Index_reflection_ = nullptr;
const ::proto::MessageDescriptor* TableInfo_descriptor_ = nullptr;
const ::proto::MessageReflection* TableInfo_reflection_ = nullptr;
const ::proto::MessageDescriptor* DatabaseCatalog_descriptor_ = nullptr;
const ::proto::MessageReflection* DatabaseCatalog_reflection_ = nullptr;

std::string GetProtoContent();

}  // namepsace

void static_init_default_instances_home_hy_Desktop_Projects_DBMS_src_Database_Catalog() {
  if (DB::ValueLimit::default_instance_ == nullptr) {
    DB::ValueLimit::default_instance_ = new DB::ValueLimit();
    DB::ValueLimit::default_instance_->InitAsDefaultInstance();
  }
  if (DB::TableField::default_instance_ == nullptr) {
    DB::TableField::default_instance_ = new DB::TableField();
    DB::TableField::default_instance_->InitAsDefaultInstance();
  }
  if (DB::Index::default_instance_ == nullptr) {
    DB::Index::default_instance_ = new DB::Index();
    DB::Index::default_instance_->InitAsDefaultInstance();
  }
  if (DB::TableInfo::default_instance_ == nullptr) {
    DB::TableInfo::default_instance_ = new DB::TableInfo();
    DB::TableInfo::default_instance_->InitAsDefaultInstance();
  }
  if (DB::DatabaseCatalog::default_instance_ == nullptr) {
    DB::DatabaseCatalog::default_instance_ = new DB::DatabaseCatalog();
    DB::DatabaseCatalog::default_instance_->InitAsDefaultInstance();
  }
}

void static_init_home_hy_Desktop_Projects_DBMS_src_Database_Catalog() {
  static bool already_called = false;
  if (already_called) return;
  already_called = true;

  ::proto::DescriptorsBuilder descriptors_builder(GetProtoContent());
  auto file_dscpt = descriptors_builder.BuildDescriptors();
  CHECK(file_dscpt != nullptr, "Build class descriptor failed.");
  ::proto::MessageFactory::RegisterParsedProtoFile(file_dscpt);

  static_init_default_instances_home_hy_Desktop_Projects_DBMS_src_Database_Catalog();

  // static init for class ValueLimit
  static const int ValueLimit_offsets_[7] = {
    PROTO_MESSAGE_FIELD_OFFSET(DB::ValueLimit, limit_int32_),
    PROTO_MESSAGE_FIELD_OFFSET(DB::ValueLimit, limit_int64_),
    PROTO_MESSAGE_FIELD_OFFSET(DB::ValueLimit, limit_double_),
    PROTO_MESSAGE_FIELD_OFFSET(DB::ValueLimit, limit_bool_),
    PROTO_MESSAGE_FIELD_OFFSET(DB::ValueLimit, limit_char_),
    PROTO_MESSAGE_FIELD_OFFSET(DB::ValueLimit, limit_str_),
    PROTO_MESSAGE_FIELD_OFFSET(DB::ValueLimit, limit_chararray_),
  };
  ValueLimit_descriptor_ = file_dscpt->FindMessageTypeByName("DB.ValueLimit");
  CHECK(ValueLimit_descriptor_ != nullptr, 
        "Can't find message descriptor for DB.ValueLimit");
  ValueLimit_reflection_ = 
      new ::proto::MessageReflection(
          ValueLimit_descriptor_,
          DB::ValueLimit::default_instance_,
          ValueLimit_offsets_,
          PROTO_MESSAGE_FIELD_OFFSET(DB::ValueLimit, has_bits_));
  ::proto::MessageFactory::RegisterGeneratedMessage(ValueLimit_reflection_);

  // static init for class TableField
  static const int TableField_offsets_[6] = {
    PROTO_MESSAGE_FIELD_OFFSET(DB::TableField, name_),
    PROTO_MESSAGE_FIELD_OFFSET(DB::TableField, index_),
    PROTO_MESSAGE_FIELD_OFFSET(DB::TableField, type_),
    PROTO_MESSAGE_FIELD_OFFSET(DB::TableField, size_),
    PROTO_MESSAGE_FIELD_OFFSET(DB::TableField, min_value_),
    PROTO_MESSAGE_FIELD_OFFSET(DB::TableField, max_value_),
  };
  TableField_descriptor_ = file_dscpt->FindMessageTypeByName("DB.TableField");
  CHECK(TableField_descriptor_ != nullptr, 
        "Can't find message descriptor for DB.TableField");
  TableField_reflection_ = 
      new ::proto::MessageReflection(
          TableField_descriptor_,
          DB::TableField::default_instance_,
          TableField_offsets_,
          PROTO_MESSAGE_FIELD_OFFSET(DB::TableField, has_bits_));
  ::proto::MessageFactory::RegisterGeneratedMessage(TableField_reflection_);

  // static init for class Index
  static const int Index_offsets_[1] = {
    PROTO_MESSAGE_FIELD_OFFSET(DB::Index, index_fields_),
  };
  Index_descriptor_ = file_dscpt->FindMessageTypeByName("DB.Index");
  CHECK(Index_descriptor_ != nullptr, 
        "Can't find message descriptor for DB.Index");
  Index_reflection_ = 
      new ::proto::MessageReflection(
          Index_descriptor_,
          DB::Index::default_instance_,
          Index_offsets_,
          PROTO_MESSAGE_FIELD_OFFSET(DB::Index, has_bits_));
  ::proto::MessageFactory::RegisterGeneratedMessage(Index_reflection_);

  // static init for class TableInfo
  static const int TableInfo_offsets_[5] = {
    PROTO_MESSAGE_FIELD_OFFSET(DB::TableInfo, name_),
    PROTO_MESSAGE_FIELD_OFFSET(DB::TableInfo, fields_),
    PROTO_MESSAGE_FIELD_OFFSET(DB::TableInfo, primary_index_),
    PROTO_MESSAGE_FIELD_OFFSET(DB::TableInfo, indexes_),
    PROTO_MESSAGE_FIELD_OFFSET(DB::TableInfo, num_records_),
  };
  TableInfo_descriptor_ = file_dscpt->FindMessageTypeByName("DB.TableInfo");
  CHECK(TableInfo_descriptor_ != nullptr, 
        "Can't find message descriptor for DB.TableInfo");
  TableInfo_reflection_ = 
      new ::proto::MessageReflection(
          TableInfo_descriptor_,
          DB::TableInfo::default_instance_,
          TableInfo_offsets_,
          PROTO_MESSAGE_FIELD_OFFSET(DB::TableInfo, has_bits_));
  ::proto::MessageFactory::RegisterGeneratedMessage(TableInfo_reflection_);

  // static init for class DatabaseCatalog
  static const int DatabaseCatalog_offsets_[2] = {
    PROTO_MESSAGE_FIELD_OFFSET(DB::DatabaseCatalog, name_),
    PROTO_MESSAGE_FIELD_OFFSET(DB::DatabaseCatalog, tables_),
  };
  DatabaseCatalog_descriptor_ = file_dscpt->FindMessageTypeByName("DB.DatabaseCatalog");
  CHECK(DatabaseCatalog_descriptor_ != nullptr, 
        "Can't find message descriptor for DB.DatabaseCatalog");
  DatabaseCatalog_reflection_ = 
      new ::proto::MessageReflection(
          DatabaseCatalog_descriptor_,
          DB::DatabaseCatalog::default_instance_,
          DatabaseCatalog_offsets_,
          PROTO_MESSAGE_FIELD_OFFSET(DB::DatabaseCatalog, has_bits_));
  ::proto::MessageFactory::RegisterGeneratedMessage(DatabaseCatalog_reflection_);

}

// Force static_init_home_hy_Desktop_Projects_DBMS_src_Database_Catalog() to be called at initialization time.
struct static_init_forcer_home_hy_Desktop_Projects_DBMS_src_Database_Catalog {
  static_init_forcer_home_hy_Desktop_Projects_DBMS_src_Database_Catalog() {
    static_init_home_hy_Desktop_Projects_DBMS_src_Database_Catalog();
  }
} static_init_forcer_home_hy_Desktop_Projects_DBMS_src_Database_Catalog_obj_;


namespace DB {

// ******************** ValueLimit ******************** //
// constructor
ValueLimit::ValueLimit() {
  for (unsigned int i = 0; i < sizeof(has_bits_); i++) {
    has_bits_[i] = 0;
  }
  default_instance_ = nullptr;
}

// copy constructor
ValueLimit::ValueLimit(const ValueLimit& other) {
  CopyFrom(other);
}

// move constructor
ValueLimit::ValueLimit(ValueLimit&& other) {
  MoveFrom(std::move(other));
}

// copy assignment
ValueLimit& ValueLimit::operator=(const ValueLimit& other) {
  CopyFrom(other);
  return *this;
}
// move assignment
ValueLimit& ValueLimit::operator=(ValueLimit&& other) {
  MoveFrom(std::move(other));
  return *this;
}

// New()
::proto::Message* ValueLimit::New() const {
  return reinterpret_cast<::proto::Message*>(new ValueLimit());
}

// CopyFrom()
void ValueLimit::CopyFrom(const ValueLimit& other) {
  limit_int32_ = other.limit_int32();
  limit_int64_ = other.limit_int64();
  limit_double_ = other.limit_double();
  limit_bool_ = other.limit_bool();
  limit_char_ = other.limit_char();
  limit_str_ = other.limit_str();
  limit_chararray_ = other.limit_chararray();
  for (unsigned int i = 0; i < sizeof(has_bits_); i++) {
    has_bits_[i] = other.has_bits_[i];
  }
}

// MoveFrom()
void ValueLimit::MoveFrom(ValueLimit&& other) {
  for (unsigned int i = 0; i < sizeof(has_bits_); i++) {
    has_bits_[i] = other.has_bits_[i];
  }
  limit_int32_ = other.limit_int32();
  limit_int64_ = other.limit_int64();
  limit_double_ = other.limit_double();
  limit_bool_ = other.limit_bool();
  limit_char_ = other.limit_char();
  limit_str_ = std::move(other.mutable_limit_str());
  limit_chararray_ = std::move(other.mutable_limit_chararray());
  for (unsigned int i = 0; i < sizeof(has_bits_); i++) {
    other.has_bits_[i] = 0;
  }
}

// Equals()
bool ValueLimit::Equals(const ValueLimit& other) const {
  for (unsigned int i = 0; i < sizeof(has_bits_); i++) {
    if (has_bits_[i] != other.has_bits_[i]) {
      return false;
    }
  }
  if (limit_int32_ != other.limit_int32_) {
    return false;
  }
  if (limit_int64_ != other.limit_int64_) {
    return false;
  }
  if (limit_double_ != other.limit_double_) {
    return false;
  }
  if (limit_bool_ != other.limit_bool_) {
    return false;
  }
  if (limit_char_ != other.limit_char_) {
    return false;
  }
  if (limit_str_ != other.limit_str_) {
    return false;
  }
  if (limit_chararray_ != other.limit_chararray_) {
    return false;
  }
  return true;
}

// Serialize()
::proto::SerializedMessage* ValueLimit::Serialize() const {
  return ValueLimit_reflection_->Serialize(this);
}

// DeSerialize()
void ValueLimit::DeSerialize(const char* buf, unsigned int size) {
  ValueLimit_reflection_->DeSerialize(this, buf, size);
}

// Print()
void ValueLimit::Print(int indent_num) const {
  PrintIndent(indent_num);
  std::cout << "ValueLimit " << "{" << std::endl;
  if (has_limit_int32()) {
    PrintIndent(indent_num + 1);
    std::cout << "limit_int32: " << limit_int32_ << std::endl;
  }
  if (has_limit_int64()) {
    PrintIndent(indent_num + 1);
    std::cout << "limit_int64: " << limit_int64_ << std::endl;
  }
  if (has_limit_double()) {
    PrintIndent(indent_num + 1);
    std::cout << "limit_double: " << limit_double_ << std::endl;
  }
  if (has_limit_bool()) {
    PrintIndent(indent_num + 1);
    std::cout << "limit_bool: " << limit_bool_ << std::endl;
  }
  if (has_limit_char()) {
    PrintIndent(indent_num + 1);
    std::cout << "limit_char: " << limit_char_ << std::endl;
  }
  if (has_limit_str()) {
    PrintIndent(indent_num + 1);
    std::cout << "limit_str: " << "\"" << limit_str_ << "\"" << std::endl;
  }
  if (has_limit_chararray()) {
    PrintIndent(indent_num + 1);
    std::cout << "limit_chararray: " << "\"" << limit_chararray_ << "\"" << std::endl;
  }
  PrintIndent(indent_num);
  std::cout << "}" << std::endl;
}

// InitAsDefaultInstance()
void ValueLimit::InitAsDefaultInstance() {
}

// swapper
void ValueLimit::Swap(ValueLimit* other) {
  // store has_bits
  char* buf = new char[2 * sizeof(has_bits_)];
  for (unsigned int i = 0; i < sizeof(has_bits_); i++) {
    buf[i] = has_bits_[i];
    buf[i + sizeof(has_bits_)] = other->has_bits_[i];
  }

  int32 limit_int32_tmp__ = other->limit_int32();
  other->set_limit_int32(limit_int32_);
  set_limit_int32(limit_int32_tmp__);

  int64 limit_int64_tmp__ = other->limit_int64();
  other->set_limit_int64(limit_int64_);
  set_limit_int64(limit_int64_tmp__);

  double limit_double_tmp__ = other->limit_double();
  other->set_limit_double(limit_double_);
  set_limit_double(limit_double_tmp__);

  bool limit_bool_tmp__ = other->limit_bool();
  other->set_limit_bool(limit_bool_);
  set_limit_bool(limit_bool_tmp__);

  char limit_char_tmp__ = other->limit_char();
  other->set_limit_char(limit_char_);
  set_limit_char(limit_char_tmp__);

  std::string limit_str_tmp__ = std::move(other->mutable_limit_str());
  other->mutable_limit_str() = std::move(limit_str_);
  limit_str_ = std::move(limit_str_tmp__);

  std::string limit_chararray_tmp__ = std::move(other->mutable_limit_chararray());
  other->mutable_limit_chararray() = std::move(limit_chararray_);
  limit_chararray_ = std::move(limit_chararray_tmp__);

  // swap has_bits
  for (unsigned int i = 0; i < sizeof(has_bits_); i++) {
    has_bits_[i] = buf[i + sizeof(has_bits_)];
    other->has_bits_[i] = buf[i];
  }
  delete buf;
}

// default_instance()
const ValueLimit& ValueLimit::default_instance() {
  if (default_instance_ == nullptr) {
    static_init_default_instances_home_hy_Desktop_Projects_DBMS_src_Database_Catalog();
  }
  return *default_instance_;
}

ValueLimit* ValueLimit::default_instance_ = nullptr;

const ::proto::MessageDescriptor* ValueLimit::GetDescriptor() const {
  return ValueLimit_descriptor_;
}

const ::proto::MessageReflection* ValueLimit::GetReflection() const {
  return ValueLimit_reflection_;
}

// destructor
ValueLimit::~ValueLimit() {
}

// "limit_int32" = 1
bool ValueLimit::has_limit_int32() const {
  return (has_bits_[0] & 0x2) != 0;
}

int32 ValueLimit::limit_int32() const {
  return limit_int32_;
}

void ValueLimit::set_limit_int32(int32 limit_int32) {
  limit_int32_ = limit_int32;
  has_bits_[0] |= 0x2;
}

void ValueLimit::clear_limit_int32() {
  limit_int32_ = 0;
  has_bits_[0] &= (~0x2);
}

// "limit_int64" = 2
bool ValueLimit::has_limit_int64() const {
  return (has_bits_[0] & 0x4) != 0;
}

int64 ValueLimit::limit_int64() const {
  return limit_int64_;
}

void ValueLimit::set_limit_int64(int64 limit_int64) {
  limit_int64_ = limit_int64;
  has_bits_[0] |= 0x4;
}

void ValueLimit::clear_limit_int64() {
  limit_int64_ = 0;
  has_bits_[0] &= (~0x4);
}

// "limit_double" = 3
bool ValueLimit::has_limit_double() const {
  return (has_bits_[0] & 0x8) != 0;
}

double ValueLimit::limit_double() const {
  return limit_double_;
}

void ValueLimit::set_limit_double(double limit_double) {
  limit_double_ = limit_double;
  has_bits_[0] |= 0x8;
}

void ValueLimit::clear_limit_double() {
  limit_double_ = 0;
  has_bits_[0] &= (~0x8);
}

// "limit_bool" = 4
bool ValueLimit::has_limit_bool() const {
  return (has_bits_[0] & 0x10) != 0;
}

bool ValueLimit::limit_bool() const {
  return limit_bool_;
}

void ValueLimit::set_limit_bool(bool limit_bool) {
  limit_bool_ = limit_bool;
  has_bits_[0] |= 0x10;
}

void ValueLimit::clear_limit_bool() {
  limit_bool_ = false;
  has_bits_[0] &= (~0x10);
}

// "limit_char" = 5
bool ValueLimit::has_limit_char() const {
  return (has_bits_[0] & 0x20) != 0;
}

char ValueLimit::limit_char() const {
  return limit_char_;
}

void ValueLimit::set_limit_char(char limit_char) {
  limit_char_ = limit_char;
  has_bits_[0] |= 0x20;
}

void ValueLimit::clear_limit_char() {
  limit_char_ = 0;
  has_bits_[0] &= (~0x20);
}

// "limit_str" = 6
bool ValueLimit::has_limit_str() const {
  return (has_bits_[0] & 0x40) != 0;
}

const std::string& ValueLimit::limit_str() const {
  return limit_str_;
}

void ValueLimit::set_limit_str(const std::string& limit_str) {
  limit_str_ = limit_str;
  has_bits_[0] |= 0x40;
}

void ValueLimit::set_limit_str(const char* limit_str) {
  limit_str_ = std::string(limit_str);
  has_bits_[0] |= 0x40;
}

void ValueLimit::set_limit_str(const char* limit_str, int size) {
  limit_str_ = std::string(limit_str, size);
  has_bits_[0] |= 0x40;
}

std::string ValueLimit::mutable_limit_str() {
  return limit_str_;
}

void ValueLimit::clear_limit_str() {
  limit_str_ = "";
  has_bits_[0] &= (~0x40);
}

// "limit_chararray" = 7
bool ValueLimit::has_limit_chararray() const {
  return (has_bits_[0] & 0x80) != 0;
}

const std::string& ValueLimit::limit_chararray() const {
  return limit_chararray_;
}

void ValueLimit::set_limit_chararray(const std::string& limit_chararray) {
  limit_chararray_ = limit_chararray;
  has_bits_[0] |= 0x80;
}

void ValueLimit::set_limit_chararray(const char* limit_chararray) {
  limit_chararray_ = std::string(limit_chararray);
  has_bits_[0] |= 0x80;
}

void ValueLimit::set_limit_chararray(const char* limit_chararray, int size) {
  limit_chararray_ = std::string(limit_chararray, size);
  has_bits_[0] |= 0x80;
}

std::string ValueLimit::mutable_limit_chararray() {
  return limit_chararray_;
}

void ValueLimit::clear_limit_chararray() {
  limit_chararray_ = "";
  has_bits_[0] &= (~0x80);
}

// ******************** TableField ******************** //
// constructor
TableField::TableField() {
  for (unsigned int i = 0; i < sizeof(has_bits_); i++) {
    has_bits_[i] = 0;
  }
  default_instance_ = nullptr;
}

// copy constructor
TableField::TableField(const TableField& other) {
  CopyFrom(other);
}

// move constructor
TableField::TableField(TableField&& other) {
  MoveFrom(std::move(other));
}

// copy assignment
TableField& TableField::operator=(const TableField& other) {
  CopyFrom(other);
  return *this;
}
// move assignment
TableField& TableField::operator=(TableField&& other) {
  MoveFrom(std::move(other));
  return *this;
}

// New()
::proto::Message* TableField::New() const {
  return reinterpret_cast<::proto::Message*>(new TableField());
}

// CopyFrom()
void TableField::CopyFrom(const TableField& other) {
  name_ = other.name();
  index_ = other.index();
  type_ = other.type();
  size_ = other.size();
  if (other.min_value_) {
    if (!min_value_) {
      min_value_ = new ValueLimit();
    }
    *min_value_ = other.min_value();
  }
  if (other.max_value_) {
    if (!max_value_) {
      max_value_ = new ValueLimit();
    }
    *max_value_ = other.max_value();
  }
  for (unsigned int i = 0; i < sizeof(has_bits_); i++) {
    has_bits_[i] = other.has_bits_[i];
  }
}

// MoveFrom()
void TableField::MoveFrom(TableField&& other) {
  for (unsigned int i = 0; i < sizeof(has_bits_); i++) {
    has_bits_[i] = other.has_bits_[i];
  }
  name_ = std::move(other.mutable_name());
  index_ = other.index();
  type_ = other.type();
  size_ = other.size();
  if (other.min_value_) {
    if (min_value_) {
      delete min_value_;
    }
    min_value_ = other.min_value_;
    other.min_value_ = nullptr;
  }
  if (other.max_value_) {
    if (max_value_) {
      delete max_value_;
    }
    max_value_ = other.max_value_;
    other.max_value_ = nullptr;
  }
  for (unsigned int i = 0; i < sizeof(has_bits_); i++) {
    other.has_bits_[i] = 0;
  }
}

// Equals()
bool TableField::Equals(const TableField& other) const {
  for (unsigned int i = 0; i < sizeof(has_bits_); i++) {
    if (has_bits_[i] != other.has_bits_[i]) {
      return false;
    }
  }
  if (name_ != other.name_) {
    return false;
  }
  if (index_ != other.index_) {
    return false;
  }
  if (type_ != other.type_) {
    return false;
  }
  if (size_ != other.size_) {
    return false;
  }
  if (min_value_ && other.min_value_ &&
      !min_value_->Equals(*other.min_value_)) {
    return false;
  }
  if (max_value_ && other.max_value_ &&
      !max_value_->Equals(*other.max_value_)) {
    return false;
  }
  return true;
}

// Serialize()
::proto::SerializedMessage* TableField::Serialize() const {
  return TableField_reflection_->Serialize(this);
}

// DeSerialize()
void TableField::DeSerialize(const char* buf, unsigned int size) {
  TableField_reflection_->DeSerialize(this, buf, size);
}

// Print()
void TableField::Print(int indent_num) const {
  PrintIndent(indent_num);
  std::cout << "TableField " << "{" << std::endl;
  if (has_name()) {
    PrintIndent(indent_num + 1);
    std::cout << "name: " << "\"" << name_ << "\"" << std::endl;
  }
  if (has_index()) {
    PrintIndent(indent_num + 1);
    std::cout << "index: " << index_ << std::endl;
  }
  if (has_type()) {
    PrintIndent(indent_num + 1);
    std::string enum_value =
        (reinterpret_cast<const proto::EnumDescriptor*>(
            TableField_descriptor_->FindFieldByName("type")->type_descriptor()))
                 ->EnumValueAsString(type_);
    std::cout << "type: " << enum_value << std::endl;
  }
  if (has_size()) {
    PrintIndent(indent_num + 1);
    std::cout << "size: " << size_ << std::endl;
  }
  if (has_min_value()) {
    PrintIndent(indent_num + 1);
    std::cout << "min_value: " << "*" << std::endl;
    min_value_->Print(indent_num + 1);
  }
  if (has_max_value()) {
    PrintIndent(indent_num + 1);
    std::cout << "max_value: " << "*" << std::endl;
    max_value_->Print(indent_num + 1);
  }
  PrintIndent(indent_num);
  std::cout << "}" << std::endl;
}

// InitAsDefaultInstance()
void TableField::InitAsDefaultInstance() {
  min_value_ = const_cast<ValueLimit*>(&ValueLimit::default_instance());
  max_value_ = const_cast<ValueLimit*>(&ValueLimit::default_instance());
}

// swapper
void TableField::Swap(TableField* other) {
  // store has_bits
  char* buf = new char[2 * sizeof(has_bits_)];
  for (unsigned int i = 0; i < sizeof(has_bits_); i++) {
    buf[i] = has_bits_[i];
    buf[i + sizeof(has_bits_)] = other->has_bits_[i];
  }

  std::string name_tmp__ = std::move(other->mutable_name());
  other->mutable_name() = std::move(name_);
  name_ = std::move(name_tmp__);

  int32 index_tmp__ = other->index();
  other->set_index(index_);
  set_index(index_tmp__);

  TableField::Type type_tmp__ = other->type();
  other->set_type(type_);
  set_type(type_tmp__);

  int32 size_tmp__ = other->size();
  other->set_size(size_);
  set_size(size_tmp__);

  ValueLimit* min_value_tmp__ = other->release_min_value();
  other->set_allocated_min_value(this->release_min_value());
  set_allocated_min_value(min_value_tmp__);

  ValueLimit* max_value_tmp__ = other->release_max_value();
  other->set_allocated_max_value(this->release_max_value());
  set_allocated_max_value(max_value_tmp__);

  // swap has_bits
  for (unsigned int i = 0; i < sizeof(has_bits_); i++) {
    has_bits_[i] = buf[i + sizeof(has_bits_)];
    other->has_bits_[i] = buf[i];
  }
  delete buf;
}

// default_instance()
const TableField& TableField::default_instance() {
  if (default_instance_ == nullptr) {
    static_init_default_instances_home_hy_Desktop_Projects_DBMS_src_Database_Catalog();
  }
  return *default_instance_;
}

TableField* TableField::default_instance_ = nullptr;

const ::proto::MessageDescriptor* TableField::GetDescriptor() const {
  return TableField_descriptor_;
}

const ::proto::MessageReflection* TableField::GetReflection() const {
  return TableField_reflection_;
}

// destructor
TableField::~TableField() {
  if (min_value_) {
    delete min_value_;
  }
  if (max_value_) {
    delete max_value_;
  }
}

// "name" = 1
bool TableField::has_name() const {
  return (has_bits_[0] & 0x2) != 0;
}

const std::string& TableField::name() const {
  return name_;
}

void TableField::set_name(const std::string& name) {
  name_ = name;
  has_bits_[0] |= 0x2;
}

void TableField::set_name(const char* name) {
  name_ = std::string(name);
  has_bits_[0] |= 0x2;
}

void TableField::set_name(const char* name, int size) {
  name_ = std::string(name, size);
  has_bits_[0] |= 0x2;
}

std::string TableField::mutable_name() {
  return name_;
}

void TableField::clear_name() {
  name_ = "";
  has_bits_[0] &= (~0x2);
}

// "index" = 2
bool TableField::has_index() const {
  return (has_bits_[0] & 0x4) != 0;
}

int32 TableField::index() const {
  return index_;
}

void TableField::set_index(int32 index) {
  index_ = index;
  has_bits_[0] |= 0x4;
}

void TableField::clear_index() {
  index_ = 0;
  has_bits_[0] &= (~0x4);
}

// "type" = 3
bool TableField::has_type() const {
  return (has_bits_[0] & 0x8) != 0;
}

TableField::Type TableField::type() const {
  return type_;
}

void TableField::set_type(TableField::Type type) {
  type_ = type;
  has_bits_[0] |= 0x8;
}

void TableField::clear_type() {
  type_ = TableField::UNKNOWN_TYPE;
  has_bits_[0] &= (~0x8);
}

// "size" = 4
bool TableField::has_size() const {
  return (has_bits_[0] & 0x10) != 0;
}

int32 TableField::size() const {
  return size_;
}

void TableField::set_size(int32 size) {
  size_ = size;
  has_bits_[0] |= 0x10;
}

void TableField::clear_size() {
  size_ = 0;
  has_bits_[0] &= (~0x10);
}

// "min_value" = 5
bool TableField::has_min_value() const {
  return (has_bits_[0] & 0x20) != 0;
}

const ValueLimit& TableField::min_value() const {
  if (has_min_value() && min_value_) {
    return *min_value_;
  }
  else {
    return ValueLimit::default_instance();
  }
}

ValueLimit* TableField::mutable_min_value() {
  if (has_min_value() && min_value_) {
    return min_value_;
  }
  else {
    min_value_ = new ValueLimit();
    has_bits_[0] |= 0x20;
    return min_value_;
  }
}

void TableField::set_allocated_min_value(ValueLimit* min_value) {
  if (min_value_) {
    delete min_value_;
  }
  min_value_ = min_value;
  if (min_value_) {
    has_bits_[0] |= 0x20;
  }
  else {
    has_bits_[0] &= (~0x20);
  }
}

ValueLimit* TableField::release_min_value() {
  ValueLimit* min_value_tmp__ = min_value_;
  min_value_ = nullptr;
  has_bits_[0] &= (~0x20);
  return min_value_tmp__;
}

void TableField::clear_min_value() {
  if (min_value_) {
    delete min_value_;
  }
  min_value_ = nullptr;
  has_bits_[0] &= (~0x20);
}

// "max_value" = 6
bool TableField::has_max_value() const {
  return (has_bits_[0] & 0x40) != 0;
}

const ValueLimit& TableField::max_value() const {
  if (has_max_value() && max_value_) {
    return *max_value_;
  }
  else {
    return ValueLimit::default_instance();
  }
}

ValueLimit* TableField::mutable_max_value() {
  if (has_max_value() && max_value_) {
    return max_value_;
  }
  else {
    max_value_ = new ValueLimit();
    has_bits_[0] |= 0x40;
    return max_value_;
  }
}

void TableField::set_allocated_max_value(ValueLimit* max_value) {
  if (max_value_) {
    delete max_value_;
  }
  max_value_ = max_value;
  if (max_value_) {
    has_bits_[0] |= 0x40;
  }
  else {
    has_bits_[0] &= (~0x40);
  }
}

ValueLimit* TableField::release_max_value() {
  ValueLimit* max_value_tmp__ = max_value_;
  max_value_ = nullptr;
  has_bits_[0] &= (~0x40);
  return max_value_tmp__;
}

void TableField::clear_max_value() {
  if (max_value_) {
    delete max_value_;
  }
  max_value_ = nullptr;
  has_bits_[0] &= (~0x40);
}

// ******************** Index ******************** //
// constructor
Index::Index() {
  for (unsigned int i = 0; i < sizeof(has_bits_); i++) {
    has_bits_[i] = 0;
  }
  default_instance_ = nullptr;
}

// copy constructor
Index::Index(const Index& other) {
  CopyFrom(other);
}

// move constructor
Index::Index(Index&& other) {
  MoveFrom(std::move(other));
}

// copy assignment
Index& Index::operator=(const Index& other) {
  CopyFrom(other);
  return *this;
}
// move assignment
Index& Index::operator=(Index&& other) {
  MoveFrom(std::move(other));
  return *this;
}

// New()
::proto::Message* Index::New() const {
  return reinterpret_cast<::proto::Message*>(new Index());
}

// CopyFrom()
void Index::CopyFrom(const Index& other) {
  index_fields_ = other.index_fields();
  for (unsigned int i = 0; i < sizeof(has_bits_); i++) {
    has_bits_[i] = other.has_bits_[i];
  }
}

// MoveFrom()
void Index::MoveFrom(Index&& other) {
  for (unsigned int i = 0; i < sizeof(has_bits_); i++) {
    has_bits_[i] = other.has_bits_[i];
  }
  index_fields_ = std::move(other.mutable_index_fields());
  for (unsigned int i = 0; i < sizeof(has_bits_); i++) {
    other.has_bits_[i] = 0;
  }
}

// Equals()
bool Index::Equals(const Index& other) const {
  for (unsigned int i = 0; i < sizeof(has_bits_); i++) {
    if (has_bits_[i] != other.has_bits_[i]) {
      return false;
    }
  }
  for (unsigned int i = 0; i < index_fields_.size(); i++) {
    if (index_fields_.at(i) != other.index_fields_.at(i)) {
      return false;
    }
  }
  return true;
}

// Serialize()
::proto::SerializedMessage* Index::Serialize() const {
  return Index_reflection_->Serialize(this);
}

// DeSerialize()
void Index::DeSerialize(const char* buf, unsigned int size) {
  Index_reflection_->DeSerialize(this, buf, size);
}

// Print()
void Index::Print(int indent_num) const {
  PrintIndent(indent_num);
  std::cout << "Index " << "{" << std::endl;
  if (index_fields_size() > 0) {
    PrintIndent(indent_num + 1);
    std::cout << "index_fields: " << "[";
    for (const auto& ele: index_fields_) {
        std::cout << ele << ", ";
    }
    std::cout << "]" << std::endl;
  }
  PrintIndent(indent_num);
  std::cout << "}" << std::endl;
}

// InitAsDefaultInstance()
void Index::InitAsDefaultInstance() {
}

// swapper
void Index::Swap(Index* other) {
  // store has_bits
  char* buf = new char[2 * sizeof(has_bits_)];
  for (unsigned int i = 0; i < sizeof(has_bits_); i++) {
    buf[i] = has_bits_[i];
    buf[i + sizeof(has_bits_)] = other->has_bits_[i];
  }

  ::proto::RepeatedField<int32> index_fields_tmp__ = std::move(other->mutable_index_fields());
  other->mutable_index_fields() = std::move(index_fields_);
  index_fields_ = std::move(index_fields_tmp__);

  // swap has_bits
  for (unsigned int i = 0; i < sizeof(has_bits_); i++) {
    has_bits_[i] = buf[i + sizeof(has_bits_)];
    other->has_bits_[i] = buf[i];
  }
  delete buf;
}

// default_instance()
const Index& Index::default_instance() {
  if (default_instance_ == nullptr) {
    static_init_default_instances_home_hy_Desktop_Projects_DBMS_src_Database_Catalog();
  }
  return *default_instance_;
}

Index* Index::default_instance_ = nullptr;

const ::proto::MessageDescriptor* Index::GetDescriptor() const {
  return Index_descriptor_;
}

const ::proto::MessageReflection* Index::GetReflection() const {
  return Index_reflection_;
}

// destructor
Index::~Index() {
}

// "index_fields" = 1
int Index::index_fields_size() const {
  return index_fields_.size();
}

int32 Index::index_fields(int index) const {
  return index_fields_.Get(index);
}

void Index::set_index_fields(int index, int32 value) {
  if ((int)index_fields_.size() > index) {
    index_fields_.Set(index, value);
  }
}

void Index::add_index_fields(int32 value) {
   index_fields_.Add(value);
}

void Index::clear_index_fields() {
  index_fields_ .Clear();
}

const ::proto::RepeatedField<int32>& Index::index_fields() const {
  return index_fields_;
}

::proto::RepeatedField<int32>& Index::mutable_index_fields() {
  return index_fields_;
}

// ******************** TableInfo ******************** //
// constructor
TableInfo::TableInfo() {
  for (unsigned int i = 0; i < sizeof(has_bits_); i++) {
    has_bits_[i] = 0;
  }
  default_instance_ = nullptr;
}

// copy constructor
TableInfo::TableInfo(const TableInfo& other) {
  CopyFrom(other);
}

// move constructor
TableInfo::TableInfo(TableInfo&& other) {
  MoveFrom(std::move(other));
}

// copy assignment
TableInfo& TableInfo::operator=(const TableInfo& other) {
  CopyFrom(other);
  return *this;
}
// move assignment
TableInfo& TableInfo::operator=(TableInfo&& other) {
  MoveFrom(std::move(other));
  return *this;
}

// New()
::proto::Message* TableInfo::New() const {
  return reinterpret_cast<::proto::Message*>(new TableInfo());
}

// CopyFrom()
void TableInfo::CopyFrom(const TableInfo& other) {
  name_ = other.name();
  for (const TableField* p: other.fields().GetElements()) {
    fields_.AddAllocated(new TableField(*p));
  }
  if (other.primary_index_) {
    if (!primary_index_) {
      primary_index_ = new Index();
    }
    *primary_index_ = other.primary_index();
  }
  for (const Index* p: other.indexes().GetElements()) {
    indexes_.AddAllocated(new Index(*p));
  }
  num_records_ = other.num_records();
  for (unsigned int i = 0; i < sizeof(has_bits_); i++) {
    has_bits_[i] = other.has_bits_[i];
  }
}

// MoveFrom()
void TableInfo::MoveFrom(TableInfo&& other) {
  for (unsigned int i = 0; i < sizeof(has_bits_); i++) {
    has_bits_[i] = other.has_bits_[i];
  }
  name_ = std::move(other.mutable_name());
  fields_ = std::move(other.mutable_fields());
  if (other.primary_index_) {
    if (primary_index_) {
      delete primary_index_;
    }
    primary_index_ = other.primary_index_;
    other.primary_index_ = nullptr;
  }
  indexes_ = std::move(other.mutable_indexes());
  num_records_ = other.num_records();
  for (unsigned int i = 0; i < sizeof(has_bits_); i++) {
    other.has_bits_[i] = 0;
  }
}

// Equals()
bool TableInfo::Equals(const TableInfo& other) const {
  for (unsigned int i = 0; i < sizeof(has_bits_); i++) {
    if (has_bits_[i] != other.has_bits_[i]) {
      return false;
    }
  }
  if (name_ != other.name_) {
    return false;
  }
  for (unsigned int i = 0; i < fields_.size(); i++) {
    if (!fields_.at(i).Equals(other.fields_.at(i))) {
      return false;
    }
  }
  if (primary_index_ && other.primary_index_ &&
      !primary_index_->Equals(*other.primary_index_)) {
    return false;
  }
  for (unsigned int i = 0; i < indexes_.size(); i++) {
    if (!indexes_.at(i).Equals(other.indexes_.at(i))) {
      return false;
    }
  }
  if (num_records_ != other.num_records_) {
    return false;
  }
  return true;
}

// Serialize()
::proto::SerializedMessage* TableInfo::Serialize() const {
  return TableInfo_reflection_->Serialize(this);
}

// DeSerialize()
void TableInfo::DeSerialize(const char* buf, unsigned int size) {
  TableInfo_reflection_->DeSerialize(this, buf, size);
}

// Print()
void TableInfo::Print(int indent_num) const {
  PrintIndent(indent_num);
  std::cout << "TableInfo " << "{" << std::endl;
  if (has_name()) {
    PrintIndent(indent_num + 1);
    std::cout << "name: " << "\"" << name_ << "\"" << std::endl;
  }
  if (fields_size() > 0) {
    PrintIndent(indent_num + 1);
    std::cout << "fields: " << "[***]" << std::endl;
    for (const auto& ele: fields_) {
        ele.Print(indent_num + 1);
    }
  }
  if (has_primary_index()) {
    PrintIndent(indent_num + 1);
    std::cout << "primary_index: " << "*" << std::endl;
    primary_index_->Print(indent_num + 1);
  }
  if (indexes_size() > 0) {
    PrintIndent(indent_num + 1);
    std::cout << "indexes: " << "[***]" << std::endl;
    for (const auto& ele: indexes_) {
        ele.Print(indent_num + 1);
    }
  }
  if (has_num_records()) {
    PrintIndent(indent_num + 1);
    std::cout << "num_records: " << num_records_ << std::endl;
  }
  PrintIndent(indent_num);
  std::cout << "}" << std::endl;
}

// InitAsDefaultInstance()
void TableInfo::InitAsDefaultInstance() {
  primary_index_ = const_cast<Index*>(&Index::default_instance());
}

// swapper
void TableInfo::Swap(TableInfo* other) {
  // store has_bits
  char* buf = new char[2 * sizeof(has_bits_)];
  for (unsigned int i = 0; i < sizeof(has_bits_); i++) {
    buf[i] = has_bits_[i];
    buf[i + sizeof(has_bits_)] = other->has_bits_[i];
  }

  std::string name_tmp__ = std::move(other->mutable_name());
  other->mutable_name() = std::move(name_);
  name_ = std::move(name_tmp__);

  ::proto::RepeatedPtrField<TableField> fields_tmp__ = std::move(other->mutable_fields());
  other->mutable_fields() = std::move(fields_);
  fields_ = std::move(fields_tmp__);

  Index* primary_index_tmp__ = other->release_primary_index();
  other->set_allocated_primary_index(this->release_primary_index());
  set_allocated_primary_index(primary_index_tmp__);

  ::proto::RepeatedPtrField<Index> indexes_tmp__ = std::move(other->mutable_indexes());
  other->mutable_indexes() = std::move(indexes_);
  indexes_ = std::move(indexes_tmp__);

  int32 num_records_tmp__ = other->num_records();
  other->set_num_records(num_records_);
  set_num_records(num_records_tmp__);

  // swap has_bits
  for (unsigned int i = 0; i < sizeof(has_bits_); i++) {
    has_bits_[i] = buf[i + sizeof(has_bits_)];
    other->has_bits_[i] = buf[i];
  }
  delete buf;
}

// default_instance()
const TableInfo& TableInfo::default_instance() {
  if (default_instance_ == nullptr) {
    static_init_default_instances_home_hy_Desktop_Projects_DBMS_src_Database_Catalog();
  }
  return *default_instance_;
}

TableInfo* TableInfo::default_instance_ = nullptr;

const ::proto::MessageDescriptor* TableInfo::GetDescriptor() const {
  return TableInfo_descriptor_;
}

const ::proto::MessageReflection* TableInfo::GetReflection() const {
  return TableInfo_reflection_;
}

// destructor
TableInfo::~TableInfo() {
  if (primary_index_) {
    delete primary_index_;
  }
}

// "name" = 1
bool TableInfo::has_name() const {
  return (has_bits_[0] & 0x2) != 0;
}

const std::string& TableInfo::name() const {
  return name_;
}

void TableInfo::set_name(const std::string& name) {
  name_ = name;
  has_bits_[0] |= 0x2;
}

void TableInfo::set_name(const char* name) {
  name_ = std::string(name);
  has_bits_[0] |= 0x2;
}

void TableInfo::set_name(const char* name, int size) {
  name_ = std::string(name, size);
  has_bits_[0] |= 0x2;
}

std::string TableInfo::mutable_name() {
  return name_;
}

void TableInfo::clear_name() {
  name_ = "";
  has_bits_[0] &= (~0x2);
}

// "fields" = 2
int TableInfo::fields_size() const {
  return fields_.size();
}

const TableField& TableInfo::fields(int index) const {
  return fields_.Get(index);
}

TableField* TableInfo::add_fields() {
  return fields_.Add();
}

TableField* TableInfo::mutable_fields(int index) {
  return fields_.GetMutable(index);
}

void TableInfo::clear_fields() {
  fields_.Clear();
}

const ::proto::RepeatedPtrField<TableField>& TableInfo::fields() const {
  return fields_;
}

::proto::RepeatedPtrField<TableField>& TableInfo::mutable_fields() {
  return fields_;
}

// "primary_index" = 3
bool TableInfo::has_primary_index() const {
  return (has_bits_[0] & 0x8) != 0;
}

const Index& TableInfo::primary_index() const {
  if (has_primary_index() && primary_index_) {
    return *primary_index_;
  }
  else {
    return Index::default_instance();
  }
}

Index* TableInfo::mutable_primary_index() {
  if (has_primary_index() && primary_index_) {
    return primary_index_;
  }
  else {
    primary_index_ = new Index();
    has_bits_[0] |= 0x8;
    return primary_index_;
  }
}

void TableInfo::set_allocated_primary_index(Index* primary_index) {
  if (primary_index_) {
    delete primary_index_;
  }
  primary_index_ = primary_index;
  if (primary_index_) {
    has_bits_[0] |= 0x8;
  }
  else {
    has_bits_[0] &= (~0x8);
  }
}

Index* TableInfo::release_primary_index() {
  Index* primary_index_tmp__ = primary_index_;
  primary_index_ = nullptr;
  has_bits_[0] &= (~0x8);
  return primary_index_tmp__;
}

void TableInfo::clear_primary_index() {
  if (primary_index_) {
    delete primary_index_;
  }
  primary_index_ = nullptr;
  has_bits_[0] &= (~0x8);
}

// "indexes" = 4
int TableInfo::indexes_size() const {
  return indexes_.size();
}

const Index& TableInfo::indexes(int index) const {
  return indexes_.Get(index);
}

Index* TableInfo::add_indexes() {
  return indexes_.Add();
}

Index* TableInfo::mutable_indexes(int index) {
  return indexes_.GetMutable(index);
}

void TableInfo::clear_indexes() {
  indexes_.Clear();
}

const ::proto::RepeatedPtrField<Index>& TableInfo::indexes() const {
  return indexes_;
}

::proto::RepeatedPtrField<Index>& TableInfo::mutable_indexes() {
  return indexes_;
}

// "num_records" = 5
bool TableInfo::has_num_records() const {
  return (has_bits_[0] & 0x20) != 0;
}

int32 TableInfo::num_records() const {
  return num_records_;
}

void TableInfo::set_num_records(int32 num_records) {
  num_records_ = num_records;
  has_bits_[0] |= 0x20;
}

void TableInfo::clear_num_records() {
  num_records_ = 0;
  has_bits_[0] &= (~0x20);
}

// ******************** DatabaseCatalog ******************** //
// constructor
DatabaseCatalog::DatabaseCatalog() {
  for (unsigned int i = 0; i < sizeof(has_bits_); i++) {
    has_bits_[i] = 0;
  }
  default_instance_ = nullptr;
}

// copy constructor
DatabaseCatalog::DatabaseCatalog(const DatabaseCatalog& other) {
  CopyFrom(other);
}

// move constructor
DatabaseCatalog::DatabaseCatalog(DatabaseCatalog&& other) {
  MoveFrom(std::move(other));
}

// copy assignment
DatabaseCatalog& DatabaseCatalog::operator=(const DatabaseCatalog& other) {
  CopyFrom(other);
  return *this;
}
// move assignment
DatabaseCatalog& DatabaseCatalog::operator=(DatabaseCatalog&& other) {
  MoveFrom(std::move(other));
  return *this;
}

// New()
::proto::Message* DatabaseCatalog::New() const {
  return reinterpret_cast<::proto::Message*>(new DatabaseCatalog());
}

// CopyFrom()
void DatabaseCatalog::CopyFrom(const DatabaseCatalog& other) {
  name_ = other.name();
  for (const TableInfo* p: other.tables().GetElements()) {
    tables_.AddAllocated(new TableInfo(*p));
  }
  for (unsigned int i = 0; i < sizeof(has_bits_); i++) {
    has_bits_[i] = other.has_bits_[i];
  }
}

// MoveFrom()
void DatabaseCatalog::MoveFrom(DatabaseCatalog&& other) {
  for (unsigned int i = 0; i < sizeof(has_bits_); i++) {
    has_bits_[i] = other.has_bits_[i];
  }
  name_ = std::move(other.mutable_name());
  tables_ = std::move(other.mutable_tables());
  for (unsigned int i = 0; i < sizeof(has_bits_); i++) {
    other.has_bits_[i] = 0;
  }
}

// Equals()
bool DatabaseCatalog::Equals(const DatabaseCatalog& other) const {
  for (unsigned int i = 0; i < sizeof(has_bits_); i++) {
    if (has_bits_[i] != other.has_bits_[i]) {
      return false;
    }
  }
  if (name_ != other.name_) {
    return false;
  }
  for (unsigned int i = 0; i < tables_.size(); i++) {
    if (!tables_.at(i).Equals(other.tables_.at(i))) {
      return false;
    }
  }
  return true;
}

// Serialize()
::proto::SerializedMessage* DatabaseCatalog::Serialize() const {
  return DatabaseCatalog_reflection_->Serialize(this);
}

// DeSerialize()
void DatabaseCatalog::DeSerialize(const char* buf, unsigned int size) {
  DatabaseCatalog_reflection_->DeSerialize(this, buf, size);
}

// Print()
void DatabaseCatalog::Print(int indent_num) const {
  PrintIndent(indent_num);
  std::cout << "DatabaseCatalog " << "{" << std::endl;
  if (has_name()) {
    PrintIndent(indent_num + 1);
    std::cout << "name: " << "\"" << name_ << "\"" << std::endl;
  }
  if (tables_size() > 0) {
    PrintIndent(indent_num + 1);
    std::cout << "tables: " << "[***]" << std::endl;
    for (const auto& ele: tables_) {
        ele.Print(indent_num + 1);
    }
  }
  PrintIndent(indent_num);
  std::cout << "}" << std::endl;
}

// InitAsDefaultInstance()
void DatabaseCatalog::InitAsDefaultInstance() {
}

// swapper
void DatabaseCatalog::Swap(DatabaseCatalog* other) {
  // store has_bits
  char* buf = new char[2 * sizeof(has_bits_)];
  for (unsigned int i = 0; i < sizeof(has_bits_); i++) {
    buf[i] = has_bits_[i];
    buf[i + sizeof(has_bits_)] = other->has_bits_[i];
  }

  std::string name_tmp__ = std::move(other->mutable_name());
  other->mutable_name() = std::move(name_);
  name_ = std::move(name_tmp__);

  ::proto::RepeatedPtrField<TableInfo> tables_tmp__ = std::move(other->mutable_tables());
  other->mutable_tables() = std::move(tables_);
  tables_ = std::move(tables_tmp__);

  // swap has_bits
  for (unsigned int i = 0; i < sizeof(has_bits_); i++) {
    has_bits_[i] = buf[i + sizeof(has_bits_)];
    other->has_bits_[i] = buf[i];
  }
  delete buf;
}

// default_instance()
const DatabaseCatalog& DatabaseCatalog::default_instance() {
  if (default_instance_ == nullptr) {
    static_init_default_instances_home_hy_Desktop_Projects_DBMS_src_Database_Catalog();
  }
  return *default_instance_;
}

DatabaseCatalog* DatabaseCatalog::default_instance_ = nullptr;

const ::proto::MessageDescriptor* DatabaseCatalog::GetDescriptor() const {
  return DatabaseCatalog_descriptor_;
}

const ::proto::MessageReflection* DatabaseCatalog::GetReflection() const {
  return DatabaseCatalog_reflection_;
}

// destructor
DatabaseCatalog::~DatabaseCatalog() {
}

// "name" = 1
bool DatabaseCatalog::has_name() const {
  return (has_bits_[0] & 0x2) != 0;
}

const std::string& DatabaseCatalog::name() const {
  return name_;
}

void DatabaseCatalog::set_name(const std::string& name) {
  name_ = name;
  has_bits_[0] |= 0x2;
}

void DatabaseCatalog::set_name(const char* name) {
  name_ = std::string(name);
  has_bits_[0] |= 0x2;
}

void DatabaseCatalog::set_name(const char* name, int size) {
  name_ = std::string(name, size);
  has_bits_[0] |= 0x2;
}

std::string DatabaseCatalog::mutable_name() {
  return name_;
}

void DatabaseCatalog::clear_name() {
  name_ = "";
  has_bits_[0] &= (~0x2);
}

// "tables" = 2
int DatabaseCatalog::tables_size() const {
  return tables_.size();
}

const TableInfo& DatabaseCatalog::tables(int index) const {
  return tables_.Get(index);
}

TableInfo* DatabaseCatalog::add_tables() {
  return tables_.Add();
}

TableInfo* DatabaseCatalog::mutable_tables(int index) {
  return tables_.GetMutable(index);
}

void DatabaseCatalog::clear_tables() {
  tables_.Clear();
}

const ::proto::RepeatedPtrField<TableInfo>& DatabaseCatalog::tables() const {
  return tables_;
}

::proto::RepeatedPtrField<TableInfo>& DatabaseCatalog::mutable_tables() {
  return tables_;
}

}  // namespace DB

namespace {

std::string GetProtoContent() {
  return "package DB;\n"
"\n"
"message ValueLimit {\n"
"  optional int32 limit_int32 = 1;\n"
"  optional int64 limit_int64 = 2;\n"
"  optional double limit_double = 3;\n"
"  optional bool limit_bool = 4;\n"
"  optional char limit_char = 5;\n"
"  optional string limit_str = 6;\n"
"  optional string limit_chararray = 7;\n"
"}\n"
"\n"
"message TableField {\n"
"  enum Type {\n"
"    UNKNOWN_TYPE,\n"
"    INT,\n"
"    LONGINT,\n"
"    DOUBLE,\n"
"    CHAR,\n"
"    STRING,\n"
"    BOOL,\n"
"    CHARARRAY,\n"
"  }\n"
"\n"
"  optional string name = 1;\n"
"  optional int32 index = 2;\n"
"  optional Type type = 3;\n"
"  optional int32 size = 4;  // Only meaningful for CharArray - length limit.\n"
"\n"
"  optional ValueLimit min_value = 5;\n"
"  optional ValueLimit max_value = 6;\n"
"}\n"
"\n"
"message Index {\n"
"  repeated int32 index_fields = 1;\n"
"}\n"
"\n"
"message TableInfo {\n"
"  optional string name = 1;\n"
"  repeated TableField fields = 2;\n"
"\n"
"  optional Index primary_index = 3;\n"
"  repeated Index indexes = 4;\n"
"\n"
"  optional int32 num_records = 5;\n"
"\n"
"  // TODO: foreign keys, constraints, assertions ...\n"
"}\n"
"\n"
"message DatabaseCatalog {\n"
"  optional string name = 1;\n"
"  repeated TableInfo tables = 2;\n"
"}\n"
"\n"
;
}

}  // namepsace

