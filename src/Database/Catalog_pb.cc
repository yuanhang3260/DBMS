#include <memory>
#include <mutex>
#include <map>

#include "Proto/Descriptor.h"
#include "Proto/DescriptorsBuilder.h"
#include "Proto/MessageReflection.h"
#include "Proto/MessageFactory.h"

#include "Catalog_pb.h"

namespace {

const ::proto::MessageDescriptor* TableField_descriptor_ = nullptr;
const ::proto::MessageReflection* TableField_reflection_ = nullptr;
const ::proto::MessageDescriptor* TableInfo_descriptor_ = nullptr;
const ::proto::MessageReflection* TableInfo_reflection_ = nullptr;
const ::proto::MessageDescriptor* DatabaseCatalog_descriptor_ = nullptr;
const ::proto::MessageReflection* DatabaseCatalog_reflection_ = nullptr;

std::string GetProtoContent();

}  // namepsace

void static_init_default_instances_home_hy_Desktop_Projects_DBMS_src_Database_Catalog() {
  if (DB::TableField::default_instance_ == nullptr) {
    DB::TableField::default_instance_ = new DB::TableField();
    DB::TableField::default_instance_->InitAsDefaultInstance();
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

  // static init for class TableField
  static const int TableField_offsets_[4] = {
    PROTO_MESSAGE_FIELD_OFFSET(DB::TableField, name_),
    PROTO_MESSAGE_FIELD_OFFSET(DB::TableField, index_),
    PROTO_MESSAGE_FIELD_OFFSET(DB::TableField, type_),
    PROTO_MESSAGE_FIELD_OFFSET(DB::TableField, size_),
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

  // static init for class TableInfo
  static const int TableInfo_offsets_[3] = {
    PROTO_MESSAGE_FIELD_OFFSET(DB::TableInfo, name_),
    PROTO_MESSAGE_FIELD_OFFSET(DB::TableInfo, fields_),
    PROTO_MESSAGE_FIELD_OFFSET(DB::TableInfo, primary_key_indexes_),
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
  PrintIndent(indent_num);
  std::cout << "}" << std::endl;
}

// InitAsDefaultInstance()
void TableField::InitAsDefaultInstance() {
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

  int index_tmp__ = other->index();
  other->set_index(index_);
  set_index(index_tmp__);

  TableField::Type type_tmp__ = other->type();
  other->set_type(type_);
  set_type(type_tmp__);

  int size_tmp__ = other->size();
  other->set_size(size_);
  set_size(size_tmp__);

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

int TableField::index() const {
  return index_;
}

void TableField::set_index(int index) {
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

int TableField::size() const {
  return size_;
}

void TableField::set_size(int size) {
  size_ = size;
  has_bits_[0] |= 0x10;
}

void TableField::clear_size() {
  size_ = 0;
  has_bits_[0] &= (~0x10);
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
  primary_key_indexes_ = other.primary_key_indexes();
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
  primary_key_indexes_ = std::move(other.mutable_primary_key_indexes());
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
  for (unsigned int i = 0; i < primary_key_indexes_.size(); i++) {
    if (primary_key_indexes_.at(i) != other.primary_key_indexes_.at(i)) {
      return false;
    }
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
  if (primary_key_indexes_size() > 0) {
    PrintIndent(indent_num + 1);
    std::cout << "primary_key_indexes: " << "[";
    for (const auto& ele: primary_key_indexes_) {
        std::cout << ele << ", ";
    }
    std::cout << "]" << std::endl;
  }
  PrintIndent(indent_num);
  std::cout << "}" << std::endl;
}

// InitAsDefaultInstance()
void TableInfo::InitAsDefaultInstance() {
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

  ::proto::RepeatedField<int> primary_key_indexes_tmp__ = std::move(other->mutable_primary_key_indexes());
  other->mutable_primary_key_indexes() = std::move(primary_key_indexes_);
  primary_key_indexes_ = std::move(primary_key_indexes_tmp__);

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

// "primary_key_indexes" = 3
int TableInfo::primary_key_indexes_size() const {
  return primary_key_indexes_.size();
}

int TableInfo::primary_key_indexes(int index) const {
  return primary_key_indexes_.Get(index);
}

void TableInfo::set_primary_key_indexes(int index, int value) {
  if ((int)primary_key_indexes_.size() > index) {
    primary_key_indexes_.Set(index, value);
  }
}

void TableInfo::add_primary_key_indexes(int value) {
   primary_key_indexes_.Add(value);
}

void TableInfo::clear_primary_key_indexes() {
  primary_key_indexes_ .Clear();
}

const ::proto::RepeatedField<int>& TableInfo::primary_key_indexes() const {
  return primary_key_indexes_;
}

::proto::RepeatedField<int>& TableInfo::mutable_primary_key_indexes() {
  return primary_key_indexes_;
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
"}\n"
"\n"
"message TableInfo {\n"
"  optional string name = 1;\n"
"  repeated TableField fields = 2;\n"
"\n"
"  repeated int32 primary_key_indexes = 3;\n"
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

