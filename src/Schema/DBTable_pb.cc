#include <memory>
#include <mutex>
#include <map>

#include "Compiler/Message.h"
#include "Compiler/ProtoParser.h"
#include "Proto/MessageReflection.h"
#include "Proto/MessageFactory.h"

#include "DBTable_pb.h"

namespace {

std::shared_ptr<::proto::ProtoParser::Message> TableField_descriptor_;
std::shared_ptr<::proto::MessageReflection> TableField_reflection_;
std::shared_ptr<::proto::ProtoParser::Message> TableSchema_descriptor_;
std::shared_ptr<::proto::MessageReflection> TableSchema_reflection_;

}  // namepsace

void static_init_default_instances_home_hy_Desktop_Projects_DBMS_src_Schema_DBTable() {
  if (Schema::TableField::default_instance_ == NULL) {
    Schema::TableField::default_instance_ = new Schema::TableField();
    Schema::TableField::default_instance_->InitAsDefaultInstance();
  }
  if (Schema::TableSchema::default_instance_ == NULL) {
    Schema::TableSchema::default_instance_ = new Schema::TableSchema();
    Schema::TableSchema::default_instance_->InitAsDefaultInstance();
  }
}

void static_init_home_hy_Desktop_Projects_DBMS_src_Schema_DBTable() {
  static bool already_called = false;
  if (already_called) return;
  already_called = true;

  ::proto::ProtoParser::ProtoParser parser(
      ::proto::ProtoParser::CPP,
      "/home/hy/Desktop/Projects/DBMS/src/Schema/DBTable.proto");
  CHECK(parser.ParseProto(),
        "static class initialization for /home/hy/Desktop/Projects/DBMS/src/Schema/DBTable.proto failed");

  static_init_default_instances_home_hy_Desktop_Projects_DBMS_src_Schema_DBTable();

  int i = 0;
  // static init for class TableField
  static const int TableField_offsets_[4] = {
    PROTO_MESSAGE_FIELD_OFFSET(Schema::TableField, name_),
    PROTO_MESSAGE_FIELD_OFFSET(Schema::TableField, index_),
    PROTO_MESSAGE_FIELD_OFFSET(Schema::TableField, type_),
    PROTO_MESSAGE_FIELD_OFFSET(Schema::TableField, size_),
  };
  i = 0;
  for (auto& field: parser.mutable_messages_list()[0]->mutable_fields_list()) {
    field->set_field_offset(TableField_offsets_[i++]);
  }
  TableField_descriptor_ = parser.mutable_messages_list()[0];
  TableField_reflection_.reset(
      new ::proto::MessageReflection(
          TableField_descriptor_,
          Schema::TableField::default_instance_,
          PROTO_MESSAGE_FIELD_OFFSET(Schema::TableField, has_bits_))
  );
  ::proto::MessageFactory::RegisterGeneratedMessage(TableField_reflection_);

  // static init for class TableSchema
  static const int TableSchema_offsets_[2] = {
    PROTO_MESSAGE_FIELD_OFFSET(Schema::TableSchema, name_),
    PROTO_MESSAGE_FIELD_OFFSET(Schema::TableSchema, fields_),
  };
  i = 0;
  for (auto& field: parser.mutable_messages_list()[1]->mutable_fields_list()) {
    field->set_field_offset(TableSchema_offsets_[i++]);
  }
  TableSchema_descriptor_ = parser.mutable_messages_list()[1];
  TableSchema_reflection_.reset(
      new ::proto::MessageReflection(
          TableSchema_descriptor_,
          Schema::TableSchema::default_instance_,
          PROTO_MESSAGE_FIELD_OFFSET(Schema::TableSchema, has_bits_))
  );
  ::proto::MessageFactory::RegisterGeneratedMessage(TableSchema_reflection_);

}

// Force static_init_home_hy_Desktop_Projects_DBMS_src_Schema_DBTable() to be called at initialization time.
struct static_init_forcer_home_hy_Desktop_Projects_DBMS_src_Schema_DBTable {
  static_init_forcer_home_hy_Desktop_Projects_DBMS_src_Schema_DBTable() {
    static_init_home_hy_Desktop_Projects_DBMS_src_Schema_DBTable();
  }
} static_init_forcer_home_hy_Desktop_Projects_DBMS_src_Schema_DBTable_obj_;


namespace Schema {

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
        (reinterpret_cast<const proto::ProtoParser::EnumType*>(
            TableField_descriptor_->FindFieldByName("type")->type_class()))
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
  if (default_instance_ == NULL) {
    static_init_default_instances_home_hy_Desktop_Projects_DBMS_src_Schema_DBTable();
  }
  return *default_instance_;
}

TableField* TableField::default_instance_ = NULL;

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
  type_ = TableField::INTEGER;
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

// ******************** TableSchema ******************** //
// constructor
TableSchema::TableSchema() {
  for (unsigned int i = 0; i < sizeof(has_bits_); i++) {
    has_bits_[i] = 0;
  }
  default_instance_ = nullptr;
}

// copy constructor
TableSchema::TableSchema(const TableSchema& other) {
  CopyFrom(other);
}

// move constructor
TableSchema::TableSchema(TableSchema&& other) {
  MoveFrom(std::move(other));
}

// copy assignment
TableSchema& TableSchema::operator=(const TableSchema& other) {
  CopyFrom(other);
  return *this;
}
// move assignment
TableSchema& TableSchema::operator=(TableSchema&& other) {
  MoveFrom(std::move(other));
  return *this;
}

// New()
::proto::Message* TableSchema::New() const {
  return reinterpret_cast<::proto::Message*>(new TableSchema());
}

// CopyFrom()
void TableSchema::CopyFrom(const TableSchema& other) {
  name_ = other.name();
  for (const TableField* p: other.fields().GetElements()) {
    fields_.AddAllocated(new TableField(*p));
  }
  for (unsigned int i = 0; i < sizeof(has_bits_); i++) {
    has_bits_[i] = other.has_bits_[i];
  }
}

// MoveFrom()
void TableSchema::MoveFrom(TableSchema&& other) {
  for (unsigned int i = 0; i < sizeof(has_bits_); i++) {
    has_bits_[i] = other.has_bits_[i];
  }
  name_ = std::move(other.mutable_name());
  fields_ = std::move(other.mutable_fields());
  for (unsigned int i = 0; i < sizeof(has_bits_); i++) {
    other.has_bits_[i] = 0;
  }
}

// Equals()
bool TableSchema::Equals(const TableSchema& other) const {
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
  return true;
}

// Serialize()
::proto::SerializedMessage* TableSchema::Serialize() const {
  return TableSchema_reflection_->Serialize(this);
}

// DeSerialize()
void TableSchema::DeSerialize(const char* buf, unsigned int size) {
  TableSchema_reflection_->DeSerialize(this, buf, size);
}

// Print()
void TableSchema::Print(int indent_num) const {
  PrintIndent(indent_num);
  std::cout << "TableSchema " << "{" << std::endl;
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
  PrintIndent(indent_num);
  std::cout << "}" << std::endl;
}

// InitAsDefaultInstance()
void TableSchema::InitAsDefaultInstance() {
}

// swapper
void TableSchema::Swap(TableSchema* other) {
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

  // swap has_bits
  for (unsigned int i = 0; i < sizeof(has_bits_); i++) {
    has_bits_[i] = buf[i + sizeof(has_bits_)];
    other->has_bits_[i] = buf[i];
  }
  delete buf;
}

// default_instance()
const TableSchema& TableSchema::default_instance() {
  if (default_instance_ == NULL) {
    static_init_default_instances_home_hy_Desktop_Projects_DBMS_src_Schema_DBTable();
  }
  return *default_instance_;
}

TableSchema* TableSchema::default_instance_ = NULL;

// destructor
TableSchema::~TableSchema() {
}

// "name" = 1
bool TableSchema::has_name() const {
  return (has_bits_[0] & 0x2) != 0;
}

const std::string& TableSchema::name() const {
  return name_;
}

void TableSchema::set_name(const std::string& name) {
  name_ = name;
  has_bits_[0] |= 0x2;
}

void TableSchema::set_name(const char* name) {
  name_ = std::string(name);
  has_bits_[0] |= 0x2;
}

void TableSchema::set_name(const char* name, int size) {
  name_ = std::string(name, size);
  has_bits_[0] |= 0x2;
}

std::string TableSchema::mutable_name() {
  return name_;
}

void TableSchema::clear_name() {
  name_ = "";
  has_bits_[0] &= (~0x2);
}

// "fields" = 2
int TableSchema::fields_size() const {
  return fields_.size();
}

const TableField& TableSchema::fields(int index) const {
  return fields_.Get(index);
}

TableField* TableSchema::add_fields() {
  return fields_.Add();
}

TableField* TableSchema::mutable_fields(int index) {
  return fields_.GetMutable(index);
}

void TableSchema::clear_fields() {
  fields_.Clear();
}

const ::proto::RepeatedPtrField<TableField>& TableSchema::fields() const {
  return fields_;
}

::proto::RepeatedPtrField<TableField>& TableSchema::mutable_fields() {
  return fields_;
}

}  // namespace Schema

