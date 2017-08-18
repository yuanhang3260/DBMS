#include <climits>
#include <string.h>
#include <iostream>
#include <stdexcept>
#include <algorithm>

#include "Base/Utils.h"
#include "Base/Log.h"
#include "Strings/Split.h"
#include "Strings/Utils.h"

#include "Storage/Record.h"

namespace Storage {

std::string RecordTypeStr(RecordType record_type) {
  switch (record_type) {
    case INDEX_RECORD:
      return "INDEX_RECORD";
    case DATA_RECORD:
      return "DATA_RECORD";
    case TREENODE_RECORD:
      return "TREENODE_RECORD";
    case UNKNOWN_RECORDTYPE:
      return "UNKNOWN_RECORD_TYPE";
  }
  return "UNKNOWN_RECORDTYPE";
}

// ****************************** RecordID ********************************** //
int RecordID::DumpToMem(byte* buf) const {
  if (!buf) {
    return -1;
  }
  memcpy(buf, &page_id_, sizeof(page_id_));
  memcpy(buf + sizeof(page_id_), &slot_id_, sizeof(slot_id_));
  return sizeof(page_id_) + sizeof(slot_id_);
}

int RecordID::LoadFromMem(const byte* buf) {
  if (!buf) {
    return -1;
  }
  memcpy(&page_id_, buf, sizeof(page_id_));
  memcpy(&slot_id_, buf + sizeof(page_id_), sizeof(slot_id_));
  return sizeof(page_id_) + sizeof(slot_id_);
}

void RecordID::Print() const {
  std::cout << "rid = (" << page_id_ << ", " << slot_id_ << ")" << std::endl; 
}

// ****************************** RecordBase ******************************** //
uint32 RecordBase::size() const {
  uint32 size = 0;
  for (const auto& field: fields_) {
    size += field->length();
  }
  return size;
}

void RecordBase::Print() const {
  PrintImpl();
  std::cout << std::endl;
}

void RecordBase::PrintImpl() const {
  std::cout << "Record: | ";
  for (auto& field: fields_) {
    if (field->type() == Schema::FieldType::STRING ||
        field->type() == Schema::FieldType::CHARARRAY) {
      std::cout << Schema::FieldTypeStr(field->type()) << ": "
                << "\"" << field->AsString() << "\" | ";
    }
    else {
      std::cout << Schema::FieldTypeStr(field->type()) << ": "
                << field->AsString() << " | ";
    }
  }
}

void RecordBase::AddField(Schema::Field* new_field) {
  fields_.push_back(std::shared_ptr<Schema::Field>(new_field));
}

void RecordBase::AddField(std::shared_ptr<Schema::Field> new_field) {
  fields_.push_back(new_field);
}

bool RecordBase::operator<(const RecordBase& other) const {
  const auto& other_fields = other.fields();
  int len = Utils::Min(fields_.size(), other_fields.size());
  for (int i = 0; i < len; i++) {
    int re = RecordBase::CompareSchemaFields(
                 fields_.at(i).get(), other_fields.at(i).get());
    if (re < 0) {
      return true;
    }
    else if (re > 0){
      return false;
    }
  }
  return fields_.size() < other_fields.size();
}

bool RecordBase::operator>(const RecordBase& other) const {
  const auto& other_fields = other.fields();
  int len = Utils::Min(fields_.size(), other_fields.size());
  for (int i = 0; i < len; i++) {
    int re = RecordBase::CompareSchemaFields(
                 fields_.at(i).get(), other_fields.at(i).get());
    if (re > 0) {
      return true;
    }
    else if (re < 0){
      return false;
    }
  }
  return fields_.size() > other_fields.size();
}

bool RecordBase::operator<=(const RecordBase& other) const {
  return !(*this > other);
}

bool RecordBase::operator>=(const RecordBase& other) const {
  return !(*this < other);
}

bool RecordBase::operator==(const RecordBase& other) const {
  uint32 len = fields_.size();
  if (len != other.fields_.size()) {
    return false;
  }

  const auto& other_fields = other.fields();
  for (uint32 i = 0; i < len; i++) {
    int re = RecordBase::CompareSchemaFields(
                 fields_.at(i).get(), other_fields.at(i).get());
    if (re != 0) {
      return false;
    }
  }
  return true;
}

int RecordBase::CompareRecords(const RecordBase& r1, const RecordBase& r2) {
  CHECK(r1.NumFields() == r2.NumFields(),
        "records have different number of fields");

  for (uint32 i = 0; i < r1.NumFields(); i++) {
    CHECK(r1.fields_.at(i)->type() == r2.fields_.at(i)->type(),
          "Comparing different types of schema fields!");
    int re = RecordBase::CompareSchemaFields(r1.fields_.at(i).get(),
                                             r2.fields_.at(i).get());
    if (re < 0) {
      return -1;
    }
    else if (re > 0) {
      return 1;
    }
  }
  return 0;
}

int RecordBase::CompareRecordsBasedOnIndex(const RecordBase& r1,
                                           const RecordBase& r2,
                                           const std::vector<uint32>& indexes) {
  CHECK(!indexes.empty(), "empty comparing indexes");
  for (uint32 i = 0; i < indexes.size(); i++) {
    int re = RecordBase::CompareSchemaFields(
                 r1.fields_.at(indexes[i]).get(),
                 r2.fields_.at(indexes[i]).get()
             );
    if (re < 0) {
      return -1;
    }
    else if (re > 0) {
      return 1;
    }
  }
  return 0;
}

bool RecordBase::RecordComparator(const RecordBase& r1,
                                  const RecordBase& r2,
                                  const std::vector<uint32>& indexes) {
  return CompareRecordsBasedOnIndex(r1, r2, indexes) < 0;
}

bool RecordBase::RecordComparatorGt(const RecordBase& r1,
                                    const RecordBase& r2,
                                    const std::vector<uint32>& indexes) {
  return CompareRecordsBasedOnIndex(r1, r2, indexes) > 0;
}

int RecordBase::CompareRecordWithKey(const RecordBase& record,
                                     const RecordBase& key,
                                     const std::vector<uint32>& indexes) {
  CHECK(!indexes.empty(), "empty comparing indexes");
  CHECK(key.NumFields() == indexes.size(),
        "Number of key fields mismatch with indexes to compare");
  for (uint i = 0; i < indexes.size(); i++) {
    int re = RecordBase::CompareSchemaFields(
                 record.fields_.at(indexes[i]).get(),
                 key.fields_.at(i).get()
             );
    if (re < 0) {
      return -1;
    }
    else if (re > 0) {
      return 1;
    }
  }
  return 0;
}

bool RecordBase::operator!=(const RecordBase& other) const {
  return !(*this == other);
}

#define COMPARE_FIELDS_WITH_TYPE(TYPE, FIELD1, FIELD2)  \
  const TYPE& f1 = *dynamic_cast<const TYPE*>(FIELD1);  \
  const TYPE& f2 = *dynamic_cast<const TYPE*>(FIELD2);  \
  if (f1 < f2) {                                      \
    return -1;                                        \
  }                                                   \
  if (f1 > f2) {                                      \
    return 1;                                         \
  }                                                   \
  return 0;                                           \

int RecordBase::CompareSchemaFields(const Schema::Field* field1,
                                    const Schema::Field* field2) {
  if (!field1 && !field2) {
    return 0;
  }
  if (!field1) {
    return -1;
  }
  if (!field2) {
    return 1;
  }

  auto type = field1->type();
  CHECK(type == field2->type(), "Comparing different types of schema fields!");

  if (type == Schema::FieldType::INT) {
    COMPARE_FIELDS_WITH_TYPE(Schema::IntField, field1, field2);
  }
  if (type == Schema::FieldType::LONGINT) {
    COMPARE_FIELDS_WITH_TYPE(Schema::LongIntField, field1, field2);
  }
  if (type == Schema::FieldType::DOUBLE) {
    COMPARE_FIELDS_WITH_TYPE(Schema::DoubleField, field1, field2);
  }
  if (type == Schema::FieldType::BOOL) {
    COMPARE_FIELDS_WITH_TYPE(Schema::BoolField, field1, field2);
  }
  if (type == Schema::FieldType::CHAR) {
    COMPARE_FIELDS_WITH_TYPE(Schema::CharField, field1, field2);
  }
  if (type == Schema::FieldType::STRING) {
    COMPARE_FIELDS_WITH_TYPE(Schema::StringField, field1, field2);
  }
  if (type == Schema::FieldType::CHARARRAY) {
    COMPARE_FIELDS_WITH_TYPE(Schema::CharArrayField, field1, field2);
  }

  throw std::runtime_error("Compare Schema Fields - Should NOT Reach Here.");
  return 0;
}

int RecordBase::DumpToMem(byte* buf) const {
  if (!buf) {
    return -1;
  }

  uint32 offset = 0;
  for (const auto& field: fields_) {
    offset += field->DumpToMem(buf + offset);
  }

  if (offset != RecordBase::size()) {
    LogFATAL("Record dump %d byte, record.size() = %d", offset, size());
  }
  return offset;
}

int RecordBase::LoadFromMem(const byte* buf) {
  if (!buf) {
    return -1;
  }

  uint32 offset = 0;
  for (const auto& field: fields_) {
    offset += field->LoadFromMem(buf + offset);
  }

  if (offset != RecordBase::size()) {
    LogFATAL("Record load %d byte, record.size() = %d", offset, size());
  }
  return offset;
}

int RecordBase::InsertToRecordPage(RecordPage* page) const {
  int slot_id = page->InsertRecord(size());
  if (slot_id >= 0) {
    // Write the record content to page.
    DumpToMem(page->Record(slot_id));
    return slot_id;
  }
  return -1;
}

RecordBase* RecordBase::Duplicate() const {
  RecordBase* new_record = new RecordBase();
  new_record->fields_ = fields_;
  return new_record;
}

bool RecordBase::CopyFieldsFrom(const RecordBase& source) {
  fields_ = source.fields_;
  return true;
}

void RecordBase::reset() {
  for (auto& field: fields_) {
    field->reset();
  }
}

void RecordBase::clear() {
  fields_.clear();
}

void RecordBase::AddField(const DB::TableField& field_info) {
  auto field_type = field_info.type();
  if (field_type == Schema::FieldType::INT) {
    AddField(new Schema::IntField());
  }
  if (field_type == Schema::FieldType::LONGINT) {
    AddField(new Schema::LongIntField());
  }
  if (field_type == Schema::FieldType::DOUBLE) {
    AddField(new Schema::DoubleField());
  }
  if (field_type == Schema::FieldType::BOOL) {
    AddField(new Schema::BoolField());
  }
  if (field_type == Schema::FieldType::CHAR) {
    AddField(new Schema::CharField());
  }
  if (field_type == Schema::FieldType::STRING) {
    AddField(new Schema::StringField());
  }
  if (field_type == Schema::FieldType::CHARARRAY) {
    AddField(new Schema::CharArrayField(field_info.size()));
  }
}

bool RecordBase::InitRecordFields(const DB::TableInfo& schema,
                                  const std::vector<uint32>& indexes) {
  clear();
  for (int index: indexes) {
    AddField(schema.fields(index));
  }
  return true;
}

// Check fields type match a schema.
bool RecordBase::CheckFieldsType(const DB::TableInfo& schema,
                                 std::vector<uint32> key_indexes) const {
  if (fields_.size() != key_indexes.size()) {
    LogERROR("Index/TreeNode record has mismatchig number of fields - "
             "key has %d fields, record has %d fields",
             key_indexes.size(), fields_.size());
    return false;
  }
  for (int i = 0; i < (int)key_indexes.size(); i++) {
    if (!fields_[i] || !fields_[i]->MatchesSchemaType(
                                      schema.fields(key_indexes[i]).type())) {
      LogERROR("Index/TreeNode record has mismatchig field type with schema "
               "field %d", key_indexes[i]);
      return false;
    }
  }
  return true;
}

bool RecordBase::CheckFieldsType(const DB::TableInfo& schema) const {
  if ((int)fields_.size() != schema.fields_size()) {
    LogERROR("Data record has mismatchig number of fields with schema - "
             "schema has %d indexes, record has %d",
             schema.fields_size(), fields_.size());
    return false;
  }
  for (int i = 0; i < (int)schema.fields_size(); i++) {
    if (!fields_[i] || !fields_[i]->MatchesSchemaType(
                                        schema.fields(i).type())) {
      LogERROR("Data record has mismatchig field type with schema field %d", i);
      return false;
    }
  }
  return true;
}

bool RecordBase::ParseFromText(std::string str, int chararray_len_limit) {
  auto tokens = Strings::Split(str, '|');
  for (auto& block: tokens) {
    block = Strings::Strip(block);
    if (block.length() == 0) {
      continue;
    }
    auto pieces = Strings::Split(block, ':');
    if ((int)pieces.size() != 2) {
      continue;
    }
    for (int i = 0; i < (int)pieces.size(); i++) {
      pieces[i] = Strings::Strip(pieces[i]);
      pieces[i] = Strings::Strip(pieces[i], "\"\"");
    }
    if (pieces[0] == "Int") {
      AddField(new Schema::IntField(std::stoi(pieces[1])));
    }
    else if (pieces[0] == "LongInt") {
      AddField(new Schema::LongIntField(std::stol(pieces[1])));
    }
    else if (pieces[0] == "Double") {
      AddField(new Schema::DoubleField(std::stod(pieces[1])));
    }
    else if (pieces[0] == "Bool") {
      AddField(new Schema::BoolField(std::stoi(pieces[1])));
    }
    else if (pieces[0] == "String") {
      AddField(new Schema::StringField(pieces[1]));
    }
    else if (pieces[0] == "CharArray") {
      AddField(new Schema::CharArrayField(pieces[1], chararray_len_limit));
    }
  }
  return (int)fields_.size() > 0;
}

// ****************************** DataRecord ******************************** //
bool DataRecord::ExtractKey(
         RecordBase* key, const std::vector<uint32>& key_indexes) const {
  if (!key) {
    return false;
  }
  key->fields().clear();
  for (uint32 index: key_indexes) {
    if (index > fields_.size()) {
      LogERROR("key_index %d > number of fields, won't fetch");
      continue;
    }
    key->fields().push_back(fields_.at(index));
  }
  return true;
}

RecordBase* DataRecord::Duplicate() const {
  DataRecord* new_record = new DataRecord();
  new_record->fields_ = fields_;
  return new_record;
}

// ***************************** IndexRecord ******************************** //
int IndexRecord::DumpToMem(byte* buf) const {
  if (!buf) {
    return -1;
  }
  uint32 offset = RecordBase::DumpToMem(buf);
  offset += rid_.DumpToMem(buf + offset);
  if (offset != size()) {
    LogFATAL("IndexRecord DumpToMem error - expect %d bytes, actual %d",
             size(), offset);
  }
  return offset;
}

int IndexRecord::LoadFromMem(const byte* buf) {
  if (!buf) {
    return -1;
  }
  uint32 offset = RecordBase::LoadFromMem(buf);
  offset += rid_.LoadFromMem(buf + offset);
  if (offset != size()) {
    LogFATAL("IndexRecord LoadFromMem error - expect %d bytes, actual %d",
             size(), offset);
  }
  return offset;
}

void IndexRecord::Print() const {
  RecordBase::PrintImpl();
  rid_.Print(); 
}

uint32 IndexRecord::size() const {
  return RecordBase::size() + rid_.size();
}

RecordBase* IndexRecord::Duplicate() const {
  IndexRecord* new_record = new IndexRecord();
  new_record->fields_ = fields_;
  new_record->rid_ = rid_;
  return new_record;
}

void IndexRecord::reset() {
  RecordBase::reset();
  rid_.set_page_id(-1);
  rid_.set_slot_id(-1);
}

// **************************** TreeNodeRecord ****************************** //
int TreeNodeRecord::DumpToMem(byte* buf) const {
  if (!buf) {
    return -1;
  }
  uint32 offset = RecordBase::DumpToMem(buf);
  memcpy(buf + offset, &page_id_, sizeof(page_id_));
  offset += sizeof(page_id_);
  if (offset != size()) {
    LogFATAL("TreeNodeRecord DumpToMem error - expect %d bytes, actual %d",
             size(), offset);
  }
  return offset;
}

int TreeNodeRecord::LoadFromMem(const byte* buf) {
  if (!buf) {
    return -1;
  }
  uint32 offset = RecordBase::LoadFromMem(buf);
  memcpy(&page_id_, buf + offset, sizeof(page_id_));
  offset += sizeof(page_id_);
  if (offset != size()) {
    LogFATAL("TreeNodeRecord LoadFromMem error - expect %d bytes, actual %d",
             size(), offset);
  }
  return offset;
}

void TreeNodeRecord::Print() const {
  RecordBase::PrintImpl();
  std::cout << "page_id = " << page_id_ << std::endl; 
}

uint32 TreeNodeRecord::size() const {
  return RecordBase::size() + sizeof(page_id_);
}

RecordBase* TreeNodeRecord::Duplicate() const {
  TreeNodeRecord* new_record = new TreeNodeRecord();
  new_record->fields_ = fields_;
  new_record->page_id_ = page_id_;
  return new_record;
}

void TreeNodeRecord::reset() {
  RecordBase::reset();
  page_id_ = -1;
}

}  // namespace Storage
