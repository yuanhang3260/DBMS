#include <climits>
#include <string.h>
#include <iostream>
#include <stdexcept>
#include <algorithm>

#include "Base/Utils.h"
#include "Base/Log.h"
#include "Record.h"

namespace Schema {

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
int RecordBase::size() const {
  int size = 0;
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
    if (field->type() == STRING || field->type() == CHARARRAY) {
      std::cout << SchemaFieldType::FieldTypeAsString(field->type()) << ": "
                << "\"" << field->AsString() << "\" | ";
    }
    else {
      std::cout << SchemaFieldType::FieldTypeAsString(field->type()) << ": "
                << field->AsString() << " | ";
    }
  }
}

void RecordBase::AddField(SchemaFieldType* new_field) {
  fields_.push_back(std::shared_ptr<SchemaFieldType>(new_field));
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

int RecordBase::CompareRecordsBasedOnIndex(const RecordBase* r1,
                                           const RecordBase* r2,
                                           const std::vector<int>& indexes) {
  for (int i = 0; i < (int)indexes.size(); i++) {
    int re = RecordBase::CompareSchemaFields(
                 r1->fields_.at(indexes[i]).get(),
                 r2->fields_.at(indexes[i]).get()
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

bool RecordBase::RecordComparator(const std::shared_ptr<RecordBase> r1,
                                  const std::shared_ptr<RecordBase> r2,
                                  const std::vector<int>& indexes) {
  return CompareRecordsBasedOnIndex(r1.get(), r2.get(), indexes) < 0;
}

int RecordBase::CompareRecordWithKey(const RecordBase* key,
                                     const RecordBase* record,
                                     const std::vector<int>& indexes) {
  for (int i = 0; i < (int)indexes.size(); i++) {
    int re = RecordBase::CompareSchemaFields(
                 key->fields_.at(i).get(),
                 record->fields_.at(indexes[i]).get()
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
  const TYPE& f1 = *reinterpret_cast<const TYPE*>(FIELD1);  \
  const TYPE& f2 = *reinterpret_cast<const TYPE*>(FIELD2);  \
  if (f1 < f2) {                                      \
    return -1;                                        \
  }                                                   \
  if (f1 > f2) {                                      \
    return 1;                                         \
  }                                                   \
  return 0;                                           \

int RecordBase::CompareSchemaFields(const SchemaFieldType* field1,
                                    const SchemaFieldType* field2) {
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
  if (type != field2->type()) {
    LogFATAL("Comparing different types of schema fields!");
  }

  if (type == INT) {
    COMPARE_FIELDS_WITH_TYPE(IntType, field1, field2);
  }
  if (type == LONGINT) {
    COMPARE_FIELDS_WITH_TYPE(LongIntType, field1, field2);
  }
  if (type == DOUBLE) {
    COMPARE_FIELDS_WITH_TYPE(DoubleType, field1, field2);
  }
  if (type == BOOL) {
    COMPARE_FIELDS_WITH_TYPE(BoolType, field1, field2);
  }
  if (type == STRING) {
    COMPARE_FIELDS_WITH_TYPE(StringType, field1, field2);
  }
  if (type == CHARARRAY) {
    COMPARE_FIELDS_WITH_TYPE(CharArrayType, field1, field2);
  }

  throw std::runtime_error("Compare Schema Fields - Should NOT Reach Here.");
  return 0;
}

int RecordBase::DumpToMem(byte* buf) const {
  if (!buf) {
    return -1;
  }

  int offset = 0;
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

  int offset = 0;
  for (const auto& field: fields_) {
    offset += field->LoadFromMem(buf + offset);
  }

  if (offset != RecordBase::size()) {
    LogFATAL("Record load %d byte, record.size() = %d", offset, size());
  }
  return offset;
}

bool RecordBase::InsertToRecordPage(DataBaseFiles::RecordPage* page) const {
  byte* buf = page->InsertRecord(size());
  if (buf) {
    // Write the record content to page.
    DumpToMem(buf);
    return true;
  }
  return false;
}

RecordBase* RecordBase::Duplicate() const {
  RecordBase* new_record = new RecordBase();
  new_record->fields_ = fields_;
  return new_record;
}

bool RecordBase::CopyFieldsFrom(const RecordBase* source) {
  if (!source) {
    LogERROR("Can't copy fields from nullptr RecordBase");
    return false;
  }
  fields_ = source->fields_;
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

bool RecordBase::InitRecordFields(const TableSchema* schema,
                                  std::vector<int> key_indexes,
                                  DataBaseFiles::FileType file_type,
                                  DataBaseFiles::PageType page_type) {
  // Create record based on file tpye and page type
  if (file_type == DataBaseFiles::INDEX_DATA &&
      page_type == DataBaseFiles::TREE_LEAVE) {
    // DataRecord should contain all fields.
    key_indexes.resize(schema->fields_size());
    for (int i = 0; i < (int)key_indexes.size(); i++) {
      key_indexes[i] = i;
    }
  }

  for (int index: key_indexes) {
    auto type = schema->fields(index).type();
    if (type == TableField::INTEGER) {
      AddField(new IntType());
    }
    if (type == TableField::LLONG) {
      AddField(new LongIntType());
    }
    if (type == TableField::DOUBLE) {
      AddField(new DoubleType());
    }
    if (type == TableField::BOOL) {
      AddField(new BoolType());
    }
    if (type == TableField::STRING) {
      AddField(new StringType());
    }
    if (type == TableField::CHARARR) {
      AddField(new CharArrayType(schema->fields(index).size()));
    }
  }
  return true;
}

// Check fields type match a schema.
bool RecordBase::CheckFieldsType(const TableSchema* schema,
                                 std::vector<int> key_indexes) const {
  if (!schema) {
    LogERROR("schema is nullptr passed to CheckFieldsType");
    return false;
  }
  if (fields_.size() != key_indexes.size()) {
    LogERROR("Index/TreeNode record has mismatchig number of fields - "
             "key has %d indexes, record has %d",
             key_indexes.size(), fields_.size());
    return false;
  }
  for (int i = 0; i < (int)key_indexes.size(); i++) {
    if (fields_[i] && fields_[i]->MatchesSchemaType(
                                      schema->fields(key_indexes[i]).type())) {
      LogERROR("Index/TreeNode record has mismatchig field type with schema "
               "field %d", key_indexes[i]);
      return false;
    }
  }
  return true;
}

bool RecordBase::CheckFieldsType(const TableSchema* schema) const {
  if (!schema) {
    LogERROR("schema is nullptr passed to CheckFieldsType");
    return false;
  }
  if ((int)fields_.size() != schema->fields_size()) {
    LogERROR("Data record has mismatchig number of fields with schema - "
             "schema has %d indexes, record has %d",
             schema->fields_size(), fields_.size());
    return false;
  }
  for (int i = 0; i < (int)schema->fields_size(); i++) {
    if (!fields_[i] || !fields_[i]->MatchesSchemaType(
                                        schema->fields(i).type())) {
      LogERROR("Data record has mismatchig field type with schema field %d", i);
      return false;
    }
  }
  return true;
}

// ****************************** DataRecord ******************************** //
bool DataRecord::ExtractKey(
         RecordBase* key, const std::vector<int>& key_indexes) const {
  if (!key) {
    return false;
  }
  key->fields().clear();
  for (int index: key_indexes) {
    if (index > (int)fields_.size()) {
      LogERROR("key_index %d > number of fields, won't fetch");
      continue;
    }
    key->fields().push_back(fields_.at(index));
  }
  return true;
}


// ***************************** IndexRecord ******************************** //
int IndexRecord::DumpToMem(byte* buf) const {
  if (!buf) {
    return -1;
  }
  int offset = RecordBase::DumpToMem(buf);
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
  int offset = RecordBase::LoadFromMem(buf);
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

int IndexRecord::size() const {
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
  int offset = RecordBase::DumpToMem(buf);
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
  int offset = RecordBase::LoadFromMem(buf);
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

int TreeNodeRecord::size() const {
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

}  // namespace Schema
