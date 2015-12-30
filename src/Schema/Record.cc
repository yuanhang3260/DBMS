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

bool RecordBase::RecordComparator(const std::shared_ptr<RecordBase> r1,
                                  const std::shared_ptr<RecordBase> r2,
                                  const std::vector<int>& indexes) {
  for (int i = 0; i < (int)indexes.size(); i++) {
    int re = RecordBase::CompareSchemaFields(
                 r1->fields_.at(indexes[i]).get(),
                 r2->fields_.at(indexes[i]).get()
             );
    if (re < 0) {
      return true;
    }
    else if (re > 0) {
      return false;
    }
  }
  return false;
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
    return type - field2->type();
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

  if (offset != size()) {
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
  return offset;
}

bool RecordBase::InsertToRecordPage(DataBaseFiles::RecordPage* page) {
  byte* buf = page->InsertRecord(size());
  if (buf) {
    // Write the record content to page.
    DumpToMem(buf);
    return true;
  }
  return false;
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

// **************************** TreeNodeRecord ****************************** //
int TreeNodeRecord::DumpToMem(byte* buf) const {
  if (!buf) {
    return -1;
  }
  int offset = RecordBase::DumpToMem(buf);
  memcpy(buf + offset, &page_id_, sizeof(page_id_));
  if (offset != size()) {
    LogFATAL("TreeNodeRecord DumpToMem error - expect %d bytes, actual %d",
             size(), offset);
  }
  return offset + sizeof(page_id_);
}

int TreeNodeRecord::LoadFromMem(const byte* buf) {
  if (!buf) {
    return -1;
  }
  int offset = RecordBase::LoadFromMem(buf);
  memcpy(&page_id_, buf + offset, sizeof(page_id_));
  if (offset != size()) {
    LogFATAL("TreeNodeRecord LoadFromMem error - expect %d bytes, actual %d",
             size(), offset);
  }
  return offset + sizeof(page_id_);
}

void TreeNodeRecord::Print() const {
  RecordBase::PrintImpl();
  std::cout << "page_id = " << page_id_ << std::endl; 
}

int TreeNodeRecord::size() const {
  return RecordBase::size() + sizeof(page_id_);
}


// **************************** PageLoadedRecord **************************** //
bool PageLoadedRecord::GenerateRecordPrototype(
         const TableSchema* schema,
         std::vector<int> key_indexes,
         DataBaseFiles::FileType file_type,
         DataBaseFiles::PageType page_type) {
  // Create record based on file tpye and page type
  if (file_type == DataBaseFiles::INDEX_DATA &&
      page_type == DataBaseFiles::TREE_LEAVE) {
    record_.reset(new DataRecord());
    // DataRecord should contain all fields.
    key_indexes.resize(schema->fields_size());
    for (int i = 0; i < (int)key_indexes.size(); i++) {
      key_indexes[i] = i;
    }
  }
  else if (file_type == DataBaseFiles::INDEX &&
      page_type == DataBaseFiles::TREE_LEAVE) {
    record_.reset(new IndexRecord());
  }
  else if (page_type == DataBaseFiles::TREE_NODE ||
      page_type == DataBaseFiles::TREE_ROOT) {
    record_.reset(new TreeNodeRecord());
  }

  if (!record_) {
    LogERROR("Illegal file_type and page_type combination");
    return false;
  }

  for (int index: key_indexes) {
    auto type = schema->fields(index).type();
    if (type == TableField::INTEGER) {
      record_->AddField(new IntType());
    }
    if (type == TableField::LLONG) {
      record_->AddField(new LongIntType());
    }
    if (type == TableField::DOUBLE) {
      record_->AddField(new DoubleType());
    }
    if (type == TableField::BOOL) {
      record_->AddField(new BoolType());
    }
    if (type == TableField::STRING) {
      record_->AddField(new StringType());
    }
    if (type == TableField::CHARARR) {
      record_->AddField(new CharArrayType(schema->fields(index).size()));
    }
  }
  return true;
}

bool PageLoadedRecord::Comparator(const PageLoadedRecord& r1,
                                  const PageLoadedRecord& r2,
                                  const std::vector<int>& indexes) {
  // TODO: Compare Rid for Index B+ tree?
  return RecordBase::RecordComparator(r1.record_, r2.record_, indexes);
}


// ************************** PageRecordsManager **************************** //
void PageRecordsManager::SortRecords(
         std::vector<std::shared_ptr<Schema::RecordBase>>& records,
         const std::vector<int>& key_indexes) {
  for (int i: key_indexes) {
    if (i >= records[0]->NumFields()) {
      LogERROR("key index = %d, records only has %d fields",
               i, records[0]->NumFields());
      throw std::out_of_range("key index out of range");
    }
  }
  auto comparator = std::bind(RecordBase::RecordComparator,
                              std::placeholders::_1, std::placeholders::_2,
                              key_indexes);
  std::sort(records.begin(), records.end(), comparator);
}

bool PageRecordsManager::LoadRecordsFromPage() {
  if (!page_) {
    LogERROR("Can't load records from page nullptr");
    return false;
  }

  // Clean previous data.
  plrecords_.clear();

  const auto& slot_directory = page_->Meta()->slot_directory();
  for (int slot_id = 0; slot_id < (int)slot_directory.size(); slot_id++) {
    int offset = slot_directory.at(slot_id).offset();
    int length = slot_directory.at(slot_id).length();
    if (offset < 0) {
      continue;
    }
    plrecords_.push_back(PageLoadedRecord(slot_id));
    plrecords_.back().GenerateRecordPrototype(schema_, key_indexes_,
                                              file_type_, page_type_);
    int load_size = plrecords_.back().record()->
                        LoadFromMem(page_->Record(slot_id));
    if (load_size != length) {
      LogERROR("Error loading slot %d from page - expect %d byte, actual %d ",
               slot_id, length, load_size);
      return false;
    }
    total_size_ += load_size;
  }

  // Sort records
  auto comparator = std::bind(PageLoadedRecord::Comparator,
                              std::placeholders::_1, std::placeholders::_2,
                              key_indexes_);
  std::sort(plrecords_.begin(), plrecords_.end(), comparator);

  return true;
}

bool PageRecordsManager::InsertRecordToPage(const RecordBase* record) {
  byte* buf = page_->InsertRecord(record->size());
  if (buf) {
    // Write the record content to page.
    record->DumpToMem(buf);
    return true;
  }
  return false;
}

bool PageRecordsManager::CheckSort() const {
  if (plrecords_.empty()) {
    return true;
  }

  std::vector<int> check_indexes = key_indexes_;
  if (!(file_type_ == DataBaseFiles::INDEX_DATA &&
        page_type_ == DataBaseFiles::TREE_LEAVE)) {
    check_indexes.clear();
    for (int i = 0; i < (int)plrecords_.at(0).NumFields(); i++) {
      check_indexes.push_back(i);
    }
  }

  for (int i = 0; i < (int)plrecords_.size() - 1; i++) {
    const auto& r1 = plrecords_.at(i);
    const auto& r2 = plrecords_.at(i + 1);
    for (int index: check_indexes) {
      int re = RecordBase::CompareSchemaFields(
                   (r1.record()->fields())[index].get(),
                   (r2.record()->fields())[index].get());
      if (re > 0) {
        return false;
      }
      if (re < 0) {
        return true;
      }
    }
  }
  return true;
}

}  // namespace Schema
