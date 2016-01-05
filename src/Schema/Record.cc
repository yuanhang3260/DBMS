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

int RecordBase::CompareRecordsBasedOnKey(const RecordBase* r1,
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
  return CompareRecordsBasedOnKey(r1.get(), r2.get(), indexes) < 0;
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

  record_->InitRecordFields(schema, key_indexes, file_type, page_type);
  return true;
}

bool PageLoadedRecord::Comparator(const PageLoadedRecord& r1,
                                  const PageLoadedRecord& r2,
                                  const std::vector<int>& indexes) {
  // TODO: Compare Rid for Index B+ tree?
  return RecordBase::RecordComparator(r1.record_, r2.record_, indexes);
}


// ************************** PageRecordsManager **************************** //
PageRecordsManager::PageRecordsManager(DataBaseFiles::RecordPage* page,
                                       TableSchema* schema,
                                       std::vector<int> key_indexes,
                                       DataBaseFiles::FileType file_type,
                                       DataBaseFiles::PageType page_type) :
    page_(page),
    schema_(schema),
    key_indexes_(key_indexes),
    file_type_(file_type),
    page_type_(page_type) {
  if (!page) {
    LogFATAL("Can't init PageRecordsManager with page nullptr");
  }
  if (!schema) {
    LogFATAL("Can't init PageRecordsManager with schema nullptr");
  }

  if (!LoadRecordsFromPage()) {
    LogFATAL("Load page %d records failed", page->id());
  }
}


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
  std::stable_sort(records.begin(), records.end(), comparator);
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

  if (plrecords_.empty()) {
    return true;  // Got empty page.
  }

  // Sort records
  auto comparator = std::bind(PageLoadedRecord::Comparator,
                              std::placeholders::_1, std::placeholders::_2,
                              ProduceIndexesToCompare());
  std::stable_sort(plrecords_.begin(), plrecords_.end(), comparator);

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

std::vector<int> PageRecordsManager::ProduceIndexesToCompare() const {
  std::vector<int> indexes;
  if (file_type_ == DataBaseFiles::INDEX_DATA &&
      page_type_ == DataBaseFiles::TREE_LEAVE) {
    indexes = key_indexes_;
  }
  else {
    for (int i = 0; i < (int)key_indexes_.size(); i++) {
      indexes.push_back(i);
    }
  }
  return indexes;
}

bool PageRecordsManager::CheckSort() const {
  if (plrecords_.empty()) {
    return true;
  }

  std::vector<int> check_indexes = ProduceIndexesToCompare();
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

int PageRecordsManager::AppendRecordAndSplitPage(RecordBase* record) {
  if (plrecords_.empty()) {
    LogERROR("Won't add the record - This PageRecordsManager has not loaded "
             "any PageLoadedRecord");
    return -1;
  }
  if (record->NumFields() != plrecords_[0].NumFields()) {
    LogERROR("Can't insert a new reocrd to PageRecordsManager - record "
             "has mismatching number of fields with that of this page");
    return -1;
  }

  // Create a new PageLoadedRecord with this record. We need to duplicate
  // the record and pass it to PageLoadedRecord so that it won't take ownership
  // of the original one.
  PageLoadedRecord new_plrecord;
  new_plrecord.set_record(record->Duplicate());
  plrecords_.insert(plrecords_.end(), new_plrecord);
  auto comparator = std::bind(PageLoadedRecord::Comparator,
                              std::placeholders::_1, std::placeholders::_2,
                              ProduceIndexesToCompare());
  std::stable_sort(plrecords_.begin(), plrecords_.end(), comparator);
  total_size_ += record->size();

  int acc_size = 0;
  int i = 0;
  for (; i < (int)plrecords_.size(); i++) {
    acc_size += plrecords_.at(i).record()->size();
    if (acc_size > total_size_ / 2) {
      break;
    }
  }
  if (acc_size - total_size_ / 2 >
      total_size_ / 2 - acc_size + (int)plrecords_.at(i).record()->size()) {
    i--;
  }
  return i + 1;
}

RecordBase* PageRecordsManager::Record(int index) const {
  if (index >= (int)plrecords_.size()) {
    return nullptr;
  }
  return plrecords_.at(index).record();
}

int PageRecordsManager::CompareRecordWithKey(const RecordBase* key,
                                             const RecordBase* record) const {
  if (page_type_ == DataBaseFiles::TREE_NODE ||
      file_type_ == DataBaseFiles::INDEX) {
    return RecordBase::CompareRecordsBasedOnKey(key, record,
                                                ProduceIndexesToCompare());
  }
  else {
    // file_type = INDEX_DATA && page_type = TREE_LEAVE
    return RecordBase::CompareRecordWithKey(key, record, key_indexes_);
  }
}

int PageRecordsManager::SearchForKey(const RecordBase* key) const {
  if (!key) {
    LogERROR("key to search for is nullptr");
    return -1;
  }

  if (plrecords_.empty()) {
    LogERROR("Empty page, won't search");
    return -1;
  }

  int index = 0;
  for (; index < (int)plrecords_.size(); index++) {
    if (CompareRecordWithKey(key, Record(index)) < 0) {
      break;
    }
  }
  index--;
  if (index < 0) {
    LogFATAL("Search for key less than all record keys of this page");
    key->Print();
    Record(0)->Print();
  }

  return index;
}

}  // namespace Schema
