#ifndef STORAGE_MERGE_SORT_
#define STORAGE_MERGE_SORT_

#include <memory>

#include "Base/BaseTypes.h"
#include "Base/MacroUtils.h"

#include "Database/Catalog_pb.h"
#include "Storage/Record.h"
#include "Storage/PageLoadedRecord.h"

namespace Storage {

// FlatRecordPage is the basic storage unit of a sort-merge intermediate
// tmpfile. It has a simple meta data at head followed by all records densely
// dumped as raw bytes. It only supports in-order read/write.
//
// Notice each record is dumped as | record_length | record data |
//                                      4 bytes     record_length
class FlatRecordPage {
 public:
  FlatRecordPage() = default;

  void Init();

  uint32 num_records() const { return num_records_; }
  byte* mutable_data() { return data_; }

  // Return the length and data of next record, and increment buffer offset.
  std::pair<uint32, byte*> ConsumeNextRecord();

  // Dump a record to page, and increment buffer offset.
  void DumpRecord(const RecordBase& record);

 private:
  uint32 num_records_ = 0;
  byte* data_ = nullptr;

  uint32 crt_rindex_ = 0;
  uint32 crt_offset_ = 0;

  FORBID_COPY_AND_ASSIGN(FlatRecordPage);
};


class SortMergeTempfileManager {
 public:
  SortMergeTempfileManager(const string& filename) = default;

  // - Open the file, in r/w mode.
  // - Create buffer FlatRecordPage;
  bool Init();

  // Get next record from the file.
  std::shared_ptr<RecordBase> NextRecord();

  // Write out (append) a record to file.
  bool WriteRecord(const RecordBase& record);

  string filename() const { return filename_; }

 private:
  string filename_;
  uint32 num_pages_;

  // There should only be one buffer FlatRecordPage for each
  // SortMergeTempfileManager. Each page is fetched from file / written to file
  // in order.
  std::unique_ptr<FlatRecordPage> buf_page_;
  uint32 crt_page_num_ = 0;

  FORBID_COPY_AND_ASSIGN(SortMergeTempfileManager);
};


class MergeSorter {
 public:
  struct Options {
    const string& db_name;
    uint32 txn_id;
    const DB::TableSchema* schema;
    std::vector<int> key_indexes;
    FileType file_type;  // data or index record?
  };

  MergeSorter(const Options& opts,
              const std::vector<std::shared_ptr<RecordBase>>& records);

  // Sort records, and return the output file name or empty in failure.
  string bool Sort();

 private:
  Options opts_;
};

}  // Storage

#endif  // STORAGE_MERGE_SORT_