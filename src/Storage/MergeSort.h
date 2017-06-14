#ifndef STORAGE_MERGE_SORT_
#define STORAGE_MERGE_SORT_

#include <memory>

#include "Base/BaseTypes.h"
#include "Base/MacroUtils.h"
#include "IO/FileDescriptor.h"

#include "Database/Catalog_pb.h"
#include "Storage/PageRecord_Common.h"
#include "Storage/Record.h"

namespace Storage {

// FlatRecordPage is the basic storage unit of a sort-merge intermediate
// tmpfile. It has a simple meta data at head followed by all records densely
// dumped as raw bytes. It only supports in-order read/write.
//
// Notice each record is dumped as | record_length | record data |
//                                      4 bytes     record_length
class FlatRecordPage {
 public:
  FlatRecordPage();
  ~FlatRecordPage();

  bool Init();

  uint32 num_records() const { return num_records_; }
  uint32 crt_rindex() const { return crt_rindex_; }
  byte* mutable_data() { return data_; }
  const byte* data() const { return data_; }

  // Return the length and data of next record, and increment buffer offset.
  std::pair<uint32, byte*> ConsumeNextRecord();

  // Dump a record to page, and increment buffer offset. Return the length of
  // the record.
  uint32 DumpRecord(const RecordBase& record);
  void FinishPage();

  void Reset();

 private:
  uint32 num_records_ = 0;
  byte* data_ = nullptr;

  uint32 crt_rindex_ = 0;
  uint32 crt_offset_ = 0;

  FORBID_COPY_AND_ASSIGN(FlatRecordPage);
};

struct MergeSortOptions {
  MergeSortOptions(const std::string& db_name_,
                   uint32 txn_id_,
                   const DB::TableSchema* schema_,
                   const std::vector<int>& key_indexes_,
                   FileType file_type_) :
    db_name(db_name_),
    txn_id(txn_id_),
    schema(schema_),
    key_indexes(key_indexes_),
    file_type(file_type_) {}

  std::string db_name;
  uint32 txn_id;
  const DB::TableSchema* schema;
  std::vector<int> key_indexes;
  FileType file_type;  // data or index record?
};

class MergeSortTempfileManager {
 public:
  explicit MergeSortTempfileManager(const MergeSortOptions* opts,
                                    const std::string& filename);

  // Open the file, in r/w mode.
  bool Init();

  // Get next record from the file.
  bool InitForReading();
  std::shared_ptr<RecordBase> NextRecord();

  // Write out (append) a record to file.
  bool WriteRecord(const RecordBase& record);
  // FinishFile must be called after writing all records to file. It flushes
  // the last page to disk and close the file descriptor.
  bool FinishFile();

  std::string filename() const { return filename_; }

 private:
  const MergeSortOptions* opts_;
  std::string filename_;
  uint32 num_pages_;

  std::unique_ptr<IO::FileDescriptor> file_descriptor_;

  // There should only be one buffer FlatRecordPage for each
  // MergeSortTempfileManager. Each page is fetched from file / written to file
  // in order.
  std::unique_ptr<FlatRecordPage> buf_page_;
  uint32 crt_page_num_ = 0;
  uint32 total_records_ = 0;

  FORBID_COPY_AND_ASSIGN(MergeSortTempfileManager);
};


class MergeSorter {
 public:
  explicit MergeSorter(const MergeSortOptions& opts);

  bool Init();

  // Sort records, and return the output file name or empty in failure.
  std::string Sort(const std::vector<std::shared_ptr<RecordBase>>& records);

  // Tempfile naming : //data/db_name/MergeSort/txnid_pass_chunk.
  std::string TempfilePath(int pass_num, int chunk_num);

  const MergeSortOptions& options() const { return opts_; }

 private:
  MergeSortOptions opts_;
};

}  // Storage

#endif  // STORAGE_MERGE_SORT_