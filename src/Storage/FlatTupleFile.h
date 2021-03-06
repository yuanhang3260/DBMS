#ifndef STORAGE_FLAT_TUPLE_FILE_H_
#define STORAGE_FLAT_TUPLE_FILE_H_

#include <memory>

#include "Base/BaseTypes.h"
#include "Base/MacroUtils.h"
#include "IO/FileDescriptor.h"

#include "Database/Catalog_pb.h"
#include "Query/Tuple.h"
#include "Storage/PageRecord_Common.h"
#include "Storage/Record.h"

namespace Storage {

struct FlatTupleFileOptions{
  FlatTupleFileOptions(const Query::TupleMeta& tuple_meta,
                       const std::vector<std::string>& tables);
  // Metadata of each table's record. Note this map also gives the order of
  // records when tuple is dumped to/loaded from memory, because the map is
  // sorted by table name.
  using TableMetas = std::map<std::string, const Query::TableRecordMeta*>;
  TableMetas table_metas;

  std::string db_name;
  uint64 txn_id;
  uint32 num_buf_pages = 5;
};

// FlatTuplePage is the basic storage unit of FlatRecordFile. It has a simple
// meta data at head followed by all records densely dumped as raw bytes.
//
// Each tuple is dumped as | tuple_length | tuple data |
//                             4 bytes     tuple_length
class FlatTuplePage {
 public:
  FlatTuplePage(const FlatTupleFileOptions* options);
  ~FlatTuplePage();

  bool Init();

  uint32 num_tuples() const { return num_tuples_; }
  uint32 crt_tindex() const { return crt_tindex_; }
  uint32 crt_offset() const { return crt_offset_; }
  byte* mutable_data() { return data_; }
  const byte* data() const { return data_; }

  // Return the length and data of next tuple, and increment buffer offset.
  std::shared_ptr<Query::Tuple> GetNextTuple();

  // Dump a tuple to page, and increment buffer offset. Return the length of
  // the record.
  uint32 DumpTuple(const Query::Tuple& tuple);
  void FinishPage();

  // Restore read status to a prev snapshot.
  bool Restore(uint32 num_tuples, uint32 tindex, uint32 offset);

  void Reset();

 private:
  const FlatTupleFileOptions* opts_;

  uint32 num_tuples_ = 0;
  byte* data_ = nullptr;

  uint32 crt_tindex_ = 0;
  uint32 crt_offset_ = 0;

  FORBID_COPY_AND_ASSIGN(FlatTuplePage);
};


class FlatTupleFile {
 public:
  explicit FlatTupleFile(const FlatTupleFileOptions& opts);
  FlatTupleFile(const FlatTupleFileOptions& opts, const std::string& filename);

  ~FlatTupleFile();

  // It must be called before reading records from file.
  bool InitForReading();

  // It must be called before writing records to file.
  bool InitForWriting();
  // Write out (append) a record to file.
  bool WriteTuple(const Query::Tuple& tuple);
  // FinishFile must be called after writing all records to file. It flushes
  // the last page to disk and close the file descriptor.
  bool FinishWriting();

  bool Close();

  std::string filename() const { return filename_; }

  bool DeleteFile();

  bool Sort(const std::vector<Query::Column>& columns);

  // Iterator for reading tuples. This is a forward-only iterator.
  class Iterator {
   public:
    Iterator() = default;
    Iterator(FlatTupleFile* ft_file);
    Iterator(const Iterator& other);
    Iterator& operator=(const Iterator& other);

    // Get next record from the file.
    std::shared_ptr<Query::Tuple> NextTuple();
    // Store current page reading status. Called whenever a new tuple is read.
    void UpdatePageReadState();
   
   private:
    uint32 page_num_ = 0;
    uint32 page_tuples_ = 0;
    uint32 page_tuple_index_ = 0;
    uint32 page_offset_ = 0;

    FlatTupleFile* ft_file_ = nullptr;
    std::unique_ptr<FlatTuplePage> buf_page_;
  };

  Iterator GetIterator() { return Iterator(this); }

 private:
  // Check file and open it.
  bool Init();

  std::string TempfileDir() const;
  std::string NewTempfileName() const;
  std::string MergeSortChunkFileName(int pass_num, int chunk_num);

  FlatTupleFileOptions opts_;
  std::string filename_;
  uint32 num_pages_;

  std::unique_ptr<IO::FileDescriptor> file_descriptor_;

  // There should only be one buffer FlatTuplePage for each
  // MergeSortTempfileManager. Each page is fetched from file / written to file
  // in order.
  std::unique_ptr<FlatTuplePage> buf_page_;
  uint32 crt_page_num_ = 0;

  enum State {
    INIT,
    WRITING,
    READING,
  };
  State state_ = INIT;

  FORBID_COPY_AND_ASSIGN(FlatTupleFile);
};

}  // Storage

#endif  // STORAGE_FLAT_TUPLE_FILE_H_