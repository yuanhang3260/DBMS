#include <algorithm>
#include <queue>

#include "Base/Log.h"
#include "Base/Path.h"
#include "Base/Ptr.h"
#include "Base/Utils.h"
#include "Strings/Utils.h"
#include "IO/FileSystemUtils.h"
#include "Utility/CleanUp.h"
#include "Storage/FlatTupleFile.h"

namespace Storage {

namespace {
using Query::FetchedResult;
using Query::TableRecordMeta;
using Query::ResultRecord;
}

FlatTupleFileOptions::FlatTupleFileOptions(
    const Query::FetchedResult::TupleMeta& tuple_meta,
    const std::vector<std::string>& tables) {
  for (const auto& table_name : tables) {
    auto it = tuple_meta.find(table_name);
    CHECK(it != tuple_meta.end(),
          "Can't find meta for table %s", table_name.c_str());
    table_metas.emplace(table_name, &it->second);
  }
}

// **************************** FlatTuplePage ******************************* //
FlatTuplePage::FlatTuplePage(const FlatTupleFileOptions* options) :
    opts_(options) {
  data_ = new byte[Storage::kPageSize];
  CHECK(data_, "Can't allocate page data");
}

FlatTuplePage::~FlatTuplePage() {
  if (data_) {
    delete[] data_;
  }
}

std::pair<uint32, byte*> FlatTuplePage::GetNextTuple() {
  CHECK(data_, "Buffer page is not created yet.");

  // Check meta data is parsed or not.
  if (num_tuples_ == 0) {
    memcpy(&num_tuples_, data_, sizeof(num_tuples_));
    // Loaded an empty page? Maybe, in case we did something wrong.
    if (num_tuples_ == 0) {
      return {0, nullptr}; 
    }
    crt_offset_ = sizeof(num_tuples_);
    crt_tindex_ = 0;
  }

  // Reach the end of this page.
  if (crt_tindex_ == num_tuples_) {
    return {0, nullptr};
  }

  uint32 tuple_length;
  memcpy(&tuple_length, data_ + crt_offset_, sizeof(tuple_length));
  byte* tuple_data = data_ + crt_offset_ + sizeof(tuple_length);
  crt_offset_ += (sizeof(tuple_length) + tuple_length);
  crt_tindex_++;

  return {tuple_length, tuple_data};
}

uint32 FlatTuplePage::DumpTuple(const Query::FetchedResult::tuple& tuple) {
  CHECK(data_, "Buffer page is not created yet.");
  if (crt_offset_ + sizeof(uint32) + FetchedResult::TupleSize(tuple) > 
      Storage::kPageSize) {
    FinishPage();
    return 0;
  }

  if (num_tuples_ == 0) {
    crt_offset_ = sizeof(num_tuples_);
    crt_tindex_ = 0;
  }

  // Dump tuple size first.
  uint32 tuple_length = FetchedResult::TupleSize(tuple);
  memcpy(data_ + crt_offset_, &tuple_length, sizeof(tuple_length));
  crt_offset_ += sizeof(tuple_length);

  // Dump tuple data, record by record. Note table_metas is sorted by table
  // name, so the dump order is also by table name.
  for (const auto& iter : opts_->table_metas) {
    const auto& table_name = iter.first;
    auto it = tuple.find(table_name);
    CHECK(it != tuple.end(),
          "Can't find record of table %s from tuple", table_name.c_str());
    it->second.record->DumpToMem(data_ + crt_offset_);
  }
  crt_offset_ += tuple_length;
  num_tuples_++;

  return tuple_length;
}

void FlatTuplePage::FinishPage() {
  CHECK(data_, "Buffer page is not created yet.");

  // Write number of records to the header of page.
  memcpy(data_, &num_tuples_, sizeof(num_tuples_));
}

void FlatTuplePage::Reset() {
  num_tuples_ = 0;
  crt_tindex_ = 0;
  crt_offset_ = 0;
}


// **************************** FlatTupleFile ******************************* //
FlatTupleFile::FlatTupleFile(
    const FlatTupleFile::Options& opts, const std::string& filename) :
  opts_(opts),
  filename_(filename) {}

bool FlatTupleFile::Init() {
  if (!FileSystem::FileExists(filename_)) {
    LogERROR("MergeSort tempfile %s doesn't exist!", filename_.c_str());
    return false;
  }

  // Check file size. It must be a multiple of PageSize.
  auto file_size = FileSystem::FileSize(filename_);
  if (file_size < 0) {
    LogERROR("Failed to get file size of %s", filename_.c_str());
    return false;
  }
  CHECK(file_size % Storage::kPageSize == 0,
        "merge sort file size expectd to be a multiple of kPageSize, "
        "but got %ld", file_size);
  num_pages_ = file_size / Storage::kPageSize;

  // Open the file in r/w mode.
  file_descriptor_ = ptr::MakeUnique<IO::FileDescriptor>(
                          filename_, IO::FileDescriptor::READ_WRITE);
  if (file_descriptor_->closed()) {
    return false;
  }

  return true;
}

bool FlatTupleFile::InitForReading() {
  if (!Init()) {
    return false;
  }
  buf_page_.reset();
  return true;
}

bool FlatTupleFile::InitForWriting() {
  // Check the file. If it already exists, empty it. Otherwise create new file. 
  if (FileSystem::FileExists(filename_)) {
    if (!FileSystem::TruncateFile(filename_, 0)) {
      LogERROR("Failed to empty existing file %s", filename_.c_str());
      return false;
    }
  } else if (!FileSystem::CreateFile(filename_)) {
    LogERROR("Failed to create file %s", filename_.c_str());
    return false;
  }

  if (!Init()) {
    return false;
  }
  buf_page_.reset();
  return true;
}

std::shared_ptr<FetchedResult::Tuple> FlatTupleFile::NextTuple() {
  // Init buffer FlatRecordPage - create instance, and read the first page from
  // file.
  if (!buf_page_) {
    buf_page_ = ptr::MakeUnique<FlatTuplePage>(&opts_);
    crt_page_num_ = 0;
    int read_size = file_descriptor_->Read(buf_page_->mutable_data(),
                                           Storage::kPageSize);
    if (read_size != Storage::kPageSize) {
      LogERROR("Failed to fetch first page from file %s", filename_.c_str());
      return nullptr;
    }
  }

  // Consume next record from buffer FlatRecordPage.
  auto re = buf_page_->GetNextTuple();
  uint32 tuple_length = re.first;
  byte* tuple_data = re.second;

  // Reach the end of this page. Fetch next page if exists.
  if (tuple_length == 0 || tuple_data == nullptr) {
    crt_page_num_++;
    if (crt_page_num_ == num_pages_) {
      // Reach the end of file, no more record is available.
      return nullptr;
    }

    // Reset buffer FlatRecordPage, and load next page data into it.
    buf_page_->Reset();
    int read_size = file_descriptor_->Read(buf_page_->mutable_data(),
                                           Storage::kPageSize);
    if (read_size != Storage::kPageSize) {
      LogERROR("Failed to fetch next page %d from file %s",
               crt_page_num_, filename_.c_str());
      return nullptr;
    }

    // Recursive call - the next page is loaded and ready to be read.
    return NextTuple();
  }

  // Load next tuple.
  PageLoadedRecord plrecord;
  plrecord.GenerateRecordPrototype(*opts_->schema, opts_->key_fields,
                                   opts_->file_type, TREE_LEAVE);

  int load_size = plrecord.LoadFromMem(tuple_data);
  SANITY_CHECK(load_size == (int)tuple_length,
               "Error record from page %u - expect %d bytes, actual %d ",
               crt_page_num_, tuple_length, load_size);
  
  total_records_++;
  return plrecord.shared_record();
}

bool FlatTupleFile::WriteTuple(const FetchedResult::Tuple& tuple) {
  if (!buf_page_) {
    buf_page_ = ptr::MakeUnique<FlatRecordPage>();
    crt_page_num_ = 0;
  }

  uint32 re = buf_page_->DumpTuple(tuple);
  if (re <= 0) {
    // Current page is full. Flush it to disk and clear the buffer for new
    // coming data.
    int nwrite = file_descriptor_->Write(buf_page_->data(), Storage::kPageSize);
    if (nwrite != Storage::kPageSize) {
      LogERROR("Error writing page %d to file", crt_page_num_);
      return false;
    }
    crt_page_num_++;
    buf_page_->Reset();
    // Recursive call - Now a new empty page is ready to write.
    return WriteTuple(tuple);
  }

  return re;
}

bool FlatTupleFile::FinishWriting() {
  buf_page_->FinishPage();
  int nwrite = file_descriptor_->Write(buf_page_->data(), Storage::kPageSize);
  if (nwrite != Storage::kPageSize) {
    LogERROR("Error writing page %d to file", crt_page_num_);
    return false;
  }
  return file_descriptor_->Close() == 0;
}

bool FlatTupleFile::DeleteFile() {
  int re = file_descriptor_->Close();
  if (re != 0) {
    LogERROR("Failed to close file %s", filename_.c_str());
    return false;
  }

  if (!FileSystem::Remove(filename_)) {
    LogERROR("Failed to delete file %s", filename_.c_str());
    return false;
  }

  return true;
}

}  // namespace Storage
