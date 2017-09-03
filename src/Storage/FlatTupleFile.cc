#include <algorithm>
#include <queue>

#include "Base/Log.h"
#include "Base/Path.h"
#include "Base/Ptr.h"
#include "Base/Utils.h"
#include "Strings/Utils.h"
#include "IO/FileSystemUtils.h"
#include "Utility/CleanUp.h"
#include "Utility/Uuid.h"

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

uint32 FlatTuplePage::DumpTuple(const Query::FetchedResult::Tuple& tuple) {
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
    crt_offset_ += it->second.record->DumpToMem(data_ + crt_offset_);
  }
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
FlatTupleFile::FlatTupleFile(const FlatTupleFileOptions& opts) :
  opts_(opts) {}

FlatTupleFile::FlatTupleFile(
    const FlatTupleFileOptions& opts, const std::string& filename) :
  opts_(opts),
  filename_(filename) {}

FlatTupleFile::~FlatTupleFile() {
  Close();
}

bool FlatTupleFile::Close() {
  if (file_descriptor_) {
    return file_descriptor_->Close() == 0;
  }
  return true;
}

std::string FlatTupleFile::TempfileDir() const {
  return Path::JoinPath(Storage::DBDataDir(opts_.db_name),
                        Strings::StrCat("txn_", std::to_string(opts_.txn_id)),
                        "tmpfiles");
}

std::string FlatTupleFile::NewTempfileName() const {
  return Path::JoinPath(TempfileDir(),
                        Strings::StrCat("ftf_", UUID::GenerateUUID()));
}

std::string FlatTupleFile::MergeSortChunkFileName(int pass_num, int chunk_num) {
  return Strings::StrCat(filename_,
                         "_ms_pass_", std::to_string(pass_num),
                         "_chunk_", std::to_string(chunk_num));
}

bool FlatTupleFile::Init() {
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
  if (!file_descriptor_) {
    file_descriptor_ = ptr::MakeUnique<IO::FileDescriptor>(
                            filename_, IO::FileDescriptor::READ_WRITE);
    if (file_descriptor_->closed()) {
      return false;
    }
  } else {
    auto re = lseek(file_descriptor_->fd(), 0, SEEK_SET);
    if (re < 0) {
      LogERROR("Failed to seek position to 0 for reading");
      return false;
    }
  }

  return true;
}

bool FlatTupleFile::InitForReading() {
  if (!FileSystem::FileExists(filename_)) {
    LogERROR("FlatTupleFile \"%s\" doesn't exist!", filename_.c_str());
    return false;
  }

  if (!Init()) {
    return false;
  }
  buf_page_.reset();
  return true;
}

bool FlatTupleFile::InitForWriting() {
  FileSystem::CreateDirRecursive(TempfileDir());

  if (filename_.empty()) {
    filename_ = NewTempfileName();
  }

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
  auto tuple = std::make_shared<FetchedResult::Tuple>();
  uint32 load_size = 0;
  for (const auto& iter : opts_.table_metas) {
    const auto& table_name = iter.first;
    const auto* table_meta = iter.second;
    auto& result_record = (*tuple)[table_name];
    if (table_meta->record_type == Storage::DATA_RECORD) {
      result_record.record.reset(new Storage::DataRecord());
    } else if (table_meta->record_type == Storage::INDEX_RECORD) {
      result_record.record.reset(new Storage::IndexRecord());
    } else {
      LogFATAL("Unexpected table record type %s",
               Storage::RecordTypeStr(table_meta->record_type).c_str());
    }

    // Set table metadata for the loaded table record.
    result_record.meta = table_meta;
    // Load record fields.
    for (const auto& field_info : table_meta->fetched_fields) {
      result_record.record->AddField(field_info);
    }
    load_size += result_record.record->LoadFromMem(tuple_data + load_size);
    // result_record.record->Print();
    // printf("load_size = %d\n", load_size);
  }

  CHECK(load_size == tuple_length,
        "Error record from page %u - expect %d bytes, actual %d ",
        crt_page_num_, tuple_length, load_size);

  total_records_++;
  return tuple;
}

bool FlatTupleFile::WriteTuple(const FetchedResult::Tuple& tuple) {
  if (!buf_page_) {
    buf_page_ = ptr::MakeUnique<FlatTuplePage>(&opts_);
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
  if (!buf_page_) {
    return true;
  }

  buf_page_->FinishPage();
  int nwrite = file_descriptor_->Write(buf_page_->data(), Storage::kPageSize);
  if (nwrite != Storage::kPageSize) {
    LogERROR("Error writing page %d to file", crt_page_num_);
    return false;
  }
  return true;
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

  file_descriptor_.reset();

  return true;
}

bool FlatTupleFile::Sort(const std::vector<Query::Column>& columns) {
  std::vector<std::string> tmpfiles;
  // Clean up merge-sort directory.
  auto cleanup = Utility::CleanUp([&] {
    for (const auto& tmp_file : tmpfiles) {
      FileSystem::Remove(tmp_file);
    }
  });

  // Compare two tuples based on columns.
  auto comparator = [&] (std::shared_ptr<FetchedResult::Tuple> t1,
                         std::shared_ptr<FetchedResult::Tuple> t2) {
    return FetchedResult::CompareBasedOnColumns(*t1, *t2, columns) < 0;
  };

  std::vector<std::shared_ptr<FlatTupleFile>> out_tempfiles;

  uint32 sort_size = 0;
  std::vector<std::shared_ptr<FetchedResult::Tuple>> sort_group;
  uint32 chunk_num = 0;
  auto create_chunkfile = [&] {
    // Sort current group and write to tempfile.
    std::stable_sort(sort_group.begin(), sort_group.end(), comparator);
    tmpfiles.push_back(MergeSortChunkFileName(0, chunk_num));
    auto out_file = std::make_shared<FlatTupleFile>(opts_, tmpfiles.back());
    if (!out_file->InitForWriting()) {
      LogERROR("Failed to init writing for tempfile %s",
               out_file->filename().c_str());
      return false;
    }
    for (uint32 j = 0; j < sort_group.size(); j++) {
      out_file->WriteTuple(*sort_group.at(j));
    }
    if (!out_file->FinishWriting()) {
      LogERROR("Failed to finish writing file %s",
               out_file->filename().c_str());
      return false;
    }
    out_tempfiles.push_back(out_file);

    chunk_num++;
    sort_size = 0;
    sort_group.clear();
    return true;
  };

  // Read all tuples and crete chunk files of pass 0.
  if (!InitForReading()) {
    return false;
  }

  while (true) {
    auto tuple = NextTuple();
    if (!tuple) {
      break;
    }
    if (sort_size + FetchedResult::TupleSize(*tuple) >
            (Storage::kPageSize - sizeof(uint32)) * opts_.num_buf_pages) {
      if (!create_chunkfile()) {
        return false;
      }
    }
    sort_group.push_back(tuple);
    sort_size += FetchedResult::TupleSize(*tuple);
  }
  if (!sort_group.empty()) {
    if (!create_chunkfile()) {
      return false;
    }
  }

  // Begin merge sort.
  uint32 pass_num = 1;
  std::vector<std::shared_ptr<FlatTupleFile>> in_tempfiles;
  while (out_tempfiles.size() > 1) {
    // Output of last pass is input of this pass.
    for (const auto& file : out_tempfiles) {
      in_tempfiles.push_back(file);
      if (!in_tempfiles.back()->InitForReading()) {
        LogERROR("Failed to init reading for tempfile %s",
                 in_tempfiles.back()->filename().c_str());
        return false;
      }
    }
    out_tempfiles.clear();

    // Group input files by num_buf_pages, and do merge sort on each group.
    uint32 num_groups =
        (in_tempfiles.size() + opts_.num_buf_pages - 1) / opts_.num_buf_pages;
    for (uint32 group = 0; group < num_groups; group++) {
      // Do merge sort on this group and write out result to a tempfile.
      tmpfiles.push_back(MergeSortChunkFileName(pass_num, group));
      auto out_file = std::make_shared<FlatTupleFile>(opts_, tmpfiles.back());
      if (!out_file->InitForWriting()) {
        LogERROR("Failed to init writing for tempfile %s",
                 out_file->filename().c_str());
        return false;
      }

      // Create a min-heap and do K-lists sort merge.
      struct HeapNode {
        std::shared_ptr<FetchedResult::Tuple> tuple;
        uint32 file_index;
      };
      auto hp_comparator = [&] (const HeapNode& n1, const HeapNode& n2) {
        return !comparator(n1.tuple, n2.tuple);
      };
      std::priority_queue<struct HeapNode, std::vector<struct HeapNode>,
                          decltype(hp_comparator)> min_heap(hp_comparator);

      // Begin merge sorting!
      uint32 group_start = group * opts_.num_buf_pages;
      uint32 group_end = Utils::Min((group + 1) * opts_.num_buf_pages,
                                    in_tempfiles.size());
      //printf("group_start = %d, group_end = %d\n", group_start, group_end);
      for (uint32 i = group_start; i < group_end; i++) {
        auto tuple = in_tempfiles.at(i)->NextTuple();
        CHECK(tuple, "Get a null record from file %s",
                      in_tempfiles.at(i)->filename().c_str());
        HeapNode node = {tuple, i};
        min_heap.push(node);
      }

      while (!min_heap.empty()) {
        // Fetch top element from heap.
        auto min_ele = min_heap.top();
        min_heap.pop();
        if (!out_file->WriteTuple(*min_ele.tuple)) {
          LogERROR("Failed to write record to file %s",
                   out_file->filename().c_str());
          return false;
        }

        auto next_tuple = in_tempfiles.at(min_ele.file_index)->NextTuple();
        if (next_tuple) {
          HeapNode node = {next_tuple, min_ele.file_index};
          min_heap.push(node);
        }
      }
      // Finish writing an out tempfile.
      if (!out_file->FinishWriting()) {
        LogERROR("Failed to finish writing file %s",
                 out_file->filename().c_str());
        return false;
      }
      out_tempfiles.push_back(out_file);
    }

    // Input tempfile of this pass is no longer needed. Delete them.
    for (auto& in_file : in_tempfiles) {
      if (!in_file->DeleteFile()) {
        return false;
      }
    }
    in_tempfiles.clear();

    pass_num++;
  }

  // Delete current file, and replace with the final result file.
  if (!DeleteFile()) {
    return false;
  }
  if (!out_tempfiles.at(0)->Close()) {
    LogERROR("Failed to close the final result chunk file");
    return false;
  }
  if (!FileSystem::RenameFile(out_tempfiles.at(0)->filename(),
                              FileSystem::FileName(filename_))) {
    LogERROR("Failed to rename final result file");
    return false;
  }
  InitForReading();

  cleanup.clear();

  return true;
}

}  // namespace Storage
