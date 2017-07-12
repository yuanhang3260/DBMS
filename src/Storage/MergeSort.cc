#include <algorithm>
#include <queue>

#include "Base/Log.h"
#include "Base/Path.h"
#include "Base/Ptr.h"
#include "Base/Utils.h"
#include "Strings/Utils.h"
#include "IO/FileSystemUtils.h"
#include "Utility/CleanUp.h"

#include "Storage/MergeSort.h"

namespace Storage {

namespace {
const char* const kResultFile = "result";
}

// *************************** FlatRecordPage ******************************* //
FlatRecordPage::FlatRecordPage() {
  data_ = new byte[Storage::kPageSize];
  CHECK(data_, "Can't allocate page data");
}

FlatRecordPage::~FlatRecordPage() {
  if (data_) {
    delete[] data_;
  }
}

std::pair<uint32, byte*> FlatRecordPage::ConsumeNextRecord() {
  SANITY_CHECK(data_, "Buffer page is not created yet.");

  // Check meta data is parsed or not.
  if (num_records_ == 0) {
    memcpy(&num_records_, data_, sizeof(num_records_));
    // Loaded an empty page? Maybe, in case we did something wrong.
    if (num_records_ == 0) {
      return {0, nullptr}; 
    }
    crt_offset_ = sizeof(num_records_);
    crt_rindex_ = 0;
  }

  // Reach the end of this page.
  if (crt_rindex_ == num_records_) {
    return {0, nullptr};
  }

  uint32 record_length;
  memcpy(&record_length, data_ + crt_offset_, sizeof(record_length));
  byte* record_data = data_ + crt_offset_ + sizeof(record_length);
  crt_offset_ += (sizeof(record_length) + record_length);
  crt_rindex_++;

  return {record_length, record_data};
}

uint32 FlatRecordPage::DumpRecord(const RecordBase& record) {
  SANITY_CHECK(data_, "Buffer page is not created yet.");
  if (crt_offset_ + sizeof(uint32) + record.size() > Storage::kPageSize) {
    FinishPage();
    return 0;
  }

  if (num_records_ == 0) {
    crt_offset_ = sizeof(num_records_);
    crt_rindex_ = 0;
  }

  uint32 record_length = record.size();
  memcpy(data_ + crt_offset_, &record_length, sizeof(record_length));
  crt_offset_ += sizeof(record_length);
  record.DumpToMem(data_ + crt_offset_);
  crt_offset_ += record_length;
  num_records_++;

  return record_length;
}

void FlatRecordPage::FinishPage() {
  SANITY_CHECK(data_, "Buffer page is not created yet.");

  // Write number of records to the header of page.
  memcpy(data_, &num_records_, sizeof(num_records_));
}

void FlatRecordPage::Reset() {
  num_records_ = 0;
  crt_rindex_ = 0;
  crt_offset_ = 0;
}


// ********************** MergeSortTempfileManager ************************** //
MergeSortTempfileManager::MergeSortTempfileManager(
    const MergeSortOptions* opts, const std::string& filename) :
  opts_(opts),
  filename_(filename) {}

bool MergeSortTempfileManager::Init() {
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
  SANITY_CHECK(file_size % Storage::kPageSize == 0,
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

bool MergeSortTempfileManager::InitForReading() {
  if (!Init()) {
    return false;
  }
  buf_page_.reset();
  return true;
}

bool MergeSortTempfileManager::InitForWriting() {
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

std::shared_ptr<RecordBase> MergeSortTempfileManager::NextRecord() {
  // Init buffer FlatRecordPage - create instance, and read the first page from
  // file.
  if (!buf_page_) {
    buf_page_ = ptr::MakeUnique<FlatRecordPage>();
    crt_page_num_ = 0;
    int read_size = file_descriptor_->Read(buf_page_->mutable_data(),
                                           Storage::kPageSize);
    if (read_size != Storage::kPageSize) {
      LogERROR("Failed to fetch first page from file %s", filename_.c_str());
      return nullptr;
    }
  }

  // Consume next record from buffer FlatRecordPage.
  auto re = buf_page_->ConsumeNextRecord();
  uint32 record_length = re.first;
  byte* record_data = re.second;

  // Reach the end of this page. Fetch next page if exists.
  if (record_length == 0 || record_data == nullptr) {
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
    return NextRecord();
  }

  // Load next record.
  PageLoadedRecord plrecord;
  plrecord.GenerateRecordPrototype(*opts_->schema, opts_->key_fields,
                                   opts_->file_type, TREE_LEAVE);
  int load_size = plrecord.LoadFromMem(record_data);
  SANITY_CHECK(load_size == (int)record_length,
               "Error record from page %u - expect %d bytes, actual %d ",
               crt_page_num_, record_length, load_size);
  
  total_records_++;
  return plrecord.Shared_Record();
}

bool MergeSortTempfileManager::WriteRecord(const RecordBase& record) {
  if (!buf_page_) {
    buf_page_ = ptr::MakeUnique<FlatRecordPage>();
    crt_page_num_ = 0;
  }

  uint32 re = buf_page_->DumpRecord(record);
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
    return WriteRecord(record);
  }

  return re;
}

bool MergeSortTempfileManager::FinishWriting() {
  buf_page_->FinishPage();
  int nwrite = file_descriptor_->Write(buf_page_->data(), Storage::kPageSize);
  if (nwrite != Storage::kPageSize) {
    LogERROR("Error writing page %d to file", crt_page_num_);
    return false;
  }
  return file_descriptor_->Close() == 0;
}

bool MergeSortTempfileManager::DeleteFile() {
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

// *************************** MergeSorter ********************************** //
MergeSorter::MergeSorter(const MergeSortOptions& opts) : opts_(opts) {
  SANITY_CHECK(OptionsValid(), "Invalid MergeSortOptions");
  ProduceSortIndexes();
}

bool MergeSorter::Init() {
  return FileSystem::CreateDirRecursive(TempfileDir());
}

std::string MergeSorter::TempfileDir() const {
  return Path::JoinPath(Storage::DBDataDir(opts_.db_name), "MergeSort",
                        Strings::StrCat("txn_", std::to_string(opts_.txn_id)));
}

std::string MergeSorter::TempfilePath(int pass_num, int chunk_num) {
  return Path::JoinPath(TempfileDir(),
                        Strings::StrCat("pass_", std::to_string(pass_num),
                                        "_chunk_", std::to_string(chunk_num)));
}

bool MergeSorter::OptionsValid() {
  if (!opts_.schema) {
    LogERROR("schema nullptr");
    return false;
  }

  // Check index duplication.
  std::set<int> key_fields_set;
  for (int i : opts_.key_fields) {
    if (key_fields_set.find(i) != key_fields_set.end()) {
      LogERROR("Duplicated key index %d", i);
      return false;
    }
    key_fields_set.insert(i);
  }

  std::set<int> sort_fields_set;
  for (int i : opts_.sort_fields) {
    if (sort_fields_set.find(i) != sort_fields_set.end()) {
      LogERROR("Duplicated sort index %d", i);
      return false;
    }
    sort_fields_set.insert(i);
  }

  // Check sort_fields. It must be a subset of key_fields if records are
  // index records.
  if (opts_.file_type == INDEX_DATA) {
    for (int i : opts_.sort_fields) {
      if (i < 0 || i >= opts_.schema->fields_size()) {
        LogERROR("Sort index %d out of range : [0, %d)",
                 i, opts_.schema->fields_size());
        return false;
      }
    }
  } else if (opts_.file_type == INDEX) {
    for (int i : opts_.sort_fields) {
      if (key_fields_set.find(i) == key_fields_set.end()) {
        LogERROR("Sort index %d can't be find in key_fields", i);
      return false;
      }
    }
  } else {
    LogERROR("Invalid file type %d", opts_.file_type);
    return false;
  }

  return true;
}

void MergeSorter::ProduceSortIndexes() {
  sort_indexes_.clear();
  if (opts_.file_type == INDEX_DATA) {
    sort_indexes_ = opts_.sort_fields;
  } else if (opts_.file_type == INDEX) {
    for (int sort_field : opts_.sort_fields) {
      for (uint32 i = 0; i < opts_.key_fields.size(); i++) {
        if (sort_field == opts_.key_fields.at(i)) {
          sort_indexes_.push_back(i);
          break;
        }
      }
    }
  }
  // for (int i : sort_indexes_) {
  //   printf("%d ", i);
  // }
  // printf("\n");
  SANITY_CHECK(sort_indexes_.size() == opts_.sort_fields.size(),
               "sort indexes size mismatch with sort fields");
}

bool MergeSorter::SortComparator(std::shared_ptr<RecordBase> r1,
                                 std::shared_ptr<RecordBase> r2) {
  if (!opts_.desc) {
    return RecordBase::RecordComparator(*r1, *r2, sort_indexes_);
  } else {
    return RecordBase::RecordComparatorGt(*r1, *r2, sort_indexes_);
  }
}

std::string MergeSorter::Sort(
    const std::vector<std::shared_ptr<RecordBase>>& records) {
  // Clean up merge-sort directory.
  auto cleanup = Utility::CleanUp([&] {
    FileSystem::CleanDir(TempfileDir());
  });

  // Load records as much as possible to all available buffer pages, sort them
  // and write to tempfiles of first pass.
  auto comparator = [&] (std::shared_ptr<RecordBase> r1,
                         std::shared_ptr<RecordBase> r2) {
    return SortComparator(r1, r2);
  };

  std::vector<std::shared_ptr<MergeSortTempfileManager>> out_tempfiles;

  uint32 sort_size = 0;
  std::vector<std::shared_ptr<RecordBase>> sort_group;
  uint32 chunk_num = 0;
  auto create_chunkfile = [&] {
    // Sort current group and write to tempfile.
    std::stable_sort(sort_group.begin(), sort_group.end(), comparator);
    std::shared_ptr<MergeSortTempfileManager> out_file_manager(
        new MergeSortTempfileManager(&opts_, TempfilePath(0, chunk_num)));
    if (!out_file_manager->InitForWriting()) {
      LogERROR("Failed to init writing for tempfile %s",
               out_file_manager->filename().c_str());
      return false;
    }
    for (uint32 j = 0; j < sort_group.size(); j++) {
      out_file_manager->WriteRecord(*sort_group.at(j));
    }
    if (!out_file_manager->FinishWriting()) {
      LogERROR("Failed to finish writing file %s",
               out_file_manager->filename().c_str());
      return false;
    }
    out_tempfiles.push_back(out_file_manager);

    chunk_num++;
    sort_size = 0;
    sort_group.clear();
    return true;
  };

  for (uint32 i = 0; i < records.size(); i++) {
    if (sort_size + records.at(i)->size() >
            Storage::kPageSize * opts_.num_buf_pages) {
      if (!create_chunkfile()) {
        return "";
      }
    }
    sort_group.push_back(records.at(i));
    sort_size += records.at(i)->size();
  }
  if (!sort_group.empty()) {
    if (!create_chunkfile()) {
      return "";
    }
  }

  // Begin merge sort.
  uint32 pass_num = 1;
  std::vector<std::shared_ptr<MergeSortTempfileManager>> in_tempfiles;
  while (out_tempfiles.size() > 1) {
    // Output of last pass is input of this pass.
    for (const auto& file : out_tempfiles) {
      in_tempfiles.push_back(file);
      if (!in_tempfiles.back()->InitForReading()) {
        LogERROR("Failed to init reading for tempfile %s",
                 in_tempfiles.back()->filename().c_str());
        return "";
      }
    }
    out_tempfiles.clear();

    // Group input files by num_buf_pages, and do merge sort on each group.
    uint32 num_groups =
        (in_tempfiles.size() + opts_.num_buf_pages - 1) / opts_.num_buf_pages;
    for (uint32 group = 0; group < num_groups; group++) {
      // Do merge sort on this group and write out result to a tempfile.
      auto out_file_manager = ptr::MakeShared<MergeSortTempfileManager>(
                                  &opts_, TempfilePath(pass_num, group));
      if (!out_file_manager->InitForWriting()) {
        LogERROR("Failed to init writing for tempfile %s",
                 out_file_manager->filename().c_str());
        return "";
      }

      // Create a min-heap and do K-lists sort merge.
      struct HeapNode {
        std::shared_ptr<RecordBase> record;
        uint32 file_index;
      };
      auto hp_comparator = [&] (const HeapNode& n1, const HeapNode& n2) {
        return !comparator(n1.record, n2.record);
      };
      std::priority_queue<struct HeapNode, std::vector<struct HeapNode>,
                          decltype(hp_comparator)> min_heap(hp_comparator);

      // Begin merge sorting!
      uint32 group_start = group * opts_.num_buf_pages;
      uint32 group_end = Utils::Min((group + 1) * opts_.num_buf_pages,
                                    in_tempfiles.size());
      //printf("group_start = %d, group_end = %d\n", group_start, group_end);
      for (uint32 i = group_start; i < group_end; i++) {
        auto record = in_tempfiles.at(i)->NextRecord();
        SANITY_CHECK(record, "Get a null record from file %s",
                             in_tempfiles.at(i)->filename().c_str());
        HeapNode node = {record, i};
        min_heap.push(node);
      }

      while (!min_heap.empty()) {
        // Fetch top element from heap.
        auto min_ele = min_heap.top();
        min_heap.pop();
        if (!out_file_manager->WriteRecord(*min_ele.record)) {
          LogERROR("Failed to write record to file %s",
                   out_file_manager->filename().c_str());
          return "";
        }

        auto next_record = in_tempfiles.at(min_ele.file_index)->NextRecord();
        if (next_record) {
          HeapNode node = {next_record, min_ele.file_index};
          min_heap.push(node);
        }
      }
      // Finish writing an out tempfile.
      if (!out_file_manager->FinishWriting()) {
        LogERROR("Failed to finish writing file %s",
                 out_file_manager->filename().c_str());
        return "";
      }
      out_tempfiles.push_back(out_file_manager);
    }

    // Input tempfile of this pass is no longer needed. Delete them.
    for (auto& in_file : in_tempfiles) {
      if (!in_file->DeleteFile()) {
        return "";
      }
    }
    in_tempfiles.clear();

    pass_num++;
  }

  // Rename the final out file as "result".
  if (!FileSystem::RenameFile(out_tempfiles.at(0)->filename(), kResultFile)) {
    LogERROR("Failed to rename final result file");
    return "";
  }

  cleanup.clear();
  return Path::JoinPath(TempfileDir(), kResultFile);
}


}  // namespace Storage
