#include "Base/Log.h"
#include "Base/Path.h"
#include "Base/Ptr.h"
#include "Strings/Utils.h"
#include "IO/FileSystemUtils.h"

#include "Storage/MergeSort.h"

namespace Storage {

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
  if (!FileSystem::FileExists(filename_) &&
      !FileSystem::CreateFile(filename_)) {
    LogERROR("Failed to create file %s", filename_);
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
  plrecord.GenerateRecordPrototype(*opts_->schema, opts_->key_indexes,
                                   opts_->file_type, TREE_LEAVE);
  int load_size = plrecord.LoadFromMem(record_data);
  SANITY_CHECK(load_size == (int)record_length,
               "Error record from page %ud - expect %d bytes, actual %d ",
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

bool MergeSortTempfileManager::FinishFile() {
  buf_page_->FinishPage();
  int nwrite = file_descriptor_->Write(buf_page_->data(), Storage::kPageSize);
  if (nwrite != Storage::kPageSize) {
    LogERROR("Error writing page %d to file", crt_page_num_);
    return false;
  }
  return file_descriptor_->Close();
}


// *************************** MergeSorter ********************************** //
MergeSorter::MergeSorter(const MergeSortOptions& opts) : opts_(opts) {}

bool MergeSorter::Init() {
  return FileSystem::CreateDir(
            Path::JoinPath(Storage::DBDataDir(opts_.db_name), "MergeSort"));
}

std::string MergeSorter::TempfilePath(int pass_num, int chunk_num) {
  return Path::JoinPath(Storage::DBDataDir(opts_.db_name), "MergeSort",
                        Strings::StrCat("txn_", std::to_string(opts_.txn_id),
                                        "_pass_", std::to_string(pass_num),
                                        "_chunk_", std::to_string(chunk_num)));
}


}  // namespace Storage