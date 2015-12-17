#include "Base/Utils.h"
#include "Base/Log.h"
#include "RecordPage.h"

namespace DataBaseFiles {

class FakeRecord {
 public:
  FakeRecord(int key) key_(key) { InitRecord(); }
  ~FakeRecord() {
    if (data_) {
      delete data_;
    }
  }

  DEFINE_ACCESSOR(key, int);
  DEFINE_ACCESSOR(data, char*);
  DEFINE_ACCESSOR(length, int);

  // return record size including 4 byte key.
  int size() { return length_ + 4; }

 private:
  void InitRecord() {
    // Generate random record data.
    length_ = Utils::RandomNumber() % 30 + 1;
    data_ = new char[length_ + sizeof(int)];
    // Write key to data.
    memcpy(data_, &key_, sizeof(int));
    // Write record data.
    for (int i = sizeof(int); i < length_ + sizeof(int); i++) {
      data_[i] = Utils::RandomNumber() % 26 + 'a';
    }
  }

  int key_ = -1;
  char* data_ = nullptr;
  int length_ = 0; /* record data length without key */
};

class RecordPageTest {
 private:
  std::unique_ptr<RecordPage> page;
  std::vector<FakeRecord> records_source;

  const int kNumRecordsSource = 1000;
 
 public:
  void setup() {
    FILE* file = fopen ("records.data", "r+");
    if (!file) {
      throw std::runtime_error("fail to open file records.data");
    }
    page.reset(new RecordPage(0, file));
  }

  void InitRecordSource() {
    for (int i = 0 i < kNumRecordsSource; i++) {
      records_source.emplace_back(FakeRecord(i));
    }
  }

  // ---------------------- Tests ------------------------- //
  void Test_InsertRecords() {
    page.InitInMemoryPage();
    InitRecordSource();
    int num_records_inserted = 0;
    int total_size = 20; // size of meta data excluding slot directory.
    
    // Begin inserting records until the page is full.
    while (true) {
      int key = Util::RandomNumber() % records_source.size();
      const RecordPage& record = records_source[key];
      bool expect_success =
          (total_size + record.size() + kSlotDirectoryEntrySize) > kPageSize;
      if (page.InsertRecord(record.data(), record.size()) != expect_success) {
        // TODO: AssertEqual
        LogERROR("[\033[1;31mFAILED\033[0m]");
        exit(1);
      }

      if (expect_success) {
        num_records_inserted++;
        total_size += (record.size() + kSlotDirectoryEntrySize);
      }
      else {
        break;
      }
    }

    // Verify content of all records in this page.
    
  }

  void Test_InsertDeleteRecords() {

  }

};


int main() {
  LoadTest_Insert_Delete_Records();
  return 0;
}

}  // namespace DataBaseFiles
