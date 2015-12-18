#include "UnitTest/UnitTest.h"
#include "Base/Utils.h"
#include "Base/Log.h"
#include "RecordPage.h"

namespace DataBaseFiles {

class FakeRecord {
 public:
  FakeRecord(int key) : key_(key) { InitRecord(); }

  DEFINE_ACCESSOR(key, int);
  DEFINE_ACCESSOR(length, int);
  byte* data() { return data_.get(); }

  // return record size including 4 byte key.
  int size() const { return length_ + 4; }

 private:
  void InitRecord() {
    // Generate random record data.
    length_ = Utils::RandomNumber() % 30 + 1;
    data_.reset(new byte[length_ + sizeof(int)]);
    // Write key to data.
    memcpy(data_.get(), &key_, sizeof(int));
    // Write record data.
    for (int i = 4; i < length_ + 4; i++) {
      (data_.get())[i] = Utils::RandomNumber() % 26 + 'a';
    }
  }

  int key_ = -1;
  std::unique_ptr<byte> data_;
  int length_ = 0; /* record data length without key */
};

class RecordPageTest: public UnitTest {
 private:
  std::unique_ptr<RecordPage> page_;
  std::vector<FakeRecord> records_source_;

  const int kNumRecordsSource = 1000;
  FILE* file = nullptr;

 public:
  void setup() override {
    file = fopen ("test/records.data", "w+");
    if (!file) {
      throw std::runtime_error("fail to open file \"test/records.data\"");
    }
    page_.reset(new RecordPage(0, file));
  }

  void InitRecordSource() {
    for (int i = 0; i < kNumRecordsSource; i++) {
      records_source_.emplace_back(FakeRecord(i));
    }
  }

  // ---------------------- Tests ------------------------- //
  void InsertRecords() {
    page_->InitInMemoryPage();
    InitRecordSource();
    int num_records_inserted = 0;
    int total_size = 20; // size of meta data excluding slot directory.

    // Begin inserting records until the page is full.
    while (true) {
      int key = Utils::RandomNumber() % records_source_.size();
      FakeRecord& record = records_source_[key];
      bool expect_success =
          (total_size + record.size() + kSlotDirectoryEntrySize) <= kPageSize;
      AssertEqual(expect_success,
                  page_->InsertRecord(record.data(), record.size()));

      if (expect_success) {
        num_records_inserted++;
        total_size += (record.size() + kSlotDirectoryEntrySize);
      }
      else {
        break;
      }
    }
    LogINFO("Records Inserted: %d", num_records_inserted);
    LogINFO("Space Occupied: %d bytes\n", total_size);
  }

  void Test_InsertRecords() {
    // Insert and save data to file.
    InsertRecords();
    AssertTrue(page_->DumpPageData());

    // Reload page from file.
    page_.reset(new RecordPage(0, file));
    AssertTrue(page_->LoadPageData());

    // Verify content of all records in this page.
    for (auto slot: page_->Meta()->slot_directory()) {
      int offset = slot.offset();
      int length = slot.length();
      const byte* record = page_->data() + offset;
      int key = *(reinterpret_cast<const int*>(record));
      AssertTrue(ContentEqual(record + 4, records_source_[key].data() + 4,
                 length - 4));
    }
  }

  void Test_InsertDeleteRecords() {
  }

};

}  // namespace DataBaseFiles

int main() {
  for (int i = 0; i < 1; i++) {
    DataBaseFiles::RecordPageTest test;
    test.setup();
    test.Test_InsertRecords();
  }

  std::cout << "Passed ^_^" << std::endl;
  return 0;
}

