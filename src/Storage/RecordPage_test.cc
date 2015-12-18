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

  void teardown() override {
    fclose(file);
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

    AssertEqual(total_size, kPageSize - page_->FreeSize());
    LogINFO("Records Inserted: %d", num_records_inserted);
    LogINFO("Space Occupied: %d bytes\n", total_size);
  }

  void VerifyPageRecords() {
    for (auto slot: page_->Meta()->slot_directory()) {
      int offset = slot.offset();
      int length = slot.length();
      const byte* record = page_->data() + offset;
      int key = *(reinterpret_cast<const int*>(record));
      AssertTrue(ContentEqual(record + 4, records_source_[key].data() + 4,
                              length - 4));
    }
  }

  void Test_InsertRecords() {
    // Insert and save data to file.
    InsertRecords();
    AssertTrue(page_->DumpPageData());

    // Reload page from file.
    page_.reset(new RecordPage(0, file));
    AssertTrue(page_->LoadPageData());

    // Verify content of all records in this page.
    VerifyPageRecords();
  }

  void Test_DeleteRecords() {
    // Insert and save data to file.
    InsertRecords();
    AssertTrue(page_->DumpPageData());

    // Reload page from file.
    page_.reset(new RecordPage(0, file));
    AssertTrue(page_->LoadPageData());

    // Delete some records, and refill the page to full. Repeat this process.
    for (int i = 0; i < 100; i++) {
      // Generate a random list of slot id to delete.
      int delete_percent = (Utils::RandomNumber() % 10 + 1) / 10;
      auto slot_directory = page_->Meta()->slot_directory();
      int delete_num = slot_directory.size() * delete_percent;
      std::vector<int> slots_to_delete =
          Utils::RandomListFromRange(0, slot_directory.size() - 1, delete_num);
      AssertFalse(slots_to_delete.empty());

      // delete records.
      int free_size = page_->FreeSize();
      for (int slot_id: slots_to_delete) {
        free_size += slot_directory[slot_id].length();
        // if deleted last slot, we save another 4 bytes from releasing the
        // slot direcotry entry.
        if (slot_id == (int)slot_directory.size() - 1) {
          free_size += 4;
        }
        AssertTrue(page_->DeleteRecord(slot_id));
      }

      // Refill page with new records.
      while (true) {
        int key = Utils::RandomNumber() % records_source_.size();
        FakeRecord& record = records_source_[key];
        bool expect_success =
            (free_size + record.size() + kSlotDirectoryEntrySize) <= kPageSize;
        AssertEqual(expect_success,
                    page_->InsertRecord(record.data(), record.size()));

        if (expect_success) {
          //num_records_inserted++;
          free_size -= (record.size() + kSlotDirectoryEntrySize);
        }
        else {
          break;
        }
      }
      // Done. Repeat.
    }
  }

};

}  // namespace DataBaseFiles

int main() {
  for (int i = 0; i < 1; i++) {
    DataBaseFiles::RecordPageTest test;
    test.setup();
    test.Test_InsertRecords();
    test.teardown();
  }

  std::cout << "\033[2;32mPassed ^_^\033[0m" << std::endl;
  return 0;
}

