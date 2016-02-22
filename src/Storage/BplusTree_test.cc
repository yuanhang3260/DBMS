#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>

#include "UnitTest/UnitTest.h"
#include "Base/Utils.h"
#include "Base/Log.h"
#include "Schema/Record.h"
#include "BplusTree.h"
#include "Schema/DBTable_pb.h"
#include "Schema/PageRecordsManager.h"
#include "DataBase/Table.h"
#include "DataBase/Operation.h"

namespace DataBaseFiles {

class BplusTreeTest: public UnitTest {
 private:
  std::string tablename = "testTable";
  std::vector<int> key_indexes;
  Schema::TableSchema* schema = nullptr;
  DataBase::Table* table;
  std::map<int, std::shared_ptr<Schema::DataRecord>> record_resource;
  const int kNumRecordsSource = 1000;

 public:
  void InitSchema() {
    // Create a table schema.
    schema = new Schema::TableSchema();
    schema->set_name(tablename);
    // Add string type
    Schema::TableField* field = schema->add_fields();
    field->set_name("name");
    field->set_index(0);
    field->set_type(Schema::TableField::STRING);
    // Add int type
    field = schema->add_fields();
    field->set_name("age");
    field->set_index(1);
    field->set_type(Schema::TableField::INTEGER);
    // Add long int type
    field = schema->add_fields();
    field->set_name("id");
    field->set_index(2);  // primary key
    field->set_type(Schema::TableField::LLONG);
    // Add double type
    field = schema->add_fields();
    field->set_name("weight");
    field->set_index(3);
    field->set_type(Schema::TableField::DOUBLE);
    // Add bool type
    field = schema->add_fields();
    field->set_name("adult");
    field->set_index(4);
    field->set_type(Schema::TableField::BOOL);
    // Add char array type
    field = schema->add_fields();
    field->set_name("signature");
    field->set_index(5);
    field->set_type(Schema::TableField::CHARARR);
    field->set_size(20);

    schema->add_primary_key_indexes(2);
    // Create key_indexes.
    for (auto i: schema->primary_key_indexes()) {
      key_indexes.push_back(i);
    }
  }

  void InitRecordResource() {
    record_resource.clear();
    for (int i = 0; i < kNumRecordsSource; i++) {
      record_resource.emplace(i, std::make_shared<Schema::DataRecord>());

      // Init fields to records.
      // name
      {
        int str_len = Utils::RandomNumber(5);
        char buf[str_len];
        for (int i = 0; i < str_len; i++) {
          buf[i] = 'a' + Utils::RandomNumber(26);
        }
        record_resource.at(i)->AddField(new Schema::StringType(buf, str_len));
      }
      // age
      int rand_int = Utils::RandomNumber(10);
      record_resource.at(i)->AddField(new Schema::IntType(rand_int));
      // money (we use this field as key for record resource map).
      //int rand_long = Utils::RandomNumber(10);
      if (i < kNumRecordsSource / 2) {
        record_resource.at(i)->AddField(new Schema::LongIntType(i));
      }
      else {
        //rand_long = Utils::RandomNumber(kNumRecordsSource);
        record_resource.at(i)->AddField(new Schema::LongIntType(i));
      }
      // weight
      double rand_double = 1.0 * Utils::RandomNumber() / Utils::RandomNumber();
      record_resource.at(i)->AddField(new Schema::DoubleType(rand_double));
      // adult
      bool rand_bool = Utils::RandomNumber() % 2 == 1 ? true : false;
      record_resource.at(i)->AddField(new Schema::BoolType(rand_bool));
      // signature
      {
        int len_limit = 20;
        int str_len = Utils::RandomNumber(len_limit) + 1;
        char buf[str_len];
        for (int i = 0; i < str_len; i++) {
          buf[i] = 'a' + Utils::RandomNumber(26);
        }
        record_resource.at(i)->AddField(
            new Schema::CharArrayType(buf, str_len, len_limit));
      }
    }
  }

  void InitRecordResourceFromFile(std::string filename) {
    record_resource.clear();
    std::ifstream infile(filename);
    std::string line;
    int i = 0;
    while (std::getline(infile, line)) {
      std::istringstream iss(line);
      auto record = std::make_shared<Schema::DataRecord>();
      if (record->ParseFromText(line, 20)) {
        record_resource.emplace(i, record);
        i++;
        record->Print();
      }
    }
    printf("Parsed %d records from file %s\n",
           (int)record_resource.size(), filename.c_str());
  }

  void setup() override {
    InitSchema();
    CreateSchemaFile(tablename);
    InitRecordResource();
    //InitRecordResourceFromFile("out1");
    table = new DataBase::Table(tablename);
  }

  void teardown() override {
    if (schema) {
      delete schema;
    }
    if (table) {
      delete table;
    }
  }

  bool CreateSchemaFile(std::string tablename) {
    // Serialize the schema message and write to schema file
    ::proto::SerializedMessage* sdmsg = schema->Serialize();
    const char* obj_data = sdmsg->GetBytes();

    std::string schema_filename = kDataDirectory + tablename + ".schema.pb";
    FILE* file = fopen(schema_filename.c_str(), "w+");
    if (!file) {
      LogERROR("Failed to open schema file %s", schema_filename.c_str());
      return false;
    }
    // Read schema file.
    int re = fwrite(obj_data, 1, sdmsg->size(), file);
    if (re != (int)sdmsg->size()) {
      LogERROR("Read schema file %s error, expect %d bytes, actual %d",
               schema_filename.c_str(), sdmsg->size(), re);
      return false;
    }
    fclose(file);
    return true;
  }

  void Test_Header_Page_Consistency_Check() {
    BplusTree tree(table, INDEX_DATA, key_indexes);

    tree.meta()->set_num_pages(1);
    AssertFalse(tree.SaveToDisk());
    tree.meta()->set_num_pages(0);

    tree.meta()->set_num_pages(1);
    tree.meta()->set_num_free_pages(1);
    AssertFalse(tree.SaveToDisk());
    tree.meta()->set_num_pages(0);
    tree.meta()->set_num_free_pages(0);

    tree.meta()->set_free_page(0);
    AssertFalse(tree.SaveToDisk());
    tree.meta()->set_free_page(-1);

    tree.meta()->set_num_leaves(1);
    AssertFalse(tree.SaveToDisk());
    tree.meta()->set_num_leaves(0);

    AssertTrue(tree.SaveToDisk());
  }

  void Test_Create_Load_Empty_Tree() {
    BplusTree tree(table, INDEX_DATA, key_indexes);

    AssertEqual(INDEX_DATA, tree.meta()->file_type());
    AssertEqual(0, tree.meta()->num_pages());
    AssertEqual(0, tree.meta()->num_free_pages());
    AssertEqual(0, tree.meta()->num_used_pages());
    AssertEqual(-1, tree.meta()->free_page());
    AssertEqual(-1, tree.meta()->root_page());
    AssertEqual(0, tree.meta()->num_leaves());
    AssertEqual(0, tree.meta()->depth());
  }

  void CheckBplusTree(FileType file_type, std::vector<int>& key_indexes) {
    BplusTree tree(table, file_type, key_indexes);
    AssertTrue(tree.ValidityCheck(), "Check B+ tree failed");
    printf("Good B+ Tree!\n");
  }

  void Test_BulkLoading() {
    std::vector<std::shared_ptr<Schema::RecordBase>> v;
    for (auto& entry: record_resource) {
      v.push_back(entry.second);
    }
    table->PreLoadData(v);

    auto file_type = INDEX;
    for (const auto& field: schema->fields()) {
      auto key_index = std::vector<int>{field.index()};
      file_type = table->IsDataFileKey(key_index[0]) ? INDEX_DATA : INDEX;
      auto tree = table->Tree(file_type, key_index);
      tree->SaveToDisk();

      CheckBplusTree(file_type, key_index);
      _VerifyAllRecordsInTree(tree, key_index);
    }
    AssertTrue(table->ValidateAllIndexRecords(v.size()));
  }

  void _VerifyAllRecordsInTree(BplusTree* tree, std::vector<int> key_indexes) {
    std::vector<std::shared_ptr<Schema::RecordBase>> v;
    for (auto& entry: record_resource) {
      v.push_back(entry.second);
    }
    Schema::PageRecordsManager::SortRecords(v, key_indexes);
    std::vector<int> start_list;
    start_list.push_back(0);
    Schema::RecordBase key;
    ((Schema::DataRecord*)v[0].get())->ExtractKey(&key, key_indexes);
    for (int i = 1; i < (int)v.size(); i++) {
      if (Schema::RecordBase::CompareRecordWithKey(
              &key, v[i].get(), key_indexes) == 0) {
        continue;
      }
      key.clear();
      ((Schema::DataRecord*)v[i].get())->ExtractKey(&key, key_indexes);
      start_list.push_back(i);
    }
    printf("%d different keys\n", (int)start_list.size());
    start_list.push_back(kNumRecordsSource);
    
    std::vector<std::shared_ptr<Schema::RecordBase>> result;
    for (int i = 0; i < (int)start_list.size() - 1; i++) {
      key.clear();
      int start = start_list[i];
      ((Schema::DataRecord*)v[start].get())->ExtractKey(&key, key_indexes);
      //key.Print();

      result.clear();
      AssertEqual(start_list[i + 1] - start_list[i],
                  tree->SearchRecords(&key, &result), "Search result differs");
    }
  }

  void _GenerateIndeRecord(Schema::IndexRecord* irecord, char c, int len) {
    // Reserve 9 bytes for '\0' and rid (8 bytes).
    len -= 9;
    char buf[len];
    memset(buf, c, len);
    irecord->AddField(new Schema::StringType(buf, len));
  }

  void _AddIndexRecordsToLeave(RecordPage* leave, std::vector<int> rlens,
                               std::vector<char> contents) {
    AssertEqual(rlens.size(), contents.size());
    int total_len = 0;
    for (int i = 0; i < (int)rlens.size(); i++) {
      Schema::IndexRecord irecord;
      _GenerateIndeRecord(&irecord, contents[i], rlens[i]);
      AssertTrue(irecord.InsertToRecordPage(leave) >= 0,
                 "Failed to add index record to leave");
      total_len += rlens[i];
      //irecord.Print();
    }
    AssertEqual((int)rlens.size(), (int)leave->Meta()->num_records());
    AssertEqual((int)total_len, (int)leave->Meta()->space_used());
  }

  void Test_SplitLeave() {
    // Create index for field "name".
    std::vector<int> key_index{0};
    BplusTree tree(table, INDEX, key_indexes);
    std::vector<Schema::DataRecordRidMutation> rid_mutations;

    {
      // page 1 <--> page2
      std::cout << "----- Left Smaller test 1 -----" << std::endl;
      RecordPage* leave = tree.AllocateNewPage(TREE_LEAVE);
      _AddIndexRecordsToLeave(leave, std::vector<int>{30, 10, 40, 50, 50},
                              std::vector<char>{'a', 'b', 'c', 'd', 'e'});

      Schema::PageRecordsManager prmanager(leave, schema, key_index,
                                           INDEX, TREE_LEAVE);
      prmanager.set_tree(&tree);
      // for (int i = 0; i < (int)prmanager.NumRecords(); i++) {
      //   prmanager.Record(i)->Print();
      // }

      Schema::IndexRecord irecord;
      _GenerateIndeRecord(&irecord, 'a', 60);
      AssertTrue(irecord.InsertToRecordPage(leave) < 0);
      auto result = prmanager.InsertRecordAndSplitPage(&irecord, rid_mutations);
      AssertEqual(2, (int)result.size());
      result[0].record->Print();
      AssertEqual(3, (int)result[0].page->Meta()->num_records());
      result[1].record->Print();
      AssertEqual(3, (int)result[1].page->Meta()->num_records());
      AssertEqual(result[0].page->Meta()->next_page(), result[1].page->id());
      AssertEqual(result[1].page->Meta()->prev_page(), result[0].page->id());
    }

    {
      // page 1 <--> page2 <--> overflow
      std::cout << "----- Left Smaller test 2 -----" << std::endl;
      RecordPage* leave = tree.AllocateNewPage(TREE_LEAVE);
      _AddIndexRecordsToLeave(leave, std::vector<int>{10, 60, 60, 60},
                              std::vector<char>{'a', 'b', 'b', 'b'});

      Schema::PageRecordsManager prmanager(leave, schema, key_index,
                                           INDEX, TREE_LEAVE);
      prmanager.set_tree(&tree);

      Schema::IndexRecord irecord;
      _GenerateIndeRecord(&irecord, 'b', 60);
      AssertTrue(irecord.InsertToRecordPage(leave) < 0);
      
      auto result = prmanager.InsertRecordAndSplitPage(&irecord, rid_mutations);
      AssertEqual(2, (int)result.size());
      result[0].record->Print();
      AssertEqual(1, (int)result[0].page->Meta()->num_records());
      result[1].record->Print();
      AssertEqual(3, (int)result[1].page->Meta()->num_records());
      AssertEqual(result[0].page->Meta()->next_page(), result[1].page->id());
      AssertEqual(result[1].page->Meta()->prev_page(), result[0].page->id());
      // check overflow page.
      int of_page_id = result[1].page->Meta()->overflow_page();
      AssertTrue(of_page_id > 0);
      AssertTrue(tree.Page(of_page_id) > 0);
      AssertEqual(tree.Page(of_page_id)->Meta()->prev_page(),
                  result[1].page->id());
      AssertEqual(result[1].page->Meta()->next_page(),
                  tree.Page(of_page_id)->id());
    }

    {
      // page 1 <--> page2 <--> overflow <--> page3
      std::cout << "----- Left Smaller test 3 -----" << std::endl;
      RecordPage* leave = tree.AllocateNewPage(TREE_LEAVE);
      _AddIndexRecordsToLeave(
          leave, std::vector<int>{10, 10, 55, 55, 55, 10},
          std::vector<char>{'a', 'a', 'b', 'b', 'b', 'c'});

      Schema::PageRecordsManager prmanager(leave, schema, key_index,
                                           INDEX, TREE_LEAVE);
      prmanager.set_tree(&tree);

      Schema::IndexRecord irecord;
      _GenerateIndeRecord(&irecord, 'b', 55);
      AssertTrue(irecord.InsertToRecordPage(leave) < 0);
      
      auto result = prmanager.InsertRecordAndSplitPage(&irecord, rid_mutations);
      AssertEqual(3, (int)result.size());
      result[0].record->Print();
      AssertEqual(2, (int)result[0].page->Meta()->num_records());
      result[1].record->Print();
      AssertEqual(3, (int)result[1].page->Meta()->num_records());
      result[2].record->Print();
      AssertEqual(1, (int)result[2].page->Meta()->num_records());

      AssertEqual(result[0].page->Meta()->next_page(), result[1].page->id());
      AssertEqual(result[1].page->Meta()->prev_page(), result[0].page->id());
      // check overflow page.
      int of_page_id = result[1].page->Meta()->overflow_page();
      AssertTrue(of_page_id > 0);
      AssertTrue(tree.Page(of_page_id) > 0);
      AssertEqual(tree.Page(of_page_id)->Meta()->prev_page(),
                  result[1].page->id(), "page 2 <- overflow");
      AssertEqual(result[1].page->Meta()->next_page(),
                  tree.Page(of_page_id)->id(), "page 2 -> overflow");

      AssertEqual(tree.Page(of_page_id)->Meta()->next_page(),
                  result[2].page->id(), "overflow -> page 3");
      AssertEqual(result[2].page->Meta()->prev_page(),
                  tree.Page(of_page_id)->id(), "overflow <- page 3");
    }

    {
      // page 1 <--> page2 <--> page3
      std::cout << "----- Left Smaller test 4 -----" << std::endl;
      RecordPage* leave = tree.AllocateNewPage(TREE_LEAVE);
      _AddIndexRecordsToLeave(leave, std::vector<int>{30, 50, 50, 50, 10, 10},
                              std::vector<char>{'a', 'b', 'b', 'b', 'c', 'c'});

      Schema::PageRecordsManager prmanager(leave, schema, key_index,
                                           INDEX, TREE_LEAVE);
      prmanager.set_tree(&tree);

      Schema::IndexRecord irecord;
      _GenerateIndeRecord(&irecord, 'b', 50);
      AssertTrue(irecord.InsertToRecordPage(leave) < 0);
      
      auto result = prmanager.InsertRecordAndSplitPage(&irecord, rid_mutations);
      AssertEqual(3, (int)result.size());
      result[0].record->Print();
      AssertEqual(1, (int)result[0].page->Meta()->num_records());
      result[1].record->Print();
      AssertEqual(4, (int)result[1].page->Meta()->num_records());
      result[2].record->Print();
      AssertEqual(2, (int)result[2].page->Meta()->num_records());

      AssertEqual(result[0].page->Meta()->next_page(), result[1].page->id());
      AssertEqual(result[1].page->Meta()->prev_page(), result[0].page->id());
      AssertEqual(result[1].page->Meta()->next_page(), result[2].page->id());
      AssertEqual(result[2].page->Meta()->prev_page(), result[1].page->id());
      // check overflow page.
      int of_page_id = result[1].page->Meta()->overflow_page();
      AssertTrue(of_page_id < 0);
    }

    {
      // page 1 <--> page2
      std::cout << "----- Left Larger test 1 -----" << std::endl;
      RecordPage* leave = tree.AllocateNewPage(TREE_LEAVE);
      _AddIndexRecordsToLeave(leave, std::vector<int>{50, 10, 40, 30, 60},
                              std::vector<char>{'a', 'b', 'c', 'd', 'd'});

      Schema::PageRecordsManager prmanager(leave, schema, key_index,
                                           INDEX, TREE_LEAVE);
      prmanager.set_tree(&tree);

      Schema::IndexRecord irecord;
      _GenerateIndeRecord(&irecord, 'a', 40);
      AssertTrue(irecord.InsertToRecordPage(leave) < 0);

      auto result = prmanager.InsertRecordAndSplitPage(&irecord, rid_mutations);
      AssertEqual(2, (int)result.size());
      result[0].record->Print();
      AssertEqual(3, (int)result[0].page->Meta()->num_records());
      result[1].record->Print();
      AssertEqual(3, (int)result[1].page->Meta()->num_records());
      AssertEqual(result[0].page->Meta()->next_page(), result[1].page->id());
      AssertEqual(result[1].page->Meta()->prev_page(), result[0].page->id());
    }

    {
      // page 1 <--> page2
      std::cout << "----- Left Larger test 2 -----" << std::endl;
      RecordPage* leave = tree.AllocateNewPage(TREE_LEAVE);
      _AddIndexRecordsToLeave(leave, std::vector<int>{50, 10, 40, 30, 60},
                              std::vector<char>{'a', 'b', 'c', 'd', 'd'});

      Schema::PageRecordsManager prmanager(leave, schema, key_index,
                                           INDEX, TREE_LEAVE);
      prmanager.set_tree(&tree);

      Schema::IndexRecord irecord;
      _GenerateIndeRecord(&irecord, 'e', 30);
      AssertTrue(irecord.InsertToRecordPage(leave) < 0);

      auto result = prmanager.InsertRecordAndSplitPage(&irecord, rid_mutations);
      AssertEqual(2, (int)result.size());
      result[0].record->Print();
      AssertEqual(3, (int)result[0].page->Meta()->num_records());
      result[1].record->Print();
      AssertEqual(3, (int)result[1].page->Meta()->num_records());
      AssertEqual(result[0].page->Meta()->next_page(), result[1].page->id());
      AssertEqual(result[1].page->Meta()->prev_page(), result[0].page->id());
    }

    {
      // page 1 <--> overflow <--> page2
      std::cout << "----- Left Larger test 3 -----" << std::endl;
      RecordPage* leave = tree.AllocateNewPage(TREE_LEAVE);
      _AddIndexRecordsToLeave(leave, std::vector<int>{50, 50, 50, 50, 10},
                              std::vector<char>{'a', 'a', 'a', 'a', 'b'});

      Schema::PageRecordsManager prmanager(leave, schema, key_index,
                                           INDEX, TREE_LEAVE);
      prmanager.set_tree(&tree);

      Schema::IndexRecord irecord;
      _GenerateIndeRecord(&irecord, 'a', 50);
      AssertTrue(irecord.InsertToRecordPage(leave) < 0);

      auto result = prmanager.InsertRecordAndSplitPage(&irecord, rid_mutations);
      AssertEqual(2, (int)result.size());
      result[0].record->Print();
      AssertEqual(4, (int)result[0].page->Meta()->num_records());
      result[1].record->Print();
      AssertEqual(1, (int)result[1].page->Meta()->num_records());

      int of_page_id = result[0].page->Meta()->overflow_page();
      AssertTrue(of_page_id > 0);
      AssertTrue(tree.Page(of_page_id) > 0);
      AssertEqual(tree.Page(of_page_id)->Meta()->prev_page(),
                  result[0].page->id(), "page 1 <- overflow");
      AssertEqual(result[0].page->Meta()->next_page(),
                  tree.Page(of_page_id)->id(), "page 1 -> overflow");
      AssertEqual(tree.Page(of_page_id)->Meta()->next_page(),
                  result[1].page->id(), "overflow -> page 2");
      AssertEqual(result[1].page->Meta()->prev_page(),
                  tree.Page(of_page_id)->id(), "overflow <- page 2");
    }

    {
      // page 1 <--> page2 <--> page3
      std::cout << "----- Left Larger test 4 -----" << std::endl;
      RecordPage* leave = tree.AllocateNewPage(TREE_LEAVE);
      _AddIndexRecordsToLeave(leave, std::vector<int>{20, 50, 50, 50, 30},
                              std::vector<char>{'a', 'b', 'b', 'b', 'c'});

      Schema::PageRecordsManager prmanager(leave, schema, key_index,
                                           INDEX, TREE_LEAVE);
      prmanager.set_tree(&tree);

      Schema::IndexRecord irecord;
      _GenerateIndeRecord(&irecord, 'b', 50);
      AssertTrue(irecord.InsertToRecordPage(leave) < 0);

      auto result = prmanager.InsertRecordAndSplitPage(&irecord, rid_mutations);
      AssertEqual(3, (int)result.size());
      result[0].record->Print();
      AssertEqual(1, (int)result[0].page->Meta()->num_records());
      result[1].record->Print();
      AssertEqual(4, (int)result[1].page->Meta()->num_records());
      result[2].record->Print();
      AssertEqual(1, (int)result[2].page->Meta()->num_records());

      AssertEqual(result[0].page->Meta()->next_page(), result[1].page->id());
      AssertEqual(result[1].page->Meta()->prev_page(), result[0].page->id());
      AssertEqual(result[1].page->Meta()->next_page(), result[2].page->id());
      AssertEqual(result[2].page->Meta()->prev_page(), result[1].page->id());
      // check no overflow page.
      AssertTrue(result[0].page->Meta()->overflow_page() < 0);
      AssertTrue(result[1].page->Meta()->overflow_page() < 0);
      AssertTrue(result[2].page->Meta()->overflow_page() < 0);
    }

    {
      // page 1 <--> page2 <--> overflow <--> page3
      std::cout << "----- Left Larger test 5 -----" << std::endl;
      RecordPage* leave = tree.AllocateNewPage(TREE_LEAVE);
      _AddIndexRecordsToLeave(leave, std::vector<int>{10, 55, 55, 55, 20, 10},
                              std::vector<char>{'a', 'b', 'b', 'b', 'c', 'e'});

      Schema::PageRecordsManager prmanager(leave, schema, key_index,
                                           INDEX, TREE_LEAVE);
      prmanager.set_tree(&tree);

      Schema::IndexRecord irecord;
      _GenerateIndeRecord(&irecord, 'b', 55);
      AssertTrue(irecord.InsertToRecordPage(leave) < 0);

      auto result = prmanager.InsertRecordAndSplitPage(&irecord, rid_mutations);
      AssertEqual(3, (int)result.size());
      result[0].record->Print();
      AssertEqual(1, (int)result[0].page->Meta()->num_records());
      result[1].record->Print();
      AssertEqual(3, (int)result[1].page->Meta()->num_records());
      result[2].record->Print();
      AssertEqual(2, (int)result[2].page->Meta()->num_records());

      int of_page_id = result[1].page->Meta()->overflow_page();
      AssertTrue(of_page_id > 0);
      AssertTrue(tree.Page(of_page_id) > 0);
      AssertEqual(tree.Page(of_page_id)->Meta()->prev_page(),
                  result[1].page->id(), "page 2 <- overflow");
      AssertEqual(result[1].page->Meta()->next_page(),
                  tree.Page(of_page_id)->id(), "page 2 -> overflow");
      AssertEqual(tree.Page(of_page_id)->Meta()->next_page(),
                  result[2].page->id(), "overflow -> page 3");
      AssertEqual(result[2].page->Meta()->prev_page(),
                  tree.Page(of_page_id)->id(), "overflow <- page 3");
    }
  }

  void Test_InsertRecord(FileType file_type_, std::vector<int> key_index) {
    BplusTree tree(table, file_type_, key_index, true);  /* craete tree */
    for (int i = 0; i < (int)record_resource.size(); i++) {
      printf("-------------------------------------------------------------\n");
      printf("-------------------------------------------------------------\n");
      printf("i = %d, record size = %d\n", i, record_resource[i]->size());
      record_resource[i]->Print();
      std::vector<Schema::DataRecordRidMutation> rid_mutations;
      if (file_type_ == INDEX_DATA) {
        auto rid =tree.Do_InsertRecord(record_resource[i].get(), rid_mutations);
        //rid.Print();
        AssertTrue(rid.IsValid(), "Insert Record returned invalid rid");
        AssertTrue(tree.VerifyRecord(rid, record_resource[i].get()),
                   "Verify record failed");
      }
      else {
        Schema::IndexRecord irecord;
        ((Schema::DataRecord*)record_resource[i].get())->
            ExtractKey(&irecord, key_index);
        auto rid = tree.Do_InsertRecord(&irecord, rid_mutations);
        AssertTrue(rid.IsValid());
      }
      // printf("rid mutations\n");
      // for (const auto& m: rid_mutations) {
      //   m.Print();
      // }
    }
    // Search and verify records.
    _VerifyAllRecordsInTree(&tree, key_index);
  }

  void Test_DeleteRecord(FileType file_type, std::vector<int> key_index) {
    // Create a tree with records.
    Test_InsertRecord(file_type, key_index);
    CheckBplusTree(file_type, key_index);
    printf("********************** Begin Deleting *************************\n");

    // Delete records.
    BplusTree tree(table, file_type, key_index);
    std::vector<int> del_keys = Utils::RandomListFromRange(0,kNumRecordsSource);
    //std::sort(del_keys.begin(), del_keys.end());
    for (int i: del_keys) {
      printf("--------------------- i = %d ---------------------------\n", i);
      DataBase::DeleteOp op;
      op.key_index = key_index[0];
      op.keys.push_back(std::make_shared<Schema::RecordBase>());
      op.keys.back()->AddField(new Schema::LongIntType(i));
      op.keys.back()->Print();

      DataBase::DeleteResult delete_result;
      tree.Do_DeleteRecordByKey(op.keys, &delete_result);

      tree.SaveToDisk();
      CheckBplusTree(file_type, key_index);
    }
  }

  void Test_UpdateRecordID() {
    std::vector<std::shared_ptr<Schema::RecordBase>> v;
    for (int i = 0; i < (int)record_resource.size(); i++) {
      v.push_back(record_resource[i]);
    }
    Schema::PageRecordsManager::SortRecords(v, key_indexes);

    // Insert first half of records.
    std::vector<std::shared_ptr<Schema::RecordBase>> v1;
    for (int i = 0; i < kNumRecordsSource / 2; i++) {
      v1.push_back(v[i]);
      printf("-------------------------------------------------------------\n");
      printf("-------------------------------------------------------------\n");
      printf("i = %d, record size = %d\n", i, v[i]->size());
      v[i]->Print();
    }
    table->PreLoadData(v1);
    AssertTrue(table->ValidateAllIndexRecords(v1.size()));

    // Insert another half records.
    int end = kNumRecordsSource;
    for (int i = kNumRecordsSource / 2; i < end; i++) {
      printf("-------------------------------------------------------------\n");
      printf("-------------------------------------------------------------\n");
      printf("i = %d, record size = %d\n", i, v[i]->size());
      v[i]->Print();
      table->InsertRecord(v[i].get());
    }
    AssertTrue(table->ValidateAllIndexRecords(end));

    // Consistency check of all B+ trees.
    auto file_type = INDEX;
    for (const auto& field: schema->fields()) {
      auto key_index = std::vector<int>{field.index()};
      file_type = table->IsDataFileKey(key_index[0]) ? INDEX_DATA : INDEX;
      auto tree = table->Tree(file_type, key_index);
      tree->SaveToDisk();

      CheckBplusTree(file_type, key_index);
      _VerifyAllRecordsInTree(tree, key_index);
    }
  }

  void Test_DeleteIndexRecordPre(FileType file_type, std::vector<int> key_index)
  {
    Test_UpdateRecordID();
    printf("********************** Begin Deleting *************************\n");

    BplusTree tree(table, DataBaseFiles::INDEX, key_index);
    DataBase::DeleteResult delete_result;
    delete_result.del_mode = DataBase::DeleteResult::DEL_INDEX_PRE;

    // Generate delete operator.
    DataBase::DeleteOp op;
    std::vector<int> ages = Utils::RandomListFromRange(0,10);
    for (int i = 0; i < (int)ages.size(); i++) {
      op.key_index = key_index[0];
      op.keys.push_back(std::make_shared<Schema::RecordBase>());
      op.keys.back()->AddField(new Schema::IntType(ages[i]));
    }

    // Delete from index tree.
    AssertTrue(tree.Do_DeleteRecordByKey(op.keys, &delete_result),
               "delete failed");
    printf("Got %d deleted rids from index tree %d\n",
           (int)delete_result.rid_deleted.size(), key_index[0]);
    for (const auto& m: delete_result.rid_deleted) {
      m.old_rid.Print();
    }

    // Delete from data tree by RecordID.
    BplusTree data_tree(table, DataBaseFiles::INDEX_DATA, table->DataTreeKey());
    for (const auto& m: delete_result.rid_deleted) {
      auto data_record = data_tree.GetRecord(m.old_rid);
      AssertTrue(data_record.get());
      data_record->Print();
    }
    // Delete data records.
    DataBase::DeleteResult data_del_result;
    data_tree.Do_DeleteRecordByRecordID(delete_result, &data_del_result);
    printf("deleted %d data records from data tree\n",
           (int)data_del_result.rid_deleted.size());
    printf("mutated %d data records in data tree\n",
           (int)data_del_result.rid_mutations.size());
  }

  void Test_TableDeleteRecord(FileType file_type, std::vector<int> key_index)
  {
    Test_UpdateRecordID();
    printf("********************** Begin Deleting *************************\n");

    BplusTree tree(table, DataBaseFiles::INDEX, key_index);
    DataBase::DeleteResult delete_result;
    delete_result.del_mode = DataBase::DeleteResult::DEL_INDEX_PRE;

    // Generate delete operator.
    DataBase::DeleteOp op;
    std::vector<int> ages = Utils::RandomListFromRange(0, 8);
    for (int i = 0; i < (int)ages.size(); i++) {
      op.key_index = key_index[0];
      op.keys.push_back(std::make_shared<Schema::RecordBase>());
      op.keys.back()->AddField(new Schema::IntType(ages[i]));
    }

    // Do deletion.
    table->DeleteRecord(op);

    // Validate index tree records consistency with data tree records.
    AssertTrue(table->ValidateAllIndexRecords(-1));

    // Consistency check of all B+ trees.
    for (const auto& field: schema->fields()) {
      auto key_index = std::vector<int>{field.index()};
      //key_index[0] = 0;
      file_type = table->IsDataFileKey(key_index[0]) ? INDEX_DATA : INDEX;
      auto tree = table->Tree(file_type, key_index);
      tree->SaveToDisk();

      CheckBplusTree(file_type, key_index);
      //_VerifyAllRecordsInTree(tree, key_index);
      //break;
    }
  }
};

}  // namespace DataBaseFiles

int main(int argc, char** argv) {
  DataBaseFiles::FileType file_type = DataBaseFiles::INDEX_DATA;
  std::vector<int> key_index{2};
  if (argc >= 2 && std::string(argv[1]) == "INDEX") {
    file_type = DataBaseFiles::INDEX;
  }
  if (argc >= 3) {
    key_index.clear();
    key_index.push_back(std::stoi(argv[2]));
  }

  DataBaseFiles::BplusTreeTest test;
  test.setup();

  //test.Test_Header_Page_Consistency_Check();
  // test.Test_Create_Load_Empty_Tree();
  
  // for (int i = 0; i < 1; i++) {
  //   test.Test_BulkLoading();
  // }

  // test.Test_SplitLeave();
  // for (int i = 0; i < 1; i++) {
  //   test.Test_InsertRecord(file_type, key_index);
  //   test.CheckBplusTree(file_type, key_index);
  // }

  // for (int i = 0; i < 100; i++) {
  //   // LogERROR("----------- ii = %d ----------", i);
  //   // printf("----------- ii = %d ----------\n", i);
  //   test.Test_DeleteRecord(file_type, key_index);
  // }

  //test.Test_UpdateRecordID();

  // for (int i = 0; i < 1; i++) {
  //   LogERROR("i == %d", i);
  //   // printf("i == %d\n", i);
  //   test.Test_DeleteIndexRecordPre(file_type, key_index);
  // }

  for (int i = 0; i < 1; i++) {
    //LogERROR("i == %d", i);
    // printf("i == %d\n", i);
    test.Test_TableDeleteRecord(file_type, key_index);
  }

  test.teardown();
  std::cout << "\033[2;32mAll Passed ^_^\033[0m" << std::endl;
  return 0;
}

