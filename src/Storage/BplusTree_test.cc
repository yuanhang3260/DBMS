#include "UnitTest/UnitTest.h"
#include "Base/Utils.h"
#include "Base/Log.h"
#include "Schema/Record.h"
#include "BplusTree.h"
#include "Schema/DBTable_pb.h"

namespace DataBaseFiles {

class BplusTreeTest: public UnitTest {
 private:
  std::string tablename = "testTable";
  std::vector<int> key_indexes;
  Schema::TableSchema* schema = nullptr;
  std::map<int, std::shared_ptr<Schema::DataRecord>> record_resource;
  const int kNumRecordsSource = 10000;

 public:
  void InitSchema() {
    // Create key_indexes.
    key_indexes = std::vector<int>{2, 0};

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
    field->set_index(2);
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
  }

  void InitRecordResource() {
    record_resource.clear();
    for (int i = 0; i < kNumRecordsSource; i++) {
      record_resource.emplace(i, std::make_shared<Schema::DataRecord>());

      // Init fields to records.
      // name
      {
        if (i >= 2 && i <= 6) {
          record_resource.at(i)->AddField(new Schema::StringType("hello"));
        }
        else {
          int str_len = Utils::RandomNumber(10);
          char buf[str_len];
          for (int i = 0; i < str_len; i++) {
            buf[i] = 'a' + Utils::RandomNumber(26);
          }
          record_resource.at(i)->AddField(new Schema::StringType(buf, str_len));
        }
      }
      // age
      int rand_int = Utils::RandomNumber(20);
      record_resource.at(i)->AddField(new Schema::IntType(rand_int));
      // money (we use this field as key for record resource map).
      int rand_long = Utils::RandomNumber(10);
      if (i >= 2 && i <= 6) {
        record_resource.at(i)->AddField(new Schema::LongIntType(-1));
      }
      else {
        record_resource.at(i)->AddField(new Schema::LongIntType(rand_long));
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

  void setup() override {
    InitSchema();
    InitRecordResource();
  }

  void teardown() override {
    if (schema) {
      delete schema;
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

  void Test_SchemaFile() {
    std::string tablename = "testTable";
    AssertTrue(CreateSchemaFile(tablename));

    // Create a B+ tree and load the schema.
    BplusTree tree;
    tree.set_tablename(tablename);
    tree.LoadSchema();
    const auto* loaded_schema = tree.schema();
    AssertTrue(loaded_schema->Equals(*schema),
               "Loaded schema differs from original");

    loaded_schema->Print();
  }

  void Test_Header_Page_Consistency_Check() {
    BplusTree tree;
    AssertTrue(tree.CreateFile(tablename, key_indexes, INDEX_DATA),
               "Create B+ tree file faild");

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
    std::string tablename = "testTable";
    // Create a new empty B+ tree and save it file.
    {
      BplusTree tree;
      AssertTrue(tree.CreateFile(tablename, key_indexes, INDEX_DATA),
                 "Create B+ tree file faild");
      AssertTrue(tree.SaveToDisk(), "Save to disk failed");
    }

    // Load a B +Tree from this file.
    {
      BplusTree tree2(tablename, key_indexes);
      AssertEqual(INDEX_DATA, tree2.meta()->file_type());
      AssertEqual(0, tree2.meta()->num_pages());
      AssertEqual(0, tree2.meta()->num_free_pages());
      AssertEqual(0, tree2.meta()->num_used_pages());
      AssertEqual(-1, tree2.meta()->free_page());
      AssertEqual(-1, tree2.meta()->root_page());
      AssertEqual(0, tree2.meta()->num_leaves());
      AssertEqual(0, tree2.meta()->depth());
    }
  }

  void CheckBplusTree() {
    BplusTree tree(tablename, key_indexes);
    AssertTrue(tree.ValidityCheck(), "Check B+ tree failed");
    printf("Good B+ Tree!\n");
  }

  void Test_BulkLoading() {
    std::vector<std::shared_ptr<Schema::RecordBase>> v;
    for (auto& entry: record_resource) {
      v.push_back(entry.second);
    }
    Schema::PageRecordsManager::SortRecords(v, key_indexes);

    BplusTree tree;
    AssertTrue(tree.CreateFile(tablename, key_indexes, INDEX_DATA),
               "Create B+ tree file faild");
    for (int i = 0; i < kNumRecordsSource; i++) {
      tree.BulkLoadRecord((Schema::DataRecord*)v.at(i).get());
    }
  }

  void Test_SearchByKey() {
    BplusTree tree(tablename, key_indexes);
    Schema::RecordBase key;
    key.AddField(new Schema::LongIntType(-1));
    key.AddField(new Schema::StringType("hello"));

    std::vector<std::shared_ptr<Schema::RecordBase>> result;
    tree.SearchByKey(&key, &result);

    printf("Searched %d records matching key (-1, \"hello\")\n", result.size());

    // Scan all records and search one by one.
    // First merge records with the same key.
    std::vector<std::shared_ptr<Schema::RecordBase>> v;
    for (auto& entry: record_resource) {
      v.push_back(entry.second);
    }
    Schema::PageRecordsManager::SortRecords(v, key_indexes);
    std::vector<int> start_list;
    start_list.push_back(0);
    key.clear();
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
    printf("%d different keys\n", start_list.size());
    start_list.push_back(kNumRecordsSource);
    for (int i = 0; i < (int)start_list.size() - 1; i++) {
      key.clear();
      int start = start_list[i];
      ((Schema::DataRecord*)v[start].get())->ExtractKey(&key, key_indexes);
      
      result.clear();
      AssertEqual(start_list[i + 1] - start_list[i],
                  tree.SearchByKey(&key, &result), "Search result differs");
    }
  }


};

}  // namespace DataBaseFiles

int main() {
  DataBaseFiles::BplusTreeTest test;
  test.setup();
  test.Test_SchemaFile();
  test.Test_Header_Page_Consistency_Check();
  test.Test_Create_Load_Empty_Tree();
  for (int i = 0; i < 1; i++) {
    test.Test_BulkLoading();
    test.CheckBplusTree();
  }
  test.Test_SearchByKey();
  test.teardown();

  std::cout << "\033[2;32mAll Passed ^_^\033[0m" << std::endl;
  return 0;
}

